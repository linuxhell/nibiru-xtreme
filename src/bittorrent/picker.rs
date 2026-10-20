//! Piece/block selection: rarest-first by default, with a sequential "playback window" for
//! streaming and endgame duplication once everything left is already requested.

use super::bitfield::Bitfield;
use std::collections::{BTreeMap, HashMap};

pub const BLOCK: u32 = 16 * 1024;

pub type PeerId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    RarestFirst,
    /// Streaming: pieces from `head` onward in order. `window` pieces right after the head are
    /// strictly sequential; the rest of the wanted set falls back to rarest-first.
    Sequential {
        head: usize,
        window: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRef {
    pub piece: usize,
    pub offset: u32,
    pub length: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Block {
    Missing,
    Requested,
    Done,
}

struct Progress {
    blocks: Vec<Block>,
}

pub struct Picker {
    piece_length: u64,
    total: u64,
    count: usize,
    have: Bitfield,
    wanted: Bitfield,
    availability: Vec<u32>,
    progress: BTreeMap<usize, Progress>,
    /// Who was asked for each block (more than one only in endgame).
    asked: HashMap<(usize, u32), Vec<PeerId>>,
    pub mode: Mode,
}

impl Picker {
    pub fn new(piece_length: u64, total: u64, have: Bitfield, wanted: Bitfield) -> Self {
        let count = have.len();
        Self {
            piece_length,
            total,
            count,
            have,
            wanted,
            availability: vec![0; count],
            progress: BTreeMap::new(),
            asked: HashMap::new(),
            mode: Mode::RarestFirst,
        }
    }

    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    pub fn wanted(&self) -> &Bitfield {
        &self.wanted
    }

    pub fn set_wanted(&mut self, wanted: Bitfield) {
        self.wanted = wanted;
    }

    pub fn piece_len(&self, piece: usize) -> u64 {
        self.total
            .saturating_sub(piece as u64 * self.piece_length)
            .min(self.piece_length)
    }

    fn block_count(&self, piece: usize) -> usize {
        self.piece_len(piece).div_ceil(BLOCK as u64) as usize
    }

    fn block_len(&self, piece: usize, index: usize) -> u32 {
        let start = index as u64 * BLOCK as u64;
        (self.piece_len(piece) - start).min(BLOCK as u64) as u32
    }

    pub fn is_complete(&self) -> bool {
        (0..self.count).all(|i| !self.wanted.get(i) || self.have.get(i))
    }

    pub fn missing_wanted(&self) -> usize {
        (0..self.count)
            .filter(|&i| self.wanted.get(i) && !self.have.get(i))
            .count()
    }

    pub fn mark_have(&mut self, piece: usize) {
        self.have.set(piece);
        self.progress.remove(&piece);
        self.asked.retain(|(p, _), _| *p != piece);
    }

    // ---- availability -------------------------------------------------------------------

    pub fn peer_joined(&mut self, bitfield: &Bitfield) {
        for i in bitfield.iter_set() {
            if i < self.count {
                self.availability[i] += 1;
            }
        }
    }

    pub fn peer_left(&mut self, bitfield: &Bitfield) {
        for i in bitfield.iter_set() {
            if i < self.count {
                self.availability[i] = self.availability[i].saturating_sub(1);
            }
        }
    }

    pub fn peer_has(&mut self, piece: usize) {
        if piece < self.count {
            self.availability[piece] += 1;
        }
    }

    pub fn availability(&self, piece: usize) -> u32 {
        self.availability.get(piece).copied().unwrap_or(0)
    }

    // ---- picking ------------------------------------------------------------------------

    fn candidate(&self, piece: usize, theirs: &Bitfield) -> bool {
        piece < self.count && self.wanted.get(piece) && !self.have.get(piece) && theirs.get(piece)
    }

    /// Next block to request from `peer`, who owns `theirs`. Prefers finishing pieces already in
    /// progress (so they complete and can be verified/shared) before opening new ones.
    pub fn next_block(
        &mut self,
        peer: PeerId,
        theirs: &Bitfield,
        allowed: &Bitfield,
    ) -> Option<BlockRef> {
        let allow = |s: &Self, p: usize| s.candidate(p, theirs) && allowed.get(p);
        let pick = self
            .pick_window(&allow)
            .or_else(|| self.pick_in_progress(&allow))
            .or_else(|| self.pick_new(&allow))
            .or_else(|| self.pick_endgame(peer, &allow))?;
        let (piece, index) = pick;
        let blocks = self.block_count(piece);
        let entry = self.progress.entry(piece).or_insert_with(|| Progress {
            blocks: vec![Block::Missing; blocks],
        });
        entry.blocks[index] = Block::Requested;
        self.asked
            .entry((piece, index as u32))
            .or_default()
            .push(peer);
        Some(BlockRef {
            piece,
            offset: index as u32 * BLOCK,
            length: self.block_len(piece, index),
        })
    }

    fn first_missing(&self, piece: usize) -> Option<usize> {
        self.progress
            .get(&piece)?
            .blocks
            .iter()
            .position(|b| *b == Block::Missing)
    }

    fn priority_key(&self, piece: usize) -> (u8, u64) {
        match self.mode {
            Mode::RarestFirst => (0, self.availability[piece] as u64),
            Mode::Sequential { head, window } => {
                if piece >= head && piece < head.saturating_add(window) {
                    (0, piece as u64)
                } else {
                    (1, self.availability[piece] as u64)
                }
            }
        }
    }

    /// Streaming: the first window piece (in playback order) that still has an unrequested block.
    fn pick_window(&self, allow: &dyn Fn(&Self, usize) -> bool) -> Option<(usize, usize)> {
        let Mode::Sequential { head, window } = self.mode else {
            return None;
        };
        let end = head.saturating_add(window).min(self.count);
        (head..end)
            .filter(|&p| allow(self, p))
            .find_map(|p| match self.progress.get(&p) {
                None => Some((p, 0)),
                Some(_) => self.first_missing(p).map(|i| (p, i)),
            })
    }

    fn pick_in_progress(&self, allow: &dyn Fn(&Self, usize) -> bool) -> Option<(usize, usize)> {
        let best = self
            .progress
            .keys()
            .copied()
            .filter(|&p| allow(self, p) && self.first_missing(p).is_some())
            .min_by_key(|&p| (self.priority_key(p), p))?;
        self.first_missing(best).map(|i| (best, i))
    }

    fn pick_new(&self, allow: &dyn Fn(&Self, usize) -> bool) -> Option<(usize, usize)> {
        let piece = (0..self.count)
            .filter(|&p| allow(self, p) && !self.progress.contains_key(&p))
            .min_by_key(|&p| (self.priority_key(p), p))?;
        Some((piece, 0))
    }

    /// All remaining blocks are in flight: duplicate them across other peers so one slow peer
    /// cannot hold the tail of the download hostage.
    fn pick_endgame(
        &self,
        peer: PeerId,
        allow: &dyn Fn(&Self, usize) -> bool,
    ) -> Option<(usize, usize)> {
        if self.missing_wanted() > 32 {
            return None;
        }
        for (&piece, progress) in &self.progress {
            if !allow(self, piece) {
                continue;
            }
            for (index, state) in progress.blocks.iter().enumerate() {
                if *state == Block::Requested
                    && let Some(who) = self.asked.get(&(piece, index as u32))
                    && !who.contains(&peer)
                    && who.len() < 3
                {
                    return Some((piece, index));
                }
            }
        }
        None
    }

    /// Reserves one whole untouched piece for a source that fetches complete pieces (a web seed).
    /// The caller finishes it through `mark_have` / `piece_failed` / `release_peer`.
    pub fn claim_piece(&mut self, peer: PeerId) -> Option<usize> {
        let allow = |s: &Self, p: usize| s.wanted.get(p) && !s.have.get(p);
        let piece = self
            .pick_window(&allow)
            .map(|(p, _)| p)
            .filter(|p| !self.progress.contains_key(p))
            .or_else(|| self.pick_new(&allow).map(|(p, _)| p))?;
        let blocks = self.block_count(piece);
        self.progress.insert(
            piece,
            Progress {
                blocks: vec![Block::Requested; blocks],
            },
        );
        for index in 0..blocks as u32 {
            self.asked.insert((piece, index), vec![peer]);
        }
        Some(piece)
    }

    // ---- results ------------------------------------------------------------------------

    /// A block arrived. Returns `Some(other peers to CANCEL)` the first time it is accepted,
    /// `None` for an unsolicited or duplicate block.
    pub fn block_received(
        &mut self,
        peer: PeerId,
        piece: usize,
        offset: u32,
    ) -> Option<Vec<PeerId>> {
        if !offset.is_multiple_of(BLOCK) {
            return None;
        }
        let index = (offset / BLOCK) as usize;
        let state = self.progress.get_mut(&piece)?.blocks.get_mut(index)?;
        if *state != Block::Requested {
            return None;
        }
        let asked = self
            .asked
            .remove(&(piece, index as u32))
            .unwrap_or_default();
        if !asked.contains(&peer) {
            // Nobody asked this peer for it; put the state back.
            self.asked.insert((piece, index as u32), asked);
            return None;
        }
        *state = Block::Done;
        Some(asked.into_iter().filter(|&p| p != peer).collect())
    }

    pub fn piece_has_all_blocks(&self, piece: usize) -> bool {
        self.progress
            .get(&piece)
            .is_some_and(|p| p.blocks.iter().all(|b| *b == Block::Done))
    }

    /// Hash check failed: throw the piece's blocks away so it is fetched again.
    pub fn piece_failed(&mut self, piece: usize) {
        self.progress.remove(&piece);
        self.asked.retain(|(p, _), _| *p != piece);
    }

    /// The peer disconnected, choked us, or rejected the request: its blocks become available.
    pub fn release_peer(&mut self, peer: PeerId) {
        let mut freed = Vec::new();
        self.asked.retain(|key, who| {
            who.retain(|p| *p != peer);
            if who.is_empty() {
                freed.push(*key);
                false
            } else {
                true
            }
        });
        for (piece, index) in freed {
            if let Some(p) = self.progress.get_mut(&piece)
                && let Some(b) = p.blocks.get_mut(index as usize)
                && *b == Block::Requested
            {
                *b = Block::Missing;
            }
        }
    }

    pub fn release_block(&mut self, peer: PeerId, piece: usize, offset: u32) {
        let index = offset / BLOCK;
        let key = (piece, index);
        if let Some(who) = self.asked.get_mut(&key) {
            who.retain(|p| *p != peer);
            if who.is_empty() {
                self.asked.remove(&key);
                if let Some(b) = self
                    .progress
                    .get_mut(&piece)
                    .and_then(|p| p.blocks.get_mut(index as usize))
                    && *b == Block::Requested
                {
                    *b = Block::Missing;
                }
            }
        }
    }

    pub fn in_flight(&self, peer: PeerId) -> usize {
        self.asked.values().filter(|w| w.contains(&peer)).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PL: u64 = 32 * 1024; // two blocks per piece

    fn picker(pieces: usize) -> Picker {
        let total = PL * pieces as u64 - 5_000; // short last piece
        let mut w = Bitfield::new(pieces);
        for i in 0..pieces {
            w.set(i);
        }
        Picker::new(PL, total, Bitfield::new(pieces), w)
    }

    fn all(n: usize) -> Bitfield {
        Bitfield::full(n)
    }

    #[test]
    fn rarest_piece_goes_first_and_in_progress_pieces_are_finished_first() {
        let mut p = picker(4);
        let mut a = Bitfield::new(4);
        a.set(0);
        a.set(1);
        a.set(2);
        p.peer_joined(&all(4));
        p.peer_joined(&a); // pieces 0..2 have 2 owners, piece 3 only 1
        let first = p.next_block(1, &all(4), &all(4)).unwrap();
        assert_eq!(first.piece, 3, "rarest");
        let second = p.next_block(1, &all(4), &all(4)).unwrap();
        assert_eq!(
            second.piece, 3,
            "finish the open piece before starting another"
        );
        assert_ne!(first.offset, second.offset);
        assert_eq!(second.length as u64, p.piece_len(3) - 16384);
    }

    #[test]
    fn only_wanted_missing_pieces_the_peer_has_are_requested() {
        let mut want = Bitfield::new(4);
        want.set(1);
        let mut have = Bitfield::new(4);
        have.set(2);
        let mut p = Picker::new(PL, PL * 4, have, want);
        let mut theirs = Bitfield::new(4);
        theirs.set(0);
        theirs.set(2);
        assert!(p.next_block(1, &theirs, &all(4)).is_none());
        theirs.set(1);
        assert_eq!(p.next_block(1, &theirs, &all(4)).unwrap().piece, 1);
    }

    #[test]
    fn streaming_window_is_strictly_sequential_even_if_other_pieces_are_rarer() {
        let mut p = picker(8);
        p.mode = Mode::Sequential { head: 2, window: 3 };
        let mut a = Bitfield::new(8);
        for i in 2..8 {
            a.set(i);
        }
        p.peer_joined(&all(8));
        p.peer_joined(&a);
        p.peer_joined(&a); // pieces 0,1 rare, but they're behind the playback head
        let mut order = Vec::new();
        for _ in 0..6 {
            order.push(p.next_block(1, &all(8), &all(8)).unwrap().piece);
        }
        assert_eq!(order, vec![2, 2, 3, 3, 4, 4]);
    }

    #[test]
    fn a_half_done_far_piece_does_not_starve_the_playback_window() {
        let mut p = picker(8);
        p.mode = Mode::RarestFirst;
        let first = p.next_block(1, &all(8), &all(8)).unwrap();
        // Viewer seeks near the end: the head jumps; the window must now win.
        let target = 6;
        p.mode = Mode::Sequential {
            head: target,
            window: 2,
        };
        let next = p.next_block(1, &all(8), &all(8)).unwrap();
        assert_eq!(
            next.piece, target,
            "window before the half-finished piece {}",
            first.piece
        );
    }

    #[test]
    fn received_blocks_complete_a_piece_and_mark_have() {
        let mut p = picker(2);
        let a = p.next_block(7, &all(2), &all(2)).unwrap();
        let b = p.next_block(7, &all(2), &all(2)).unwrap();
        assert_eq!(a.piece, b.piece);
        assert!(p.block_received(7, a.piece, a.offset).is_some());
        assert!(!p.piece_has_all_blocks(a.piece));
        assert!(p.block_received(7, b.piece, b.offset).is_some());
        assert!(p.piece_has_all_blocks(a.piece));
        assert!(
            p.block_received(7, b.piece, b.offset).is_none(),
            "duplicate"
        );
        p.mark_have(a.piece);
        assert!(p.have().get(a.piece));
    }

    #[test]
    fn unsolicited_blocks_are_ignored() {
        let mut p = picker(2);
        let a = p.next_block(1, &all(2), &all(2)).unwrap();
        assert!(
            p.block_received(2, a.piece, a.offset).is_none(),
            "wrong peer"
        );
        assert!(
            p.block_received(1, a.piece, a.offset + 7).is_none(),
            "misaligned"
        );
        assert!(
            p.block_received(1, 1 - a.piece, 0).is_none(),
            "never requested"
        );
        assert!(p.block_received(1, a.piece, a.offset).is_some());
    }

    #[test]
    fn disconnecting_peer_returns_its_blocks_to_the_pool() {
        let mut p = picker(1);
        let a = p.next_block(1, &all(1), &all(1)).unwrap();
        p.release_peer(1);
        assert_eq!(p.in_flight(1), 0);
        let again = p.next_block(2, &all(1), &all(1)).unwrap();
        assert_eq!(a, again);
    }

    #[test]
    fn a_failed_hash_requeues_the_whole_piece() {
        let mut p = picker(1);
        for _ in 0..2 {
            let b = p.next_block(1, &all(1), &all(1)).unwrap();
            p.block_received(1, b.piece, b.offset);
        }
        assert!(p.piece_has_all_blocks(0));
        p.piece_failed(0);
        assert!(!p.piece_has_all_blocks(0));
        assert_eq!(p.next_block(1, &all(1), &all(1)).unwrap().offset, 0);
    }

    #[test]
    fn endgame_duplicates_requests_to_other_peers_and_cancels_the_loser() {
        let mut p = picker(1);
        let a = p.next_block(1, &all(1), &all(1)).unwrap();
        let b = p.next_block(1, &all(1), &all(1)).unwrap();
        // Everything is in flight with peer 1; peer 2 gets duplicates, peer 1 does not.
        assert!(p.next_block(1, &all(1), &all(1)).is_none());
        let dup = p.next_block(2, &all(1), &all(1)).unwrap();
        assert!(dup == a || dup == b);
        let cancel = p.block_received(2, dup.piece, dup.offset).unwrap();
        assert_eq!(cancel, vec![1], "the slower peer is told to cancel");
    }

    #[test]
    fn allowed_set_restricts_choice_for_fast_set_peers() {
        let mut p = picker(3);
        let mut allowed = Bitfield::new(3);
        allowed.set(2);
        assert_eq!(p.next_block(1, &all(3), &allowed).unwrap().piece, 2);
    }

    #[test]
    fn a_claimed_piece_is_exclusive_until_released() {
        let mut p = picker(2);
        let first = p.claim_piece(9).unwrap();
        let second = p.claim_piece(10).unwrap();
        assert_ne!(first, second);
        assert!(p.claim_piece(11).is_none(), "everything is reserved");
        // Block-level peers do not get blocks of a reserved piece, except as endgame duplicates.
        p.release_peer(9);
        assert_eq!(p.claim_piece(11), Some(first));
    }

    #[test]
    fn completion_tracks_only_wanted_pieces() {
        let mut want = Bitfield::new(3);
        want.set(0);
        let mut p = Picker::new(PL, PL * 3, Bitfield::new(3), want);
        assert!(!p.is_complete());
        p.mark_have(0);
        assert!(p.is_complete());
        assert_eq!(p.missing_wanted(), 0);
    }
}
