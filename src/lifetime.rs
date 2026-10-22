//! Ownership signal for background listeners. Dropping the owner wakes idle tasks too.
use tokio::sync::watch;

pub(crate) struct Stop(watch::Sender<bool>);
impl Default for Stop {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}
impl Stop {
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.0.subscribe()
    }
    pub fn stop(&self) {
        self.0.send_replace(true);
    }
}
impl Drop for Stop {
    fn drop(&mut self) {
        self.stop();
    }
}
