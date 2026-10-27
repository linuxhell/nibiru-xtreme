//! Win32 component rules shared by torrent metadata and ED2K names.
pub(crate) fn reserved(name: &str) -> bool {
    let base = name
        .split('.')
        .next()
        .unwrap_or(name)
        .trim_end()
        .to_uppercase();
    matches!(
        base.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || ["COM", "LPT"].iter().any(|prefix| {
        base.strip_prefix(prefix).is_some_and(|tail| {
            matches!(
                tail,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        })
    })
}
pub(crate) fn bounded(text: &str, limit: usize) -> String {
    let mut width = 0;
    text.chars()
        .take_while(|c| {
            width += c.len_utf8();
            width <= limit
        })
        .collect()
}
pub(crate) fn trim_tail(name: &str) -> &str {
    name.trim_end_matches(|c: char| c == '.' || c.is_whitespace())
}

pub(crate) fn portable_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name
            .chars()
            .any(|c| c.is_control() || "<>:\"/\\|?*".contains(c))
        && trim_tail(name) == name
        && name.trim_start() == name
        && !reserved(name)
        && name.len() <= 255
}
