/// Escape an HTML string suitable for use anywhere in HTML. This is slightly
/// more conservative than it needs to be so it works everywhere.
pub fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => {
                out += "&amp;";
            }
            '<' => {
                out += "&lt;";
            }
            '>' => {
                out += "&gt;";
            }
            '"' => {
                out += "&quot;";
            }
            '\'' => {
                out += "&#39;";
            }
            c => {
                out.push(c);
            }
        }
    }
    out
}

/// Escape a string for use in a URL query string.
pub fn escape_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.bytes() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'.' | b' ' | b'~' => {
                out.push(c as char);
            }
            c => {
                out.push_str(&format!("%{:02X}", c));
            }
        }
    }
    out
}
