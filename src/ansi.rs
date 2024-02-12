/// Strip ANSI colour codes from a string.
pub fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());

    enum State {
        Normal,
        Escape,
        Csi,
    }

    let mut state = State::Normal;

    for b in s.chars() {
        match &state {
            State::Normal => {
                if b == '\x1B' {
                    // ESC
                    state = State::Escape;
                } else {
                    out.push(b);
                }
            }
            State::Escape => {
                if b == '[' {
                    // [
                    state = State::Csi;
                } else {
                    state = State::Normal;
                }
            }
            State::Csi => {
                if b >= '\x40' && b <= '\x7F' {
                    state = State::Normal;
                }
            }
        }
    }
    out
}
