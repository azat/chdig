use cursive::{event::Event, theme::Effect, utils::markup::StyledString};

#[derive(Clone)]
pub struct ActionDescription {
    pub text: &'static str,
    pub event: Event,
}

impl ActionDescription {
    pub fn event_string(&self) -> String {
        match self.event {
            Event::Char(c) => {
                // - It is hard to understand that nothing is a space
                // - And it overlaps with no shortcut actions
                if c == ' ' {
                    return "<Space>".to_string();
                } else {
                    return c.to_string();
                }
            }
            Event::CtrlChar(c) => {
                return format!("Ctrl+{}", c.to_string());
            }
            Event::AltChar(c) => {
                return format!("Alt+{}", c.to_string());
            }
            Event::Key(k) => {
                return format!("{:?}", k);
            }
            Event::Unknown(_) => {
                return "".to_string();
            }
            _ => panic!("{:?} is not supported", self.event),
        }
    }
    pub fn preview_styled(&self) -> StyledString {
        let mut text = StyledString::default();
        text.append_styled(format!("{:>10}", self.event_string()), Effect::Bold);
        text.append_plain(format!(" - {}\n", self.text));
        return text;
    }
}
