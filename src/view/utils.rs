use cursive::theme::{BaseColor, Color, PaletteColor, Theme};
use cursive::views::Dialog;
use cursive::Cursive;

// TODO: use the same color schema as in htop/csysdig
pub fn make_cursive_theme_from_therminal(siv: &Cursive) -> Theme {
    let mut theme = siv.current_theme().clone();
    theme.palette[PaletteColor::Background] = Color::TerminalDefault;
    theme.palette[PaletteColor::View] = Color::TerminalDefault;
    theme.palette[PaletteColor::Primary] = Color::TerminalDefault;
    theme.palette[PaletteColor::Highlight] = Color::Light(BaseColor::Cyan);
    theme.palette[PaletteColor::HighlightText] = Color::Dark(BaseColor::Black);
    return theme;
}

pub fn pop_ui(siv: &mut cursive::Cursive) {
    if siv.screen_mut().len() == 1 {
        siv.quit();
    } else {
        siv.pop_layer();
    }
}

// TODO: more enhanced help (like in htop(1))
pub fn show_help_dialog(siv: &mut cursive::Cursive) {
    siv.add_layer(Dialog::info(
        r#"
    chdig - v0.001

    General shortcuts:

    F1          - show help
    Enter       - show query logs (from system.text_log)
    Up/Down/j/k - navigate through the queries

    Tools:

    f           - query flamegraph
    F           - server flamegraph

    q/Esc/Backspace - go back
                               "#,
    ));
}
