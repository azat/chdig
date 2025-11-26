use cursive::theme::{ColorStyle, PaletteColor};
use cursive::traits::Resizable;
use cursive::view::Offset;
use cursive::views::{EditView, LinearLayout, TextView};
use cursive::{Cursive, XY};

/// Shows a less-style filter/options prompt at the bottom left of the screen
///
/// # Arguments
/// * `on_submit` - Callback to execute when the user submits the filter (presses Enter)
///
/// The filter prompt appears at the bottom-left corner with a `prefix`.
/// The callback receives the entered text (without the `prefix`).
pub fn show_bottom_prompt<F>(siv: &mut Cursive, prefix: &'static str, on_submit: F)
where
    F: Fn(&mut Cursive, &str) + 'static + Send + Sync,
{
    let prompt = TextView::new(prefix).style(ColorStyle::new(
        PaletteColor::Primary,
        PaletteColor::Background,
    ));
    let edit_view = EditView::new()
        .on_submit(on_submit)
        .style(ColorStyle::new(
            PaletteColor::Primary,
            PaletteColor::Background,
        ))
        .full_width();

    let filter_bar = LinearLayout::horizontal()
        .child(prompt)
        .child(edit_view)
        .full_width()
        .fixed_height(1);

    // Position at bottom left using add_transparent_layer_at
    let screen_size = siv.screen_size();
    let position = XY::new(
        Offset::Absolute(0),
        Offset::Absolute(screen_size.y.saturating_sub(1)),
    );

    siv.screen_mut()
        .add_transparent_layer_at(position, filter_bar);
}
