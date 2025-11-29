use crate::interpreter::ContextArc;
use cursive::event::Key;
use cursive::theme::{ColorStyle, PaletteColor};
use cursive::traits::Resizable;
use cursive::view::Offset;
use cursive::views::{EditView, LinearLayout, OnEventView, ResizedView, TextView};
use cursive::{Cursive, XY};

/// Shows a less-style filter/options prompt at the bottom left of the screen
///
/// # Arguments
/// * `on_submit` - Callback to execute when the user submits the filter (presses Enter)
///
/// The filter prompt appears at the bottom-left corner with a `prefix`.
/// The callback receives the entered text (without the `prefix`).
/// Supports Up/Down arrow keys to navigate through search history.
///
/// TODO: add a callback in case of view has been removed w/o any item selected
pub fn show_bottom_prompt<F>(siv: &mut Cursive, prefix: &'static str, on_submit: F)
where
    F: Fn(&mut Cursive, &str) + 'static + Send + Sync,
{
    // Get search history from context
    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let search_history = context.lock().unwrap().search_history.clone();
    let search_history_submit = search_history.clone();

    search_history.reset_index();

    let prompt = TextView::new(prefix).style(ColorStyle::new(
        PaletteColor::Primary,
        PaletteColor::Background,
    ));

    let search_history_up = search_history.clone();
    let search_history_down = search_history.clone();

    let edit_view = EditView::new()
        .on_submit(move |siv: &mut Cursive, text: &str| {
            // Add to history before calling the callback
            search_history_submit.add_entry(text.to_string());
            on_submit(siv, text);
        })
        .style(ColorStyle::new(
            PaletteColor::Primary,
            PaletteColor::Background,
        ))
        .full_width();

    let edit_with_history = OnEventView::new(edit_view)
        .on_pre_event_inner(Key::Up, move |v: &mut ResizedView<EditView>, _event| {
            let edit = v.get_inner_mut();
            let current = edit.get_content();
            if let Some(prev) = search_history_up.navigate_up(&current) {
                edit.set_content(prev);
            }
            Some(cursive::event::EventResult::Consumed(None))
        })
        .on_pre_event_inner(Key::Down, move |v: &mut ResizedView<EditView>, _event| {
            let edit = v.get_inner_mut();
            if let Some(next) = search_history_down.navigate_down() {
                edit.set_content(next);
            }
            Some(cursive::event::EventResult::Consumed(None))
        });

    let filter_bar = LinearLayout::horizontal()
        .child(prompt)
        .child(edit_with_history)
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
