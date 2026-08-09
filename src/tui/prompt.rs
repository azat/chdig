use super::app::App;
use super::component::{Component, Nameable, OnEventView};
use super::edit::EditView;
use super::event::{EventResult, Key};
use super::linear::LinearLayout;
use super::resize::Resizable;
use super::style::{Style, StyledString};
use super::text::TextView;
use crate::interpreter::ContextArc;

/// Wraps a form (dialog content) so that Enter anywhere inside it submits the
/// form, instead of requiring to Tab to the submit button. Wrap only the
/// dialog content, not the whole Dialog, otherwise Enter on the dialog
/// buttons (e.g. Cancel) would be intercepted too.
pub fn submit_on_enter<V, F>(content: V, on_submit: F) -> OnEventView<V>
where
    V: Component,
    F: Fn(&mut App) + Send + Sync + 'static,
{
    OnEventView::new(content).on_pre_event(Key::Enter, on_submit)
}

/// Shows a less-style prompt at the bottom left of the screen. The callback
/// receives the entered text (without the `prefix`). Up/Down navigate the
/// search history.
pub fn show_bottom_prompt<F>(app: &mut App, prefix: &'static str, on_submit: F)
where
    F: Fn(&mut App, &str) + Send + Sync + 'static,
{
    let search_history = app
        .user_data::<ContextArc>()
        .map(|context| context.lock().unwrap().search_history.clone());

    if let Some(history) = &search_history {
        history.reset_index();
    }

    let prompt = TextView::new(StyledString::styled(prefix, Style::default())).no_wrap();

    let history_submit = search_history.clone();
    let history_up = search_history.clone();
    let history_down = search_history;

    let edit_view =
        EditView::new()
            .style(Style::default())
            .on_submit(move |app: &mut App, text: &str| {
                if let Some(history) = &history_submit {
                    history.add_entry(text.to_string());
                }
                on_submit(app, text);
            });

    let edit_with_history = OnEventView::new(edit_view)
        .on_pre_event_inner(Key::Up, move |edit: &mut EditView, _event| {
            if let Some(history) = &history_up {
                let current = edit.get_content();
                if let Some(prev) = history.navigate_up(&current) {
                    return Some(EventResult::Consumed(Some(edit.set_content(prev))));
                }
            }
            Some(EventResult::consumed())
        })
        .on_pre_event_inner(Key::Down, move |edit: &mut EditView, _event| {
            if let Some(history) = &history_down
                && let Some(next) = history.navigate_down()
            {
                return Some(EventResult::Consumed(Some(edit.set_content(next))));
            }
            Some(EventResult::consumed())
        });

    let filter_bar = LinearLayout::horizontal()
        .child(prompt)
        .child(edit_with_history.with_name("bottom_prompt").full_width())
        .full_width()
        .fixed_height(1);

    let y = app.screen_size().height.saturating_sub(1);
    app.add_layer_at(0, y, filter_bar);
}
