use super::app::App;
use super::component::{Component, Nameable, OnEventView};
use super::edit::EditView;
use super::event::{EventResult, Key};
use super::linear::LinearLayout;
use super::resize::Resizable;
use super::style::{Color, Modifier, Style, StyledString};
use super::text::TextView;
use crate::interpreter::ContextArc;
use std::sync::{Arc, Mutex};

/// Completion callback of show_bottom_prompt_with_suggestions(): candidates
/// for the token under the cursor (each replaces that whole token).
pub type Suggest = Arc<dyn Fn(&mut App, &str, usize) -> Vec<String> + Send + Sync>;

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
    show_bottom_prompt_impl(app, prefix, String::new(), None, None, on_submit);
}

/// show_bottom_prompt() pre-filled with `initial`, notifying `on_edit` on
/// every change and completing with Tab: `suggest` yields the candidates for
/// the token under the cursor, shown on a hint line above the prompt.
pub fn show_bottom_prompt_with_suggestions<E, F>(
    app: &mut App,
    prefix: &'static str,
    initial: String,
    on_edit: E,
    suggest: Suggest,
    on_submit: F,
) where
    E: Fn(&mut App, &str) + Send + Sync + 'static,
    F: Fn(&mut App, &str) + Send + Sync + 'static,
{
    show_bottom_prompt_impl(
        app,
        prefix,
        initial,
        Some(Arc::new(on_edit)),
        Some(suggest),
        on_submit,
    );
}

const HINT_VIEW: &str = "bottom_prompt_hint";

/// Tab completion state: the candidates for the token that was under the
/// cursor before the first Tab (frozen while cycling, since every accepted
/// candidate narrows what suggest() would return for the new text).
#[derive(Default)]
struct Completion {
    candidates: Vec<String>,
    /// Text and cursor the candidates were computed for
    base: (String, usize),
    /// The candidate currently applied (cycling position)
    index: Option<usize>,
    /// Text produced by the last Tab, to recognize its own on_edit
    applied: Option<String>,
}

impl Completion {
    fn reset(&mut self, candidates: Vec<String>, text: &str, cursor: usize) {
        self.candidates = candidates;
        self.base = (text.to_string(), cursor);
        self.index = None;
        self.applied = None;
    }

    /// The next (or previous) candidate applied to the base text: (text, cursor).
    fn step(&mut self, forward: bool) -> Option<(String, usize)> {
        // Hints (<...>) are not completions
        if self.candidates.is_empty() || self.candidates[0].starts_with('<') {
            return None;
        }
        let len = self.candidates.len();
        let index = match (self.index, forward) {
            (None, true) => 0,
            (None, false) => len - 1,
            (Some(i), true) => (i + 1) % len,
            (Some(i), false) => (i + len - 1) % len,
        };
        self.index = Some(index);
        let (text, cursor) = crate::interpreter::queries_filter::complete(
            &self.base.0,
            self.base.1,
            &self.candidates[index],
        );
        self.applied = Some(text.clone());
        Some((text, cursor))
    }

    fn hint(&self) -> StyledString {
        let mut hint = StyledString::new();
        for (i, candidate) in self.candidates.iter().enumerate() {
            if i > 0 {
                hint.append_plain("  ");
            }
            let style = if self.index == Some(i) {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            hint.append_styled(candidate.clone(), style);
        }
        hint
    }
}

fn show_bottom_prompt_impl<F>(
    app: &mut App,
    prefix: &'static str,
    initial: String,
    on_edit: Option<Arc<dyn Fn(&mut App, &str) + Send + Sync>>,
    suggest: Option<Suggest>,
    on_submit: F,
) where
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

    let completion = Arc::new(Mutex::new(Completion::default()));
    if let Some(suggest) = &suggest {
        let candidates = suggest(app, &initial, initial.len());
        completion
            .lock()
            .unwrap()
            .reset(candidates, &initial, initial.len());
    }
    let initial_hint = completion.lock().unwrap().hint();

    let edit_completion = completion.clone();
    let edit_suggest = suggest.clone();
    let mut edit_view = EditView::new()
        .content(initial)
        .style(Style::default())
        .on_submit(move |app: &mut App, text: &str| {
            if let Some(history) = &history_submit {
                history.add_entry(text.to_string());
            }
            on_submit(app, text);
        });
    if on_edit.is_some() || suggest.is_some() {
        edit_view = edit_view.on_edit(move |app: &mut App, text: &str, cursor: usize| {
            if let Some(suggest) = &edit_suggest {
                // Not for the text a Tab has just produced: the cycle goes on
                let own = edit_completion.lock().unwrap().applied.as_deref() == Some(text);
                if !own {
                    let candidates = suggest(app, text, cursor);
                    edit_completion
                        .lock()
                        .unwrap()
                        .reset(candidates, text, cursor);
                }
                let hint = edit_completion.lock().unwrap().hint();
                app.call_on_name(HINT_VIEW, |view: &mut TextView| {
                    view.set_content(hint);
                });
            }
            if let Some(on_edit) = &on_edit {
                on_edit(app, text);
            }
        });
    }

    // Tab/Shift-Tab cycle through the candidates
    let tab_completion = completion.clone();
    let backtab_completion = completion;
    let edit_with_history = OnEventView::new(edit_view)
        .on_pre_event_inner(Key::Tab, move |edit: &mut EditView, _event| {
            let Some((text, cursor)) = tab_completion.lock().unwrap().step(true) else {
                return Some(EventResult::consumed());
            };
            let cb = edit.set_content(text);
            edit.set_cursor(cursor);
            Some(EventResult::Consumed(Some(cb)))
        })
        .on_pre_event_inner(Key::BackTab, move |edit: &mut EditView, _event| {
            let Some((text, cursor)) = backtab_completion.lock().unwrap().step(false) else {
                return Some(EventResult::consumed());
            };
            let cb = edit.set_content(text);
            edit.set_cursor(cursor);
            Some(EventResult::Consumed(Some(cb)))
        })
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

    if suggest.is_some() {
        let hint = TextView::new(initial_hint).no_wrap().with_name(HINT_VIEW);
        let layer = LinearLayout::vertical()
            .child(hint.full_width().fixed_height(1))
            .child(filter_bar)
            .full_width()
            .fixed_height(2);
        let y = app.screen_size().height.saturating_sub(2);
        app.add_layer_at(0, y, layer);
    } else {
        let y = app.screen_size().height.saturating_sub(1);
        app.add_layer_at(0, y, filter_bar);
    }
}
