use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use std::sync::Arc;

use super::actions::ActionDescription;
use super::app::App;
use super::component::{Nameable, OnEventView};
use super::edit::EditView;
use super::event::{Event, Key};
use super::linear::LinearLayout;
use super::panel::Panel;
use super::select::SelectView;

pub fn fuzzy_actions<F>(app: &mut App, actions: Vec<ActionDescription>, on_select: F)
where
    F: Fn(&mut App, String) + Send + Sync + 'static,
{
    let items: Vec<(String, String)> = actions
        .iter()
        .map(|a| {
            let text = a.text.to_string();
            (text.clone(), text)
        })
        .collect();
    fuzzy_select_strings(app, "Fuzzy search", items, on_select);
}

/// Modal fuzzy picker: an edit line filtering a list below it.
/// `items` are (label, value) pairs; `on_select` receives the value.
pub fn fuzzy_select_strings<F>(
    app: &mut App,
    title: &str,
    items: Vec<(String, String)>,
    on_select: F,
) where
    F: Fn(&mut App, String) + Send + Sync + 'static,
{
    if app.has_view("fuzzy_search") {
        return;
    }

    let on_select: Arc<dyn Fn(&mut App, String) + Send + Sync> = Arc::new(on_select);

    let mut select = SelectView::<String>::new().autojump();
    for (label, value) in &items {
        select.add_item(label.clone(), value.clone());
    }
    {
        let on_select = on_select.clone();
        select.set_on_submit(move |app, value: &String| {
            let selected = value.clone();
            app.pop_layer();
            on_select(app, selected);
        });
    }

    let submit_on_select = on_select.clone();
    let search = EditView::new()
        .on_edit(move |app, query, _cursor| {
            let query = query.to_string();
            app.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.clear();

                let matcher = SkimMatcherV2::default();
                let query_words: Vec<&str> = query.split_whitespace().collect();

                let mut matches: Vec<(i64, String, String)> = items
                    .iter()
                    .filter_map(|(label, value)| {
                        if query_words.is_empty() {
                            return Some((0, label.clone(), value.clone()));
                        }

                        let mut total_score = 0i64;
                        for word in &query_words {
                            match matcher.fuzzy_match(label, word) {
                                Some(score) => total_score += score,
                                None => return None,
                            }
                        }

                        Some((total_score, label.clone(), value.clone()))
                    })
                    .collect();

                matches.sort_by(|a, b| b.0.cmp(&a.0));

                for (_, label, value) in matches {
                    view.add_item(label, value);
                }
            });
        })
        .on_submit(move |app, _content| {
            // Enter in the edit field submits the best (top) match.
            let selected = app.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.set_selection(0);
                view.selection().cloned()
            });
            if let Some(Some(selected)) = selected {
                app.pop_layer();
                submit_on_select(app, selected);
            }
        })
        .with_name("fuzzy_search");

    let layout = LinearLayout::vertical()
        .child(search)
        .child(select.with_name("fuzzy_select"));

    let dialog = OnEventView::new(Panel::new(layout).title(title.to_string()))
        .on_pre_event(Event::CtrlChar('k'), |app| {
            app.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.select_up(1);
            });
        })
        .on_pre_event(Event::CtrlChar('j'), |app| {
            app.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.select_down(1);
            });
        })
        // Swallow Backspace on empty input (would otherwise trigger the
        // global Back action).
        .on_event(Key::Backspace, |_| {})
        .on_event(Event::CtrlChar('p'), |app| {
            app.pop_layer();
        })
        .on_event(Key::Esc, |app| {
            app.pop_layer();
        });

    app.add_layer(dialog);
    app.focus_name("fuzzy_search");
}
