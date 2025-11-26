use crate::actions::ActionDescription;
use crate::interpreter::{ClickHouse, Worker, options::ChDigOptions};
use anyhow::Result;
use chrono::Duration;
use cursive::{Cursive, View, event::Event, event::EventResult, views::Dialog, views::OnEventView};
use std::sync::{Arc, Condvar, Mutex, atomic};

pub type ContextArc = Arc<Mutex<Context>>;

type GlobalActionCallback = Arc<Box<dyn Fn(&mut Cursive) + Send + Sync>>;
pub struct GlobalAction {
    pub description: ActionDescription,
    pub callback: GlobalActionCallback,
}

type ViewActionCallback =
    Arc<Box<dyn Fn(&mut dyn View) -> Result<Option<EventResult>> + Send + Sync>>;
pub struct ViewAction {
    pub description: ActionDescription,
    pub callback: ViewActionCallback,
}

pub struct Context {
    pub options: ChDigOptions,

    pub clickhouse: Arc<ClickHouse>,
    pub server_version: String,
    pub worker: Worker,
    pub background_runner_cv: Arc<(Mutex<()>, Condvar)>,
    pub background_runner_force: Arc<atomic::AtomicBool>,
    pub background_runner_summary_force: Arc<atomic::AtomicBool>,

    pub cb_sink: cursive::CbSink,

    pub global_actions: Vec<GlobalAction>,
    pub views_menu_actions: Vec<GlobalAction>,
    pub view_actions: Vec<ViewAction>,

    pub pending_view_callback: Option<ViewActionCallback>,
    pub view_registry: crate::view::ViewRegistry,

    pub search_history: crate::view::search_history::SearchHistory,
}

impl Context {
    pub async fn new(
        options: ChDigOptions,
        clickhouse: Arc<ClickHouse>,
        cb_sink: cursive::CbSink,
    ) -> Result<ContextArc> {
        let server_version = clickhouse.version();
        let worker = Worker::new();
        let background_runner_cv = Arc::new((Mutex::new(()), Condvar::new()));
        let background_runner_force = Arc::new(atomic::AtomicBool::new(false));
        let background_runner_summary_force = Arc::new(atomic::AtomicBool::new(false));

        let view_registry = crate::view::ViewRegistry::new();

        let context = Arc::new(Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            background_runner_cv,
            background_runner_force,
            background_runner_summary_force,
            cb_sink,
            global_actions: Vec::new(),
            views_menu_actions: Vec::new(),
            view_actions: Vec::new(),
            pending_view_callback: None,
            view_registry,
            search_history: crate::view::search_history::SearchHistory::new(),
        }));

        context.lock().unwrap().worker.start(context.clone());

        return Ok(context);
    }

    pub fn add_global_action<F, E>(
        &mut self,
        siv: &mut Cursive,
        text: &'static str,
        event: E,
        cb: F,
    ) where
        F: Fn(&mut Cursive) + Send + Sync + Copy + 'static,
        E: Into<Event>,
    {
        let event = event.into();
        let action = GlobalAction {
            description: ActionDescription { text, event },
            callback: Arc::new(Box::new(cb)),
        };
        siv.add_global_callback(action.description.event.clone(), cb);
        self.global_actions.push(action);
    }
    pub fn add_global_action_without_shortcut<F>(
        &mut self,
        siv: &mut Cursive,
        text: &'static str,
        cb: F,
    ) where
        F: Fn(&mut Cursive) + Send + Sync + Copy + 'static,
    {
        return self.add_global_action(siv, text, Event::Unknown(Vec::from([0u8])), cb);
    }

    pub fn add_view<F>(&mut self, text: &'static str, cb: F)
    where
        F: Fn(&mut Cursive) + Send + Sync + 'static,
    {
        let action = GlobalAction {
            description: ActionDescription {
                text,
                event: Event::Unknown(Vec::from([0u8])),
            },
            callback: Arc::new(Box::new(cb)),
        };
        self.views_menu_actions.push(action);
    }

    pub fn register_provider(&mut self, provider: Arc<dyn crate::view::ViewProvider>) {
        let name = provider.name();
        self.view_registry.register(provider);
        self.add_view(name, move |siv| {
            let context = siv.user_data::<ContextArc>().unwrap().clone();
            let provider = context.lock().unwrap().view_registry.get(name);
            provider.show(siv, context.clone());
        });
    }

    pub fn add_view_action<F, E, V>(
        &mut self,
        view: &mut OnEventView<V>,
        text: &'static str,
        event: E,
        cb: F,
    ) where
        F: Fn(&mut dyn View) -> Result<Option<EventResult>> + Send + Sync + Copy + 'static,
        E: Into<Event>,
        V: View,
    {
        let event = event.into();
        let action = ViewAction {
            description: ActionDescription { text, event },
            callback: Arc::new(Box::new(cb)),
        };
        let event = action.description.event.clone();
        let cb = action.callback.clone();
        view.set_on_event_inner(event, move |sub_view, _event| {
            let result = cb.as_ref()(sub_view);
            match result {
                Err(err) => {
                    return Some(EventResult::with_cb_once(move |siv: &mut Cursive| {
                        siv.add_layer(Dialog::info(err.to_string()));
                    }));
                }
                Ok(event) => return event,
            }
        });
        self.view_actions.push(action);
    }

    pub fn add_view_action_without_shortcut<F, V>(
        &mut self,
        view: &mut OnEventView<V>,
        text: &'static str,
        cb: F,
    ) where
        F: Fn(&mut dyn View) -> Result<Option<EventResult>> + Send + Sync + Copy + 'static,
        V: View,
    {
        return self.add_view_action(view, text, Event::Unknown(Vec::from([0u8])), cb);
    }

    pub fn trigger_view_refresh(&self) {
        self.background_runner_force
            .store(true, atomic::Ordering::SeqCst);
        self.background_runner_summary_force
            .store(true, atomic::Ordering::SeqCst);
        self.background_runner_cv.1.notify_all();
    }

    pub fn shift_time_interval(&mut self, is_sub: bool, minutes: i64) {
        let new_start = &mut self.options.view.start;
        let new_end = &mut self.options.view.end;

        if is_sub {
            *new_start -= Duration::try_minutes(minutes).unwrap();
            *new_end -= Duration::try_minutes(minutes).unwrap();
            log::debug!(
                "Set time frame to ({}, {}) ({} minutes backward)",
                new_start,
                new_end,
                minutes
            );
        } else {
            *new_start += Duration::try_minutes(minutes).unwrap();
            *new_end += Duration::try_minutes(minutes).unwrap();
            log::debug!(
                "Set time frame to ({}, {}) ({} minutes forward)",
                new_start,
                new_end,
                minutes
            );
        }
    }
}
