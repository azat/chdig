use crate::actions::ActionDescription;
use crate::interpreter::{
    ClickHouse, Worker,
    debug_metrics::DebugMetrics,
    options::{ChDigOptions, ChDigViews},
    perfetto::PerfettoServer,
};
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
    /// Name of the view the action belongs to (actions of several live views
    /// can coexist, each view drops only its own).
    pub owner: &'static str,
    pub description: ActionDescription,
    pub callback: ViewActionCallback,
}

pub struct Context {
    pub options: ChDigOptions,

    pub clickhouse: Arc<ClickHouse>,
    pub server_version: String,
    pub worker: Worker,
    pub background_runner_cv: Arc<(Mutex<()>, Condvar)>,
    // Bumped by trigger_view_refresh(); the summary is deliberately not
    // subscribed to it (it has its own generation below) - switching views
    // does not invalidate the summary.
    pub background_runner_generation: Arc<atomic::AtomicU64>,
    // Bumped only by trigger_full_refresh() (Shift-R).
    pub background_runner_summary_generation: Arc<atomic::AtomicU64>,

    pub cb_sink: cursive::CbSink,

    pub cursive_global_actions: Vec<GlobalAction>,
    pub cursive_views_menu_actions: Vec<GlobalAction>,
    pub cursive_view_actions: Vec<ViewAction>,

    pub cursive_pending_view_callback: Option<ViewActionCallback>,
    pub cursive_view_registry: crate::view::ViewRegistry,

    pub ui_sink: crate::tui::UiSink,

    pub global_actions: Vec<crate::tui::actions::GlobalAction>,
    pub views_menu_actions: Vec<crate::tui::actions::GlobalAction>,
    pub view_actions: Vec<crate::tui::actions::ViewAction>,

    pub pending_view_callback: Option<crate::tui::actions::ViewActionCallback>,
    pub view_registry: crate::tui::ViewRegistry,

    pub search_history: crate::tui::views::search_history::SearchHistory,

    pub selected_host: Option<String>,
    pub current_view: Option<ChDigViews>,
    pub view_history: Vec<ChDigViews>,

    pub perfetto_server: Option<Arc<PerfettoServer>>,

    pub queries_filter: Arc<Mutex<String>>,
    pub queries_limit: Arc<Mutex<u64>>,
    pub query_patterns_metric:
        &'static crate::tui::views::providers::query_patterns_metrics::Metric,

    pub debug_metrics: Arc<DebugMetrics>,
}

impl Context {
    pub async fn new(
        options: ChDigOptions,
        clickhouse: Arc<ClickHouse>,
        cb_sink: cursive::CbSink,
    ) -> Result<ContextArc> {
        let server_version = clickhouse.version();
        let debug_metrics = DebugMetrics::new();
        let worker = Worker::new();
        let background_runner_cv = Arc::new((Mutex::new(()), Condvar::new()));
        let background_runner_generation = Arc::new(atomic::AtomicU64::new(0));
        let background_runner_summary_generation = Arc::new(atomic::AtomicU64::new(0));

        let cursive_view_registry = crate::view::ViewRegistry::new();

        let queries_filter = Arc::new(Mutex::new(String::new()));
        let queries_limit = Arc::new(Mutex::new(options.view.queries_limit));
        let query_patterns_metric =
            crate::tui::views::providers::query_patterns_metrics::default_metric();

        // Metrics are always collected; display is toggled with `!`. The refresh thread
        // sleeps when hidden, so this is free when unused.
        debug_metrics.spawn_refresh(cb_sink.clone(), std::time::Duration::from_millis(500));

        let context = Arc::new(Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            background_runner_cv,
            background_runner_generation,
            background_runner_summary_generation,
            cb_sink,
            cursive_global_actions: Vec::new(),
            cursive_views_menu_actions: Vec::new(),
            cursive_view_actions: Vec::new(),
            cursive_pending_view_callback: None,
            cursive_view_registry,
            // Dangling until the ratatui App attaches (Context::new gets the
            // real sink once cursive is gone).
            ui_sink: crossbeam_channel::unbounded().0,
            global_actions: Vec::new(),
            views_menu_actions: Vec::new(),
            view_actions: Vec::new(),
            pending_view_callback: None,
            view_registry: crate::tui::ViewRegistry::new(),
            search_history: crate::tui::views::search_history::SearchHistory::new(),
            selected_host: None,
            current_view: None,
            view_history: Vec::new(),
            perfetto_server: None,
            queries_filter,
            queries_limit,
            query_patterns_metric,
            debug_metrics,
        }));

        context.lock().unwrap().worker.start(context.clone());

        return Ok(context);
    }

    pub fn cursive_add_global_action<F, E>(
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
        self.cursive_global_actions.push(action);
    }
    pub fn cursive_add_global_action_without_shortcut<F>(
        &mut self,
        siv: &mut Cursive,
        text: &'static str,
        cb: F,
    ) where
        F: Fn(&mut Cursive) + Send + Sync + Copy + 'static,
    {
        return self.cursive_add_global_action(siv, text, Event::Unknown(Vec::from([0u8])), cb);
    }

    pub fn cursive_add_view<F>(&mut self, text: &'static str, cb: F)
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
        self.cursive_views_menu_actions.push(action);
    }

    /// Switch the current view, remembering the previous one in the history
    /// (for going back on Backspace).
    pub fn set_current_view(&mut self, view: ChDigViews) {
        if self.current_view == Some(view) {
            return;
        }
        if let Some(current) = self.current_view {
            self.view_history.push(current);
        }
        self.current_view = Some(view);
    }

    pub fn cursive_register_provider(&mut self, provider: Arc<dyn crate::view::ViewProvider>) {
        let name = provider.name();
        self.cursive_view_registry.register(provider);
        self.cursive_add_view(name, move |siv| {
            let context = siv.user_data::<ContextArc>().unwrap().clone();
            let provider = context.lock().unwrap().cursive_view_registry.get(name);
            {
                let mut ctx = context.lock().unwrap();
                ctx.set_current_view(provider.view_type());
            }
            provider.show(siv, context.clone());
        });
    }

    pub fn cursive_add_view_action<F, E, V>(
        &mut self,
        view: &mut OnEventView<V>,
        owner: &'static str,
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
            owner,
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
        self.cursive_view_actions.push(action);
    }

    pub fn cursive_add_view_action_without_shortcut<F, V>(
        &mut self,
        view: &mut OnEventView<V>,
        owner: &'static str,
        text: &'static str,
        cb: F,
    ) where
        F: Fn(&mut dyn View) -> Result<Option<EventResult>> + Send + Sync + Copy + 'static,
        V: View,
    {
        return self.cursive_add_view_action(
            view,
            owner,
            text,
            Event::Unknown(Vec::from([0u8])),
            cb,
        );
    }

    pub fn add_global_action<F, E>(
        &mut self,
        app: &mut crate::tui::App,
        text: &'static str,
        event: E,
        cb: F,
    ) where
        F: Fn(&mut crate::tui::App) + Send + Sync + Copy + 'static,
        E: Into<crate::tui::Event>,
    {
        let event = event.into();
        let action = crate::tui::actions::GlobalAction {
            description: crate::tui::actions::ActionDescription { text, event },
            callback: Arc::new(cb),
        };
        app.add_global_callback(action.description.event.clone(), cb);
        self.global_actions.push(action);
    }

    pub fn add_global_action_without_shortcut<F>(
        &mut self,
        app: &mut crate::tui::App,
        text: &'static str,
        cb: F,
    ) where
        F: Fn(&mut crate::tui::App) + Send + Sync + Copy + 'static,
    {
        self.add_global_action(app, text, crate::tui::Event::Unknown(Vec::from([0u8])), cb);
    }

    pub fn add_view<F>(&mut self, text: &'static str, cb: F)
    where
        F: Fn(&mut crate::tui::App) + Send + Sync + 'static,
    {
        let action = crate::tui::actions::GlobalAction {
            description: crate::tui::actions::ActionDescription {
                text,
                event: crate::tui::Event::Unknown(Vec::from([0u8])),
            },
            callback: Arc::new(cb),
        };
        self.views_menu_actions.push(action);
    }

    pub fn register_provider(&mut self, provider: Arc<dyn crate::tui::ViewProvider>) {
        let name = provider.name();
        self.view_registry.register(provider);
        self.add_view(name, move |app| {
            let context = app.user_data::<ContextArc>().unwrap().clone();
            let provider = context.lock().unwrap().view_registry.get(name);
            {
                let mut ctx = context.lock().unwrap();
                ctx.set_current_view(provider.view_type());
            }
            provider.show(app, context.clone());
        });
    }

    pub fn add_view_action<F, E, V>(
        &mut self,
        view: &mut crate::tui::OnEventView<V>,
        owner: &'static str,
        text: &'static str,
        event: E,
        cb: F,
    ) where
        F: Fn(&mut dyn crate::tui::Component) -> Result<Option<crate::tui::EventResult>>
            + Send
            + Sync
            + Copy
            + 'static,
        E: Into<crate::tui::Event>,
        V: crate::tui::Component,
    {
        let event = event.into();
        let action = crate::tui::actions::ViewAction {
            owner,
            description: crate::tui::actions::ActionDescription { text, event },
            callback: Arc::new(cb),
        };
        let event = action.description.event.clone();
        let cb = action.callback.clone();
        view.set_on_event_inner(event, move |sub_view, _event| match cb.as_ref()(sub_view) {
            Err(err) => {
                let err = err.to_string();
                Some(crate::tui::EventResult::with_cb_once(
                    move |app: &mut crate::tui::App| {
                        app.add_layer(crate::tui::Dialog::info(err));
                    },
                ))
            }
            Ok(result) => result,
        });
        self.view_actions.push(action);
    }

    pub fn add_view_action_without_shortcut<F, V>(
        &mut self,
        view: &mut crate::tui::OnEventView<V>,
        owner: &'static str,
        text: &'static str,
        cb: F,
    ) where
        F: Fn(&mut dyn crate::tui::Component) -> Result<Option<crate::tui::EventResult>>
            + Send
            + Sync
            + Copy
            + 'static,
        V: crate::tui::Component,
    {
        self.add_view_action(
            view,
            owner,
            text,
            crate::tui::Event::Unknown(Vec::from([0u8])),
            cb,
        );
    }

    pub fn get_or_start_perfetto_server(&mut self) -> Arc<PerfettoServer> {
        if let Some(ref server) = self.perfetto_server {
            return server.clone();
        }
        let server = Arc::new(PerfettoServer::new());
        self.perfetto_server = Some(server.clone());
        server
    }

    pub fn trigger_view_refresh(&self) {
        self.background_runner_generation
            .fetch_add(1, atomic::Ordering::SeqCst);
        self.background_runner_cv.1.notify_all();
    }

    pub fn trigger_full_refresh(&self) {
        self.background_runner_summary_generation
            .fetch_add(1, atomic::Ordering::SeqCst);
        self.trigger_view_refresh();
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
