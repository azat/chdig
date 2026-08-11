use crate::interpreter::{
    ClickHouse, Worker,
    debug_metrics::DebugMetrics,
    options::{ChDigOptions, ChDigViews},
    perfetto::PerfettoServer,
};
use anyhow::Result;
use chrono::Duration;
use std::sync::{Arc, Condvar, Mutex, atomic};

pub type ContextArc = Arc<Mutex<Context>>;

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

    pub ui_sink: crate::tui::UiSink,

    pub global_actions: Vec<crate::tui::actions::GlobalAction>,
    pub views_menu_actions: Vec<crate::tui::actions::GlobalAction>,
    pub view_actions: Vec<crate::tui::actions::ViewAction>,

    pub view_registry: crate::tui::ViewRegistry,

    pub search_history: crate::tui::views::search_history::SearchHistory,

    pub selected_host: Option<String>,
    pub current_view: Option<ChDigViews>,
    pub view_history: Vec<ChDigViews>,

    pub perfetto_server: Option<Arc<PerfettoServer>>,

    /// Per-view '/'-filters of the queries views, keyed by view name; entries
    /// outlive the views so the filter survives switching views.
    queries_filters: std::collections::HashMap<&'static str, Arc<Mutex<String>>>,
    pub queries_limit: Arc<Mutex<u64>>,
    pub query_patterns_metric:
        &'static crate::tui::views::providers::query_patterns_metrics::Metric,

    pub debug_metrics: Arc<DebugMetrics>,
}

impl Context {
    pub async fn new(
        options: ChDigOptions,
        clickhouse: Arc<ClickHouse>,
        ui_sink: crate::tui::UiSink,
    ) -> Result<ContextArc> {
        let server_version = clickhouse.version();
        let debug_metrics = DebugMetrics::new();
        let worker = Worker::new();
        let background_runner_cv = Arc::new((Mutex::new(()), Condvar::new()));
        let background_runner_generation = Arc::new(atomic::AtomicU64::new(0));
        let background_runner_summary_generation = Arc::new(atomic::AtomicU64::new(0));

        let queries_limit = Arc::new(Mutex::new(options.view.queries_limit));
        let query_patterns_metric =
            crate::tui::views::providers::query_patterns_metrics::default_metric();

        // Metrics are always collected; display is toggled with `!`. The refresh thread
        // sleeps when hidden, so this is free when unused.
        debug_metrics.spawn_refresh(ui_sink.clone(), std::time::Duration::from_millis(500));

        let context = Arc::new(Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            background_runner_cv,
            background_runner_generation,
            background_runner_summary_generation,
            ui_sink,
            global_actions: Vec::new(),
            views_menu_actions: Vec::new(),
            view_actions: Vec::new(),
            view_registry: crate::tui::ViewRegistry::new(),
            search_history: crate::tui::views::search_history::SearchHistory::new(),
            selected_host: None,
            current_view: None,
            view_history: Vec::new(),
            perfetto_server: None,
            queries_filters: std::collections::HashMap::new(),
            queries_limit,
            query_patterns_metric,
            debug_metrics,
        }));

        context.lock().unwrap().worker.start(context.clone());

        return Ok(context);
    }

    /// Configured initial '/'-filter for the view whose main widget is
    /// `view_name` (`views:` config section).
    pub fn view_filter_seed(&self, view_name: &str) -> Option<String> {
        let view_type = self.view_registry.view_type_by_view_name(view_name)?;
        self.options.views.get(&view_type)?.filter.clone()
    }

    /// The '/'-filter of a queries view, created on first use (seeded from the
    /// config).
    pub fn queries_filter(&mut self, view_name: &'static str) -> Arc<Mutex<String>> {
        if let Some(filter) = self.queries_filters.get(view_name) {
            return filter.clone();
        }
        let seed = self.view_filter_seed(view_name).unwrap_or_default();
        let filter = Arc::new(Mutex::new(seed));
        self.queries_filters.insert(view_name, filter.clone());
        filter
    }

    /// Queries filter edited in the settings dialog: the current queries
    /// view's one (falls back to the processes view when the current view is
    /// not a queries view).
    pub fn settings_queries_filter(&mut self) -> Arc<Mutex<String>> {
        let view_type = match self.current_view {
            Some(
                view @ (ChDigViews::Queries | ChDigViews::SlowQueries | ChDigViews::LastQueries),
            ) => view,
            _ => ChDigViews::Queries,
        };
        let view_name = self
            .view_registry
            .get_by_view_type(view_type)
            .view_name()
            .unwrap();
        self.queries_filter(view_name)
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
        view.set_on_event_inner(event.clone(), move |sub_view, _event| match cb(sub_view) {
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
        self.view_actions.push(crate::tui::actions::ViewAction {
            owner,
            description: crate::tui::actions::ActionDescription { text, event },
        });
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
            crate::tui::actions::synthetic_event(),
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
