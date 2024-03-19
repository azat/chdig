use crate::interpreter::{options::ChDigOptions, ClickHouse, Worker};
use anyhow::Result;
use chdig::ActionDescription;
use cursive::{event::Event, event::EventResult, views::Dialog, views::OnEventView, Cursive, View};
use std::sync::{Arc, Condvar, Mutex};
use chrono::Duration;

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

    pub cb_sink: cursive::CbSink,

    pub global_actions: Vec<GlobalAction>,
    pub views_menu_actions: Vec<GlobalAction>,
    pub view_actions: Vec<ViewAction>,

    pub pending_view_callback: Option<ViewActionCallback>,
}

impl Context {
    pub async fn new(options: ChDigOptions, cb_sink: cursive::CbSink) -> Result<ContextArc> {
        let clickhouse = Arc::new(ClickHouse::new(options.clickhouse.clone()).await?);
        let server_version = clickhouse.version();
        let worker = Worker::new();
        let background_runner_cv = Arc::new((Mutex::new(()), Condvar::new()));

        let context = Arc::new(Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            background_runner_cv,
            cb_sink,
            global_actions: Vec::new(),
            views_menu_actions: Vec::new(),
            view_actions: Vec::new(),
            pending_view_callback: None,
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
        self.background_runner_cv.1.notify_all();
    }

    pub fn shift_time_interval(&mut self, is_sub: bool, minutes: i64) {
        let new_start = &mut self.options.view.start;
        let new_end = &mut self.options.view.end;

        if is_sub {
            *new_start -= Duration::minutes(minutes);
            *new_end -= Duration::minutes(minutes);
            log::debug!(
                "Set time frame to ({}, {}) (seeked to {} minutes backward)",
                new_start,
                new_end,
                minutes
            );
        } else {
            *new_start += Duration::minutes(minutes);
            *new_end += Duration::minutes(minutes);
            log::debug!(
                "Set time frame to ({}, {}) (seeked to {} minutes backward)",
                new_start,
                new_end,
                minutes
            );
        }
    }

}
