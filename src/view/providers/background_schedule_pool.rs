use crate::{
    actions::ActionDescription,
    interpreter::{ContextArc, WorkerEvent, options::ChDigViews},
    utils::fuzzy_actions,
    view::{self, navigation::Navigation, provider::ViewProvider},
};
use cursive::{
    Cursive,
    event::Event,
    view::{Nameable, Resizable},
    views::Dialog,
};
use std::collections::HashMap;

pub struct BackgroundSchedulePoolViewProvider;

impl ViewProvider for BackgroundSchedulePoolViewProvider {
    fn name(&self) -> &'static str {
        "Background Tasks"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::BackgroundSchedulePool
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("background_schedule_pool") {
            return;
        }

        let mut columns = vec![
            "pool",
            "database",
            "table",
            "log_name",
            "query_id",
            "elapsed_ms",
            "executing",
            "scheduled",
            "delayed",
        ];

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let columns_to_compare = if cluster {
            columns.insert(0, "hostName() host");
            vec!["host", "pool", "database", "table", "log_name"]
        } else {
            vec!["pool", "database", "table", "log_name"]
        };

        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "background_schedule_pool");

        let query = format!(
            "SELECT {} FROM {} ORDER BY pool, database, table, log_name",
            columns.join(", "),
            dbtable,
        );

        siv.drop_main_view();

        let mut view = view::SQLQueryView::new(
            context.clone(),
            "background_schedule_pool",
            "elapsed_ms",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get background_schedule_pool"));

        let background_schedule_pool_action_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                show_background_schedule_pool_actions(siv, columns, row);
            };
        view.get_inner_mut()
            .set_on_submit(background_schedule_pool_action_callback);

        let view = view.with_name("background_schedule_pool").full_screen();
        siv.set_main_view(Dialog::around(view).title("Background Schedule Pool"));
    }
}

fn show_background_schedule_pool_actions(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
) {
    let actions = vec![
        ActionDescription {
            text: "Show tasks logs",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show tasks",
            event: Event::Unknown(vec![]),
        },
    ];

    let columns_clone = columns.clone();
    let row_clone = row.clone();

    fuzzy_actions(siv, actions, move |siv, selected| match selected.as_str() {
        "Show tasks logs" => {
            show_tasks_logs(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show tasks" => {
            show_tasks_summary(siv, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

fn show_tasks_logs(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let log_name = map
        .get("log_name")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let database = map
        .get("database")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let table = map.get("table").map(|s| s.to_owned()).unwrap_or_default();

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.clone().lock().unwrap().options.view.clone();

    context.lock().unwrap().worker.send(
        true,
        WorkerEvent::BackgroundSchedulePoolLogs(
            Some(log_name),
            database,
            table,
            view_options.start,
            view_options.end,
        ),
    );
}

fn show_tasks_summary(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let log_name = map.get("log_name").map(|s| s.to_owned());
    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());

    let context = siv.user_data::<ContextArc>().unwrap().clone();

    super::background_schedule_pool_log::show_background_schedule_pool_log_dialog(
        siv, context, log_name, database, table,
    );
}
