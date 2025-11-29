use crate::{
    interpreter::{ContextArc, WorkerEvent, options::ChDigViews},
    view::{self, navigation::Navigation, provider::ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::Dialog,
};
use std::collections::HashMap;

pub struct BackgroundSchedulePoolViewProvider;

impl ViewProvider for BackgroundSchedulePoolViewProvider {
    fn name(&self) -> &'static str {
        "Background jobs"
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

        let background_schedule_pool_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                show_logs_for_background_schedule_pool_task(siv, columns, row);
            };
        view.get_inner_mut()
            .set_on_submit(background_schedule_pool_logs_callback);

        let view = view.with_name("background_schedule_pool").full_screen();
        siv.set_main_view(Dialog::around(view).title("Background Schedule Pool"));
    }
}

fn show_logs_for_background_schedule_pool_task(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
) {
    let row = row.0;

    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row.iter()).for_each(|(c, r)| {
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
            log_name,
            database,
            table,
            view_options.start,
            view_options.end,
        ),
    );
}
