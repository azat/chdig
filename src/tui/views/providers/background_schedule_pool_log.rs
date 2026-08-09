// Registered in providers::register() once the views menu wiring is ported.

use crate::{
    interpreter::{ContextArc, TextLogArguments, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, LinearLayout, Nameable, NamedView, Navigation, Resizable,
        SizeConstraint, TextView, ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
        views::text_log_view::TextLogView,
    },
};
use std::collections::HashMap;

pub struct BackgroundSchedulePoolLogViewProvider;

impl ViewProvider for BackgroundSchedulePoolLogViewProvider {
    fn name(&self) -> &'static str {
        "Background Tasks History"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::BackgroundSchedulePoolLog
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_background_schedule_pool_log(app, context, None, None, None);
    }
}

struct FilterParams {
    log_name: Option<String>,
    database: Option<String>,
    table: Option<String>,
}

impl FilterParams {
    fn build_where_clauses(&self) -> Vec<String> {
        let mut clauses = vec![
            "event_date BETWEEN toDate(start_) AND toDate(end_)".to_string(),
            "event_time BETWEEN toDateTime(start_) AND toDateTime(end_)".to_string(),
        ];

        if let Some(ref log_name) = self.log_name {
            clauses.push(format!("log_name = '{}'", log_name.replace('\'', "''")));
        }
        if let Some(ref database) = self.database {
            clauses.push(format!("database = '{}'", database.replace('\'', "''")));
        }
        if let Some(ref table) = self.table {
            clauses.push(format!("table = '{}'", table.replace('\'', "''")));
        }

        clauses
    }

    fn build_title(&self, for_dialog: bool) -> String {
        match (&self.log_name, &self.database, &self.table) {
            (Some(ln), _, _) => {
                if for_dialog {
                    format!("Task summary: {}", ln)
                } else {
                    format!("Background Tasks Logs: {}", ln)
                }
            }
            (None, Some(db), Some(tbl)) => {
                if for_dialog {
                    format!("Tasks for: {}.{}", db, tbl)
                } else {
                    format!("Background Tasks Logs: {}.{}", db, tbl)
                }
            }
            (None, Some(db), None) => {
                if for_dialog {
                    format!("Tasks for: {}", db)
                } else {
                    format!("Background Tasks Logs: {}", db)
                }
            }
            (None, None, Some(tbl)) => {
                if for_dialog {
                    format!("Tasks for table: {}", tbl)
                } else {
                    format!("Background Tasks Logs: table {}", tbl)
                }
            }
            (None, None, None) => "Background Tasks Logs".to_string(),
        }
    }

    fn generate_view_name(&self) -> String {
        format!(
            "background_schedule_pool_log_{}_{}_{}",
            self.log_name.as_deref().unwrap_or("any"),
            self.database.as_deref().unwrap_or("any"),
            self.table.as_deref().unwrap_or("any")
        )
    }
}

fn build_query(context: &ContextArc, filters: &FilterParams) -> String {
    let (view_options, limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.options.clickhouse.limit,
            ctx.clickhouse
                .get_log_table_name("background_schedule_pool_log"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let start_sql = view_options
        .start
        .to_sql_datetime_64()
        .unwrap_or_else(|| "now() - INTERVAL 1 HOUR".to_string());
    let end_sql = view_options
        .end
        .to_sql_datetime_64()
        .unwrap_or_else(|| "now()".to_string());

    let mut where_clauses = filters.build_where_clauses();

    let host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref());
    if !host_filter.is_empty() {
        where_clauses.push(format!("1 {}", host_filter));
    }

    format!(
        r#"
        WITH {start} AS start_, {end} AS end_
        SELECT event_time, log_name, database, table, query_id, duration_ms, error, exception
        FROM {dbtable}
        WHERE
            {where_clause}
        ORDER BY event_time DESC
        LIMIT {limit}
        "#,
        start = start_sql,
        end = end_sql,
        dbtable = dbtable,
        where_clause = where_clauses.join(" AND "),
        limit = limit,
    )
}

fn get_columns() -> (Vec<&'static str>, Vec<&'static str>) {
    let columns = vec![
        "event_time",
        "log_name",
        "database",
        "table",
        "query_id",
        "duration_ms",
        "error",
        "exception",
    ];
    let columns_to_compare = vec!["event_time", "log_name", "database", "table"];
    (columns, columns_to_compare)
}

fn show_task_logs(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
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
    let query_id = map
        .get("query_id")
        .map(|s| s.to_owned())
        .unwrap_or_default();

    if query_id.is_empty() {
        return;
    }

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.clone().lock().unwrap().options.view.clone();

    app.add_layer(Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new(format!("Logs for {} ({})", log_name, query_id)).center())
            .child(DummyView.fixed_height(1))
            .child(NamedView::new(
                "background_task_logs",
                TextLogView::new(
                    "background_task_logs",
                    context,
                    TextLogArguments {
                        query_ids: Some(vec![query_id]),
                        logger_names: None,
                        hostname: None,
                        message_filter: None,
                        max_level: None,
                        start: view_options.start.into(),
                        end: view_options.end,
                    },
                ),
            )),
    ));
    app.focus_name("background_task_logs");
}

pub fn show_background_schedule_pool_log(
    app: &mut App,
    context: ContextArc,
    log_name: Option<String>,
    database: Option<String>,
    table: Option<String>,
) {
    let view_name = "background_schedule_pool_log";

    if app.has_view(view_name) {
        return;
    }

    let filters = FilterParams {
        log_name,
        database,
        table,
    };

    let query = build_query(&context, &filters);
    let (columns, columns_to_compare) = get_columns();

    let mut view = SQLQueryView::new(
        context.clone(),
        view_name,
        "event_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut().set_on_submit(show_task_logs);

    view.get_inner_mut().set_title(filters.build_title(false));

    app.present_view(view_name, view.with_name(view_name).full_screen());
}

pub fn show_background_schedule_pool_log_dialog(
    app: &mut App,
    context: ContextArc,
    log_name: Option<String>,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = FilterParams {
        log_name,
        database,
        table,
    };

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let query = build_query(&context, &filters);
    let (columns, columns_to_compare) = get_columns();

    let mut sql_view = SQLQueryView::new(
        context.clone(),
        view_name,
        "event_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view.get_inner_mut().set_on_submit(show_task_logs);
    sql_view
        .get_inner_mut()
        .set_title(filters.build_title(true));

    app.add_layer(
        Dialog::around(
            sql_view
                .with_name(view_name)
                .resized(SizeConstraint::AtLeast(140), SizeConstraint::AtLeast(30)),
        )
        .title("Background Schedule Pool Logs"),
    );
}
