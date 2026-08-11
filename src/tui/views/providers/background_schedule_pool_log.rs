use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, TextLogArguments, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, LinearLayout, NamedView, Resizable, TextView, ViewProvider,
        views::sql_query_view::Row as QueryResultRow, views::text_log_view::TextLogView,
    },
};
use std::collections::HashMap;

pub struct BackgroundSchedulePoolLogViewProvider;

impl ViewProvider for BackgroundSchedulePoolLogViewProvider {
    fn name(&self) -> &'static str {
        "Background Tasks History"
    }

    fn view_name(&self) -> Option<&'static str> {
        Some("background_schedule_pool_log")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::BackgroundSchedulePoolLog
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_background_schedule_pool_log(app, context, None, None, None, Presentation::FullScreen);
    }
}

const COLUMNS: &[&str] = &[
    "event_time",
    "log_name",
    "database",
    "table",
    "query_id",
    "duration_ms",
    "error",
    "exception",
];

fn build_title(
    log_name: &Option<String>,
    database: &Option<String>,
    table: &Option<String>,
    for_dialog: bool,
) -> String {
    match (log_name, database, table) {
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

fn build_query(context: &ContextArc, filters: &TableFilterParams) -> String {
    let (limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.clickhouse.limit,
            ctx.clickhouse
                .get_log_table_name("background_schedule_pool_log"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let (with_prelude, mut where_clauses) = super::log_time_window(context);
    where_clauses.extend(filters.build_where_clauses());
    super::push_host_filter(
        &mut where_clauses,
        &clickhouse,
        selected_host.as_ref(),
        true,
    );

    format!(
        r#"
        {with_prelude}
        SELECT {select_clause}
        FROM {dbtable}
        WHERE
            {where_clause}
        ORDER BY event_time DESC
        LIMIT {limit}
        "#,
        with_prelude = with_prelude,
        select_clause = COLUMNS.join(", "),
        dbtable = dbtable,
        where_clause = where_clauses.join(" AND "),
        limit = limit,
    )
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
    presentation: Presentation,
) {
    let title = build_title(&log_name, &database, &table, presentation.is_dialog());
    let filters = TableFilterParams::new(
        database,
        table,
        "background_schedule_pool_log",
        "Background Tasks Logs",
    )
    .with_eq("log_name", log_name);

    // The dialog keeps the database/table columns: it can be scoped to a
    // log_name only.
    let spec = QueryTableSpec {
        view_name: filters.view_name(presentation),
        title,
        dialog_title: "Background Schedule Pool Logs".to_string(),
        sort_by: "event_time",
        query: build_query(&context, &filters),
        columns: COLUMNS.to_vec(),
        columns_to_compare: vec!["event_time", "log_name", "database", "table"],
        wide_columns: vec!["exception"],
    };
    super::present_query_table(app, context, spec, show_task_logs, presentation);
}
