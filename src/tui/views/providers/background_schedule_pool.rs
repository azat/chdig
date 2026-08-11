use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, WorkerEvent, options::ChDigViews},
    tui::{
        App, Event, ViewProvider, actions::ActionDescription, fuzzy_actions,
        views::sql_query_view::Row as QueryResultRow,
    },
};
use std::collections::HashMap;

pub struct BackgroundSchedulePoolViewProvider;

impl ViewProvider for BackgroundSchedulePoolViewProvider {
    fn name(&self) -> &'static str {
        "Background Tasks"
    }

    fn view_name(&self) -> Option<&'static str> {
        Some("background_schedule_pool")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::BackgroundSchedulePool
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_background_schedule_pool(app, context, None, None, Presentation::FullScreen);
    }
}

const COLUMNS: &[&str] = &[
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

fn build_query(context: &ContextArc, filters: &TableFilterParams, columns: &[&str]) -> String {
    let (dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.clickhouse
                .get_table_name_no_history("background_schedule_pool"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let mut where_clauses = filters.build_where_clauses();
    super::push_host_filter(
        &mut where_clauses,
        &clickhouse,
        selected_host.as_ref(),
        false,
    );

    let where_clause = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    format!(
        "SELECT {} FROM {} {} ORDER BY pool, database, table, log_name",
        columns.join(", "),
        dbtable,
        where_clause,
    )
}

fn show_background_schedule_pool_actions(
    app: &mut App,
    columns: Vec<&'static str>,
    row: QueryResultRow,
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

    fuzzy_actions(app, actions, move |app, selected| match selected.as_str() {
        "Show tasks logs" => {
            show_tasks_logs(app, columns_clone.clone(), row_clone.clone());
        }
        "Show tasks" => {
            show_tasks_summary(app, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

fn show_tasks_logs(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
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

    let context = app.user_data::<ContextArc>().unwrap().clone();
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

fn show_tasks_summary(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let log_name = map.get("log_name").map(|s| s.to_owned());
    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());

    let context = app.user_data::<ContextArc>().unwrap().clone();

    super::background_schedule_pool_log::show_background_schedule_pool_log(
        app,
        context,
        log_name,
        database,
        table,
        Presentation::Dialog,
    );
}

pub fn show_background_schedule_pool(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    presentation: Presentation,
) {
    let title = match (&database, &table) {
        (Some(db), Some(tbl)) => format!("Running tasks: {}.{}", db, tbl),
        (Some(db), None) => format!("Running tasks: {}", db),
        (None, Some(tbl)) => format!("Running tasks: table {}", tbl),
        (None, None) if presentation.is_dialog() => "Running tasks".to_string(),
        (None, None) => "Background Schedule Pool".to_string(),
    };
    let filters = TableFilterParams::new(
        database,
        table,
        "background_schedule_pool",
        "Background Tasks",
    );

    let mut columns = COLUMNS.to_vec();
    let mut columns_to_compare = vec!["pool", "database", "table", "log_name"];

    // Only show hostname column when in cluster mode AND no host filter is active
    if presentation == Presentation::FullScreen {
        let (cluster, selected_host) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.clickhouse.cluster.is_some(),
                ctx.selected_host.clone(),
            )
        };
        if cluster && selected_host.is_none() {
            columns.insert(0, "hostName() host");
            columns_to_compare.insert(0, "host");
        }
    }

    let spec = QueryTableSpec {
        view_name: filters.view_name(presentation),
        dialog_title: title.clone(),
        title,
        sort_by: "elapsed_ms",
        query: build_query(&context, &filters, &columns),
        columns,
        columns_to_compare,
        wide_columns: vec!["log_name"],
    };
    super::present_query_table(
        app,
        context,
        spec,
        show_background_schedule_pool_actions,
        presentation,
    );
}
