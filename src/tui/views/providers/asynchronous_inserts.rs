use super::asynchronous_insert_log::show_asynchronous_insert_log;
use super::{Presentation, QueryTableSpec, TableFilterParams, escape_sql_string};
use crate::{
    interpreter::{ContextArc, TextLogArguments, options::ChDigViews},
    tui::{
        App, Dialog, Event, NamedView, Navigation, ViewProvider,
        actions::ActionDescription,
        fuzzy_actions,
        views::sql_query_view::{Field, Row as QueryResultRow},
        views::text_log_view::TextLogView,
    },
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Duration, Local};
use std::collections::HashMap;

pub struct AsynchronousInsertsViewProvider;

impl ViewProvider for AsynchronousInsertsViewProvider {
    fn name(&self) -> &'static str {
        "Asynchronous Inserts"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::AsynchronousInserts
    }

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        show_asynchronous_inserts(app, context, None, None, Presentation::FullScreen);
    }
}

const COLUMNS: &[&str] = &[
    "database",
    "table",
    "query",
    "total_bytes",
    "format",
    "first_update::DateTime first_update",
    // query_id is a client-controlled string (may contain any byte), so each
    // is base64-encoded before joining; a plain separator could not be split
    // back unambiguously. Decoded in entry_query_ids().
    "arrayStringConcat(arrayMap(id -> base64Encode(id), entries.query_id), ',') _query_ids",
];

fn row_map<'a>(
    columns: &[&'static str],
    row: &'a QueryResultRow,
) -> HashMap<&'static str, &'a Field> {
    let mut map = HashMap::new();
    columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
        map.insert(*c, r);
    });
    map
}

fn entry_query_ids(map: &HashMap<&'static str, &Field>) -> Vec<String> {
    map["_query_ids"]
        .to_string()
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|s| BASE64.decode(s).ok())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .collect()
}

fn build_query(
    context: &ContextArc,
    view_name: &str,
    filters: &TableFilterParams,
    columns: &[&str],
) -> String {
    let (limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.view_limit(view_name, ctx.options.clickhouse.limit),
            ctx.clickhouse.get_table_name("asynchronous_inserts"),
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
        r#"
        SELECT
            {select_clause}
        FROM {dbtable}
        {where_clause}
        ORDER BY first_update DESC
        LIMIT {limit}
        "#,
        select_clause = columns.join(",\n            "),
        dbtable = dbtable,
        where_clause = where_clause,
        limit = limit,
    )
}

fn present_text_log(app: &mut App, context: ContextArc, args: TextLogArguments) {
    let log_view = NamedView::new(
        "asynchronous_insert_logs",
        TextLogView::new("asynchronous_insert_logs", context, args),
    );
    app.present_logs("asynchronous_insert_logs", "Logs:", log_view);
}

/// Logs of the pending entries themselves; the flush is not there yet, so its
/// (future) query_id is picked up via the asynchronous_insert_log subquery on
/// every refresh.
fn show_logs_by_query_ids(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let map = row_map(&columns, &row);
    let query_ids = entry_query_ids(&map);

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let (view_options, dbtable) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.clickhouse.get_log_table_name("asynchronous_insert_log"),
        )
    };
    let flush_subquery = format!(
        "SELECT flush_query_id FROM {} WHERE query_id IN ('{}')",
        dbtable,
        query_ids
            .iter()
            .map(|id| escape_sql_string(id))
            .collect::<Vec<_>>()
            .join("','"),
    );
    // The query context exists before the entry lands in the queue, so its
    // first log lines precede first_update.
    let start = map["first_update"].as_datetime().unwrap() - Duration::try_seconds(1).unwrap();

    present_text_log(
        app,
        context,
        TextLogArguments {
            query_ids: Some(query_ids),
            query_ids_subquery: Some(flush_subquery),
            logger_names: None,
            hostname: None,
            message_filter: None,
            max_level: None,
            limit: None,
            start,
            end: view_options.end,
        },
    );
}

/// Logs of every insert matched by the query text in asynchronous_insert_log
/// (both the enqueue and flush sides), over the whole view interval.
fn show_logs_by_query(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let map = row_map(&columns, &row);

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let (view_options, dbtable) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.clickhouse.get_log_table_name("asynchronous_insert_log"),
        )
    };
    let subquery = format!(
        "SELECT arrayJoin([query_id, flush_query_id]) FROM {} WHERE {} = {}",
        dbtable,
        super::asynchronous_insert_log::query_match_expr("query"),
        super::asynchronous_insert_log::query_match_expr(&format!(
            "'{}'",
            escape_sql_string(&map["query"].to_string())
        )),
    );

    present_text_log(
        app,
        context,
        TextLogArguments {
            query_ids: None,
            query_ids_subquery: Some(subquery),
            logger_names: None,
            hostname: None,
            message_filter: None,
            max_level: None,
            limit: None,
            start: DateTime::<Local>::from(view_options.start),
            end: view_options.end,
        },
    );
}

/// asynchronous_insert_log rows of this pending insert's entries (they show up
/// there once flushed, the view auto-refreshes).
fn show_log_entries_by_query_ids(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let map = row_map(&columns, &row);
    let query_ids = entry_query_ids(&map);
    let context = app.user_data::<ContextArc>().unwrap().clone();
    show_asynchronous_insert_log(
        app,
        context,
        None,
        None,
        None,
        Some(query_ids),
        Presentation::Dialog,
    );
}

/// asynchronous_insert_log rows matched by the query text.
fn show_log_entries_by_query(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let map = row_map(&columns, &row);
    let query = map["query"].to_string();
    let context = app.user_data::<ContextArc>().unwrap().clone();
    show_asynchronous_insert_log(
        app,
        context,
        None,
        None,
        Some(query),
        None,
        Presentation::Dialog,
    );
}

fn show_insert_details(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let width = columns.iter().map(|c| c.len()).max().unwrap_or_default();
    let info = columns
        .iter()
        .filter(|c| !c.starts_with('_'))
        .filter_map(|c| map.get(*c).map(|v| (*c, v)))
        .map(|(c, v)| format!("{:<width$}: {}", c, v, width = width))
        .collect::<Vec<_>>()
        .join("\n");

    app.add_layer(Dialog::info(info).title("Asynchronous Insert Details"));
}

fn asynchronous_inserts_action_callback(
    app: &mut App,
    columns: Vec<&'static str>,
    row: QueryResultRow,
) {
    let actions = vec![
        ActionDescription {
            text: "Show logs (match by query_ids)",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show logs (match by query)",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show log entries (match by query_ids)",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show log entries (match by query)",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Details",
            event: Event::Unknown(vec![]),
        },
    ];

    let columns_clone = columns.clone();
    let row_clone = row.clone();

    fuzzy_actions(app, actions, move |app, selected| match selected.as_str() {
        "Show logs (match by query_ids)" => {
            show_logs_by_query_ids(app, columns_clone.clone(), row_clone.clone());
        }
        "Show logs (match by query)" => {
            show_logs_by_query(app, columns_clone.clone(), row_clone.clone());
        }
        "Show log entries (match by query_ids)" => {
            show_log_entries_by_query_ids(app, columns_clone.clone(), row_clone.clone());
        }
        "Show log entries (match by query)" => {
            show_log_entries_by_query(app, columns_clone.clone(), row_clone.clone());
        }
        "Details" => {
            show_insert_details(app, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

pub fn show_asynchronous_inserts(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    presentation: Presentation,
) {
    let filters = TableFilterParams::new(
        database,
        table,
        "asynchronous_inserts",
        "Asynchronous Inserts",
    );

    let columns = if presentation.is_dialog() {
        super::dialog_columns(COLUMNS)
    } else {
        COLUMNS.to_vec()
    };

    let view_name = filters.view_name(presentation);
    let spec = QueryTableSpec {
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Asynchronous Inserts".to_string(),
        sort_by: "first_update",
        query: build_query(&context, &view_name, &filters, &columns),
        view_name,
        columns,
        columns_to_compare: vec!["first_update"],
        wide_columns: vec!["query"],
    };
    super::present_query_table(
        app,
        context,
        spec,
        asynchronous_inserts_action_callback,
        presentation,
    );
}
