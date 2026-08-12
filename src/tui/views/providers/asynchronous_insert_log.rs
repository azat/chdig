use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, TextLogArguments, options::ChDigViews},
    tui::{
        App, Event, NamedView, Navigation, ViewProvider, actions::ActionDescription, fuzzy_actions,
        views::sql_query_view::Row as QueryResultRow, views::text_log_view::TextLogView,
    },
};
use chrono::Duration;
use std::collections::HashMap;

/// Match key for correlating an insert's query text between
/// system.asynchronous_inserts and system.asynchronous_insert_log: the stored
/// FORMAT differs between the two (the transport format vs the client format
/// vs Values, depending on the version and protocol) and the inlined SETTINGS
/// clause is not stored consistently either, so the whole SETTINGS/FORMAT tail
/// is stripped (both are keywords, they cannot appear unquoted earlier in a
/// formatted INSERT); what remains is already canonically formatted.
pub(super) fn query_match_expr(expr: &str) -> String {
    format!("replaceRegexpOne({}, ' (SETTINGS|FORMAT) .*$', '')", expr)
}

pub struct AsynchronousInsertLogViewProvider;

impl ViewProvider for AsynchronousInsertLogViewProvider {
    fn name(&self) -> &'static str {
        "Asynchronous insert log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::AsynchronousInsertLog
    }

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        show_asynchronous_insert_log(
            app,
            context,
            None,
            None,
            None,
            None,
            Presentation::FullScreen,
        );
    }
}

const COLUMNS: &[&str] = &[
    "event_time",
    "database",
    "table",
    "format",
    "bytes",
    "status",
    "flush_time",
    "exception",
    "query",
    "query_id _query_id",
    "flush_query_id _flush_query_id",
];

fn build_query(
    context: &ContextArc,
    view_name: &str,
    filters: &TableFilterParams,
    columns: &[&str],
) -> String {
    let (limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.view_limit(view_name, *ctx.queries_limit.lock().unwrap()),
            ctx.clickhouse.get_log_table_name("asynchronous_insert_log"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let (with_prelude, mut where_clauses) = super::log_time_window(context, view_name);
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
        SELECT
            {select_clause}
        FROM {dbtable}
        WHERE
            {where_clause}
        ORDER BY event_time DESC
        LIMIT {limit}
        "#,
        with_prelude = with_prelude,
        select_clause = columns.join(",\n            "),
        dbtable = dbtable,
        where_clause = where_clauses.join(" AND "),
        limit = limit,
    )
}

fn show_flush_logs(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let mut map = HashMap::new();
    columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
        map.insert(*c, r);
    });

    let mut query_ids = [map["_query_id"], map["_flush_query_id"]]
        .iter()
        .map(|f| f.to_string())
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    query_ids.dedup();

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.lock().unwrap().options.view.clone();
    // The query context is created before the entry lands in the queue, so its
    // first log lines precede event_time.
    let start = map["event_time"].as_datetime().unwrap() - Duration::try_seconds(1).unwrap();

    let log_view = NamedView::new(
        "asynchronous_insert_flush_logs",
        TextLogView::new(
            "asynchronous_insert_flush_logs",
            context,
            TextLogArguments {
                query_ids: Some(query_ids),
                query_ids_subquery: None,
                logger_names: None,
                hostname: None,
                message_filter: None,
                max_level: None,
                limit: None,
                start,
                end: view_options.end,
            },
        ),
    );
    app.present_logs("asynchronous_insert_flush_logs", "Logs:", log_view);
}

fn asynchronous_insert_log_action_callback(
    app: &mut App,
    columns: Vec<&'static str>,
    row: QueryResultRow,
) {
    let actions = vec![
        ActionDescription {
            text: "Show flush logs",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show details",
            event: Event::Unknown(vec![]),
        },
    ];

    let columns_clone = columns.clone();
    let row_clone = row.clone();

    fuzzy_actions(app, actions, move |app, selected| match selected.as_str() {
        "Show flush logs" => {
            show_flush_logs(app, columns_clone.clone(), row_clone.clone());
        }
        "Show details" => {
            super::query_result_show_row(app, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

pub fn show_asynchronous_insert_log(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    query: Option<String>,
    query_ids: Option<Vec<String>>,
    presentation: Presentation,
) {
    let filters = TableFilterParams::new(
        database,
        table,
        "asynchronous_insert_log",
        "Asynchronous Insert Log",
    )
    .with_raw(query.map(|q| {
        format!(
            "{} = {}",
            query_match_expr("query"),
            query_match_expr(&format!("'{}'", super::escape_sql_string(&q))),
        )
    }))
    .with_in("query_id", query_ids);

    let columns = if presentation.is_dialog() {
        super::dialog_columns(COLUMNS)
    } else {
        COLUMNS.to_vec()
    };

    let view_name = filters.view_name(presentation);
    let spec = QueryTableSpec {
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Asynchronous Insert Log".to_string(),
        sort_by: "event_time",
        query: build_query(&context, &view_name, &filters, &columns),
        view_name,
        columns,
        columns_to_compare: vec!["event_time", "_query_id"],
        wide_columns: vec!["query"],
    };
    super::present_query_table(
        app,
        context,
        spec,
        asynchronous_insert_log_action_callback,
        presentation,
    );
}
