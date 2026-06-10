use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    theme::{BaseColor, Color},
    view::{Nameable, Resizable},
};
use std::collections::HashMap;

pub struct QueryPatternsViewProvider;

impl ViewProvider for QueryPatternsViewProvider {
    fn name(&self) -> &'static str {
        "Query patterns"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::QueryPatterns
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_query_patterns(siv, context);
    }
}

fn build_query(context: &ContextArc) -> String {
    let (view_options, limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.options.clickhouse.limit,
            ctx.clickhouse.get_log_table_name("system", "query_log"),
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

    format!(
        r#"
        WITH {start} AS start_, {end} AS end_
        SELECT
            hash,
            cnt,
            p50,
            p90,
            stddev,
            total,
            arrayStringConcat(
                arrayMap(
                    h -> ['▁','▂','▃','▄','▅','▆','▇','█'][toUInt32(least(8, greatest(1, ceil(h / arrayMax(heights_) * 8))))],
                    heights_),
                '') AS dist,
            normalized_query
        FROM
        (
            SELECT
                toString(normalized_query_hash) AS hash,
                count() AS cnt,
                quantile(0.5)(query_duration_ms)/1e3 AS p50,
                quantile(0.9)(query_duration_ms)/1e3 AS p90,
                stddevPop(query_duration_ms)/1e3 AS stddev,
                sum(query_duration_ms)/1e3 AS total,
                arrayMap(t -> t.3, histogram(16)(query_duration_ms)) AS heights_,
                any(normalizeQuery(query)) AS normalized_query
            FROM {dbtable}
            WHERE
                event_date BETWEEN toDate(start_) AND toDate(end_) AND
                event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                is_initial_query AND
                type NOT IN ('QueryStart', 'ExceptionBeforeStart')
                {internal}
                {host_filter}
            GROUP BY normalized_query_hash
            ORDER BY total DESC
            LIMIT {limit}
        )
        "#,
        start = start_sql,
        end = end_sql,
        dbtable = dbtable,
        internal = clickhouse.get_internal_filter_clause(),
        host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref()),
        limit = limit,
    )
}

fn open_last_queries_for_hash(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
) {
    let mut map = HashMap::new();
    columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
        map.insert(*c, r.to_string());
    });
    let hash = map.get("hash").cloned().unwrap_or_default();
    if hash.is_empty() {
        return;
    }

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let provider = {
        let mut ctx = context.lock().unwrap();
        *ctx.queries_filter.lock().unwrap() = hash;
        ctx.set_current_view(ChDigViews::LastQueries);
        ctx.view_registry.get_by_view_type(ChDigViews::LastQueries)
    };
    provider.show(siv, context.clone());
    context.lock().unwrap().trigger_view_refresh();
}

fn show_query_patterns(siv: &mut Cursive, context: ContextArc) {
    let view_name = "query_patterns";

    if siv.has_view(view_name) {
        return;
    }

    let query = build_query(&context);
    let columns = vec![
        "hash",
        "cnt",
        "p50",
        "p90",
        "stddev",
        "total",
        "dist",
        "normalized_query",
    ];
    let columns_to_compare = vec!["normalized_query"];

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "total",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut()
        .set_on_submit(open_last_queries_for_hash);
    view.get_inner_mut().set_title("Query patterns");
    view.get_inner_mut().set_color_log_scale(
        "p90",
        vec![
            Color::Dark(BaseColor::Green),
            Color::Dark(BaseColor::Yellow),
            Color::Dark(BaseColor::Magenta),
            Color::Dark(BaseColor::Red),
        ],
    );

    siv.drop_main_view();
    siv.set_main_view(view.with_name(view_name).full_screen());
    siv.focus_name(view_name).unwrap();
}
