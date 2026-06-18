use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{
        self, Navigation, ViewProvider,
        providers::query_patterns_metrics::{self, METRICS, Metric},
    },
};
use cursive::{
    Cursive,
    event::Event,
    theme::{BaseColor, Color},
    view::{Nameable, Resizable},
    views::OnEventView,
};
use std::collections::HashMap;

// "heatmap" column is pinned to this width via set_column_width() below, so
// the bucket count and the rendered column width line up exactly.
const HEATMAP_BUCKETS: usize = 40;

const VIEW_NAME: &str = "query_patterns";

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

fn build_query(context: &ContextArc, metric: &Metric) -> String {
    let (view_options, limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.options.clickhouse.limit,
            ctx.clickhouse.get_log_table_name("query_log"),
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
        WITH
            {start} AS start_,
            {end} AS end_,
            toUInt32(toDateTime(start_)) AS start_ts,
            greatest(toUInt32(toDateTime(end_)) - start_ts, 1) AS span_
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
            '' AS heatmap,
            _heatmap,
            normalized_query
        FROM
        (
            SELECT
                toString(normalized_query_hash) AS hash,
                count() AS cnt,
                quantile(0.5)(query_duration_ms)/1e3 AS p50,
                quantile(0.9)(query_duration_ms)/1e3 AS p90,
                stddevPop(query_duration_ms)/1e3 AS stddev,
                {total_expr} AS total,
                arrayMap(t -> t.3, histogram(16)(query_duration_ms)) AS heights_,
                arrayStringConcat(
                    arrayMap(v -> toString(v),
                        {bucket_agg}Resample(0, {buckets}, 1)(
                            {bucket_value},
                            toUInt16(least({buckets} - 1,
                                intDiv(toUInt64(toUInt32(event_time) - start_ts) * {buckets}, span_)))
                        )),
                    ',') AS _heatmap,
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
        buckets = HEATMAP_BUCKETS,
        total_expr = metric.agg_expr,
        bucket_value = metric.bucket_value,
        bucket_agg = metric.bucket_agg,
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

fn cycle_metric(siv: &mut Cursive) {
    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let next = {
        let mut ctx = context.lock().unwrap();
        let idx = METRICS
            .iter()
            .position(|m| m.key == ctx.query_patterns_metric.key)
            .unwrap_or(0);
        let next = &METRICS[(idx + 1) % METRICS.len()];
        ctx.query_patterns_metric = next;
        next
    };
    log::trace!("Query patterns metric switched to {}", next.key);
    apply_metric_change(siv, context);
}

fn apply_metric_change(siv: &mut Cursive, context: ContextArc) {
    let metric = context.lock().unwrap().query_patterns_metric;
    let query = build_query(&context, metric);
    siv.call_on_name(
        VIEW_NAME,
        |v: &mut cursive::views::OnEventView<view::SQLQueryView>| {
            let v = v.get_inner_mut();
            v.set_query(query);
            v.set_value_unit("total", metric.unit);
            v.set_column_title("heatmap", &format!("{} heatmap", metric.label));
            v.set_column_title("total", metric.label);
        },
    );
    context.lock().unwrap().trigger_view_refresh();
}

fn show_metric_picker(siv: &mut Cursive) {
    let items: Vec<(String, String)> = METRICS
        .iter()
        .map(|m| (m.label.to_string(), m.key.to_string()))
        .collect();
    crate::utils::fuzzy_select_strings(siv, "Select metric", items, |siv, key| {
        let Some(metric) = query_patterns_metrics::find(&key) else {
            return;
        };
        let context = siv.user_data::<ContextArc>().unwrap().clone();
        context.lock().unwrap().query_patterns_metric = metric;
        apply_metric_change(siv, context);
    });
}

fn show_query_patterns(siv: &mut Cursive, context: ContextArc) {
    if siv.has_view(VIEW_NAME) {
        return;
    }
    siv.drop_main_view();
    build_and_install(siv, context);
}

fn build_and_install(siv: &mut Cursive, context: ContextArc) {
    let metric = context.lock().unwrap().query_patterns_metric;

    let query = build_query(&context, metric);
    let columns = vec![
        "hash",
        "cnt",
        "p50",
        "p90",
        "stddev",
        "total",
        "dist",
        "heatmap",
        "_heatmap",
        "normalized_query",
    ];
    let columns_to_compare = vec!["normalized_query"];

    let mut view = view::SQLQueryView::new(
        context.clone(),
        VIEW_NAME,
        "total",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", VIEW_NAME));

    view.get_inner_mut()
        .set_on_submit(open_last_queries_for_hash);
    view.get_inner_mut()
        .set_heatmap_column("heatmap", "_heatmap");
    view.get_inner_mut()
        .set_column_width("heatmap", HEATMAP_BUCKETS);
    view.get_inner_mut()
        .set_column_title("heatmap", &format!("{} heatmap", metric.label));
    view.get_inner_mut().set_column_title("total", metric.label);
    view.get_inner_mut().set_value_unit("total", metric.unit);
    let total_width = METRICS.iter().map(|m| m.label.len()).max().unwrap_or(5);
    view.get_inner_mut().set_column_width("total", total_width);
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

    let wrapped = OnEventView::new(view.with_name(VIEW_NAME).full_screen())
        .on_event(Event::Char('m'), show_metric_picker)
        .on_event(Event::Char(' '), cycle_metric);

    siv.set_main_view(wrapped);
    siv.focus_name(VIEW_NAME).unwrap();
}
