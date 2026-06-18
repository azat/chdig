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

fn build_query(context: &ContextArc) -> String {
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

    // Every metric is computed in this single grouped scan; the view switches
    // between them client-side. Inner subquery emits per-metric `_total_<key>`
    // (sortable) and `_hm_<key>` (per-bucket heatmap string); the outer SELECT
    // adds the placeholder `total`/`heatmap` columns the view sources from them.
    let cols = query_patterns_metrics::metric_columns();
    let inner_totals = METRICS
        .iter()
        .zip(cols)
        .map(|(m, (_, total_col, _))| format!("{} AS {}", m.agg_expr, total_col))
        .collect::<Vec<_>>()
        .join(",\n                ");
    let inner_heatmaps = METRICS
        .iter()
        .zip(cols)
        .map(|(m, (_, _, hm_col))| {
            format!(
                r#"arrayStringConcat(
                    arrayMap(v -> toString(v),
                        {bucket_agg}Resample(0, {buckets}, 1)(
                            {bucket_value},
                            toUInt16(least({buckets} - 1,
                                intDiv(toUInt64(toUInt32(event_time) - start_ts) * {buckets}, span_)))
                        )),
                    ',') AS {hm_col}"#,
                bucket_agg = m.bucket_agg,
                bucket_value = m.bucket_value,
                buckets = HEATMAP_BUCKETS,
                hm_col = hm_col,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n                ");
    let outer_passthrough = cols
        .iter()
        .map(|(_, total_col, hm_col)| format!("{},\n            {}", total_col, hm_col))
        .collect::<Vec<_>>()
        .join(",\n            ");

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
            0 AS total,
            arrayStringConcat(
                arrayMap(
                    h -> ['▁','▂','▃','▄','▅','▆','▇','█'][toUInt32(least(8, greatest(1, ceil(h / arrayMax(heights_) * 8))))],
                    heights_),
                '') AS dist,
            '' AS heatmap,
            {outer_passthrough},
            normalized_query
        FROM
        (
            SELECT
                toString(normalized_query_hash) AS hash,
                count() AS cnt,
                quantile(0.5)(query_duration_ms)/1e3 AS p50,
                quantile(0.9)(query_duration_ms)/1e3 AS p90,
                stddevPop(query_duration_ms)/1e3 AS stddev,
                arrayMap(t -> t.3, histogram(16)(query_duration_ms)) AS heights_,
                {inner_totals},
                {inner_heatmaps},
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
            ORDER BY cnt DESC
            LIMIT {limit}
        )
        "#,
        start = start_sql,
        end = end_sql,
        dbtable = dbtable,
        internal = clickhouse.get_internal_filter_clause(),
        host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref()),
        limit = limit,
        inner_totals = inner_totals,
        inner_heatmaps = inner_heatmaps,
        outer_passthrough = outer_passthrough,
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

// Point the displayed `total`/`heatmap` columns at the metric's data and set
// its unit and titles. Shared by initial build and the runtime switch.
fn configure_metric(v: &mut view::SQLQueryView, metric: &Metric) {
    let (total_col, hm_col) = query_patterns_metrics::cols_for(metric.key);
    v.set_value_source("total", total_col);
    v.set_value_unit("total", metric.unit);
    v.set_heatmap_column("heatmap", hm_col);
    v.set_column_title("heatmap", &format!("{} heatmap", metric.label));
    v.set_column_title("total", metric.label);
}

fn apply_metric_change(siv: &mut Cursive, context: ContextArc) {
    let metric = context.lock().unwrap().query_patterns_metric;
    // All metrics are already in the result set: re-source and re-derive
    // client-side instead of re-querying.
    siv.call_on_name(
        VIEW_NAME,
        |v: &mut cursive::views::OnEventView<view::SQLQueryView>| {
            let v = v.get_inner_mut();
            configure_metric(v, metric);
            v.recompute_derived();
        },
    );
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

    let query = build_query(&context);
    // Visible columns, followed by the hidden per-metric `_total_*`/`_hm_*`
    // columns the view sources `total`/`heatmap` from on a metric switch.
    let mut columns = vec![
        "hash",
        "cnt",
        "p50",
        "p90",
        "stddev",
        "total",
        "dist",
        "heatmap",
        "normalized_query",
    ];
    for (_, total_col, hm_col) in query_patterns_metrics::metric_columns() {
        columns.push(total_col);
        columns.push(hm_col);
    }
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
        .set_column_width("heatmap", HEATMAP_BUCKETS);
    configure_metric(view.get_inner_mut(), metric);
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
