use std::sync::{Arc, Mutex};

use crate::interpreter::{
    ContextArc, PROFILE_EVENTS_PREFIX,
    options::{ChDigViewSettings, ChDigViews, FlamelensPane, LogLevel},
    queries_filter::token_under_cursor,
};
use crate::tui::app::App;
use crate::tui::checkbox::Checkbox;
use crate::tui::component::{Component, DummyView, Nameable, OnEventView};
use crate::tui::dialog::Dialog;
use crate::tui::edit::EditView;
use crate::tui::event::Key;
use crate::tui::linear::LinearLayout;
use crate::tui::prompt::show_bottom_prompt_with_suggestions;
use crate::tui::resize::Resizable;
use crate::tui::scroll::ScrollView;
use crate::tui::style::{Modifier, Style, StyledString};
use crate::tui::tabs::Tabs;
use crate::tui::text::TextView;
use crate::tui::views::providers::part_log;
use crate::tui::views::queries_view::{
    QueriesView, ordered_query_columns, query_column_by_id, query_column_id,
};
use crate::tui::views::sql_query_view::SQLQueryView;
use crate::tui::views::summary_view::SummaryView;
use crate::tui::{Mux, Navigation, show_bottom_prompt, submit_on_enter};

/// The focused pane's view (reopened after applying the settings).
struct FocusedView {
    view_type: ChDigViews,
    instance: Option<String>,
}

fn focused_view(app: &mut App, context: &ContextArc) -> Option<FocusedView> {
    // The pane's slot name identifies the view: a widget name (builtin views,
    // including same-named `views:` settings entries) or a named instance.
    let name = app
        .call_on_name("panes", |mux: &mut Mux| mux.focused_view_name())
        .flatten()?;
    let ctx = context.lock().unwrap();
    if let Some(view_type) = ctx.view_registry.view_type_by_view_name(&name) {
        return Some(FocusedView {
            view_type,
            instance: None,
        });
    }
    let view_type = ctx.options.views.get(&name)?.view_type;
    Some(FocusedView {
        view_type,
        instance: Some(name),
    })
}

/// Per-view settings shown in the dialog for `view_type` (Client has none).
struct ViewFields {
    filter: bool,
    query_kind: bool,
    interval: bool,
    limit: bool,
    level: bool,
    /// A free-form list (the queries views get a checkbox list instead)
    columns: bool,
}

impl ViewFields {
    fn of(view_type: ChDigViews) -> Self {
        let client = view_type == ChDigViews::Client;
        let flamegraph = view_type.is_flamegraph();
        Self {
            filter: !client && !flamegraph,
            query_kind: view_type.is_queries(),
            interval: !client,
            limit: !client && !flamegraph,
            level: view_type == ChDigViews::ServerLogs,
            columns: view_type == ChDigViews::PartLog,
        }
    }

    fn any(&self) -> bool {
        self.filter || self.query_kind || self.interval || self.limit || self.level || self.columns
    }
}

/// Space/comma separated list.
fn parse_list(text: &str) -> Vec<String> {
    text.split([',', ' '])
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

/// Empty means unset (the global value applies).
fn parse_optional<T: std::str::FromStr>(value: &str, what: &str) -> Result<Option<T>, String>
where
    T::Err: std::fmt::Display,
{
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<T>()
        .map(Some)
        .map_err(|err| format!("Invalid {}: {}", what, err))
}

fn edit_content(app: &mut App, name: &str) -> String {
    app.call_on_name(name, |v: &mut EditView| v.get_content())
        .unwrap_or_default()
}

/// A view with a tab under "Views": a builtin or a named instance.
struct ViewEntry {
    /// `views:` config key
    key: String,
    view_type: ChDigViews,
    /// Widget name (what the per-view lookups in the context are keyed by)
    view_name: String,
    instance: bool,
}

/// The builtin views with settings (CLI order), then the configured named
/// instances (sorted).
fn configurable_views(ctx: &crate::interpreter::Context) -> Vec<ViewEntry> {
    let mut views: Vec<ViewEntry> = ChDigViews::all()
        .filter(|view_type| ViewFields::of(*view_type).any())
        .filter_map(|view_type| {
            let provider = ctx.view_registry.find_by_view_type(view_type)?;
            Some(ViewEntry {
                key: view_type.config_name().to_string(),
                view_type,
                view_name: provider
                    .view_name()
                    .unwrap_or(view_type.config_name())
                    .to_string(),
                instance: false,
            })
        })
        .collect();
    let mut instances: Vec<ViewEntry> = ctx
        .options
        .views
        .iter()
        .filter(|(key, instance)| *key != instance.view_type.config_name())
        .map(|(key, instance)| ViewEntry {
            key: key.clone(),
            view_type: instance.view_type,
            view_name: key.clone(),
            instance: true,
        })
        .collect();
    instances.sort_by(|a, b| a.key.cmp(&b.key));
    views.extend(instances);
    views
}

/// Widget name prefix of a view's settings editor
fn view_prefix(key: &str) -> String {
    format!("set_v_{}_", key)
}

/// Reads a per-view settings editor (widgets named `{prefix}{field}`) over
/// `settings` (only the fields it shows).
fn read_view_settings(
    app: &mut App,
    prefix: &str,
    fields: &ViewFields,
    mut settings: ChDigViewSettings,
) -> Result<ChDigViewSettings, String> {
    let content = |app: &mut App, field: &str| edit_content(app, &format!("{}{}", prefix, field));
    if fields.filter {
        let filter = content(app, "filter");
        settings.filter = (!filter.trim().is_empty()).then(|| filter.trim().to_string());
    }
    if fields.query_kind {
        settings.query_kind = parse_list(&content(app, "query_kind"));
    }
    if fields.columns {
        let columns = parse_list(&content(app, "columns"));
        if let Some(unknown) = columns.iter().find(|column| !part_log::is_column(column)) {
            return Err(format!(
                "Unknown part_log column: {} (expected one of {} or ProfileEvents.<Name>)",
                unknown,
                part_log::column_labels().collect::<Vec<_>>().join(", ")
            ));
        }
        // The default list spelled out (see the layout) is no override
        settings.columns = if columns.iter().eq(part_log::column_labels()) {
            Vec::new()
        } else {
            columns
        };
    }
    if fields.interval {
        settings.start = parse_optional::<crate::common::RelativeDateTime>(
            &content(app, "start"),
            "view start",
        )?;
        settings.end =
            parse_optional::<crate::common::RelativeDateTime>(&content(app, "end"), "view end")?;
    }
    if fields.limit {
        settings.limit = parse_optional::<u64>(&content(app, "limit"), "view limit")?;
    }
    if fields.level {
        settings.level = parse_optional::<LogLevel>(&content(app, "level"), "level")?;
    }
    Ok(settings)
}

/// Reads a queries view's columns editor: the checkboxes (laid out in the
/// `current` order, see the layout) plus the "add" field.
fn read_queries_columns(
    app: &mut App,
    prefix: &str,
    current: &[String],
) -> Result<Vec<String>, String> {
    let mut columns: Vec<String> = Vec::new();
    for col in ordered_query_columns(current) {
        let Some(label) = query_column_id(col) else {
            continue;
        };
        let checked = app
            .call_on_name(&format!("{}qcol_{}", prefix, label), |v: &mut Checkbox| {
                v.is_checked()
            })
            .unwrap_or(true);
        if checked {
            columns.push(label);
        }
    }
    for label in edit_content(app, &format!("{}qcol_extra", prefix)).split_whitespace() {
        if query_column_by_id(label).is_none() {
            return Err(format!("Unknown column: {}", label));
        }
        if !columns.iter().any(|c| c == label) {
            columns.push(label.to_string());
        }
    }
    Ok(columns)
}

/// Where a columns field takes its completion candidates from
#[derive(Clone)]
enum ColumnSource {
    /// The queries view with this widget name (or any builtin one with rows)
    Queries(String),
    /// The part log view's `_profile_events`
    PartLog,
}

/// Column label candidates from the loaded rows.
fn extra_column_candidates(
    app: &mut App,
    context: &ContextArc,
    source: ColumnSource,
) -> Vec<String> {
    match source {
        ColumnSource::PartLog => app
            .call_on_name(
                ChDigViews::PartLog.config_name(),
                |v: &mut OnEventView<SQLQueryView>| v.get_inner_mut().map_keys("_profile_events"),
            )
            .unwrap_or_default()
            .into_iter()
            .map(|name| format!("{}{}", PROFILE_EVENTS_PREFIX, name))
            .collect(),
        ColumnSource::Queries(view_name) => queries_column_candidates(app, view_name, context),
    }
}

/// Extra-column candidates from the loaded rows of the queries view
/// `view_name`, falling back to the builtin queries views (other panes may
/// have rows).
fn queries_column_candidates(
    app: &mut App,
    view_name: String,
    context: &ContextArc,
) -> Vec<String> {
    let mut view_names = vec![view_name];
    {
        let ctx = context.lock().unwrap();
        view_names.extend(
            [
                ChDigViews::Queries,
                ChDigViews::LastQueries,
                ChDigViews::SlowQueries,
            ]
            .into_iter()
            .filter_map(|view_type| ctx.view_registry.get_by_view_type(view_type).view_name())
            .map(str::to_string),
        );
    }
    for view_name in view_names {
        let candidates = app.call_on_name(&view_name, |v: &mut OnEventView<QueriesView>| {
            v.get_inner_mut().column_candidates()
        });
        if let Some(candidates) = candidates
            && !candidates.is_empty()
        {
            return candidates;
        }
    }
    Vec::new()
}

/// Tab in a columns field of the dialog: edits its content in the bottom
/// prompt, completing the labels from the loaded rows of the focused view
/// (`pe.`/`s.` stand for `ProfileEvents.`/`Settings.` while typing).
fn complete_column_field(app: &mut App, context: &ContextArc, field: &str, source: ColumnSource) {
    const MAX_SUGGESTIONS: usize = 30;
    let candidates = Arc::new(extra_column_candidates(app, context, source));
    let field = field.to_string();
    let suggest = Arc::new(
        move |_app: &mut App, text: &str, cursor: usize| -> Vec<String> {
            let token = token_under_cursor(text, cursor).to_ascii_lowercase();
            let token = match token.strip_prefix("pe.") {
                Some(name) => format!("profileevents.{}", name),
                None => match token.strip_prefix("s.") {
                    Some(name) => format!("settings.{}", name),
                    None => token,
                },
            };
            candidates
                .iter()
                .filter(|candidate| candidate.to_ascii_lowercase().contains(&token))
                .take(MAX_SUGGESTIONS)
                .cloned()
                .collect()
        },
    );
    let initial = edit_content(app, &field);
    let on_submit = move |app: &mut App, text: &str| {
        app.pop_layer();
        let callback = app.call_on_name(&field, |v: &mut EditView| v.set_content(text.trim()));
        if let Some(callback) = callback {
            callback(app);
        }
        app.focus_name(&field);
    };
    show_bottom_prompt_with_suggestions(app, "columns: ", initial, |_, _| {}, suggest, on_submit);
}

fn apply_settings(app: &mut App, context: &ContextArc) {
    let history = app
        .call_on_name("set_history", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let internal_queries = app
        .call_on_name("set_internal_queries", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let limit_str = app
        .call_on_name("set_limit", |v: &mut EditView| {
            v.get_content().trim().to_string()
        })
        .unwrap();
    let logs_order_desc = app
        .call_on_name("set_logs_order_desc", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let skip_unavailable_shards = app
        .call_on_name("set_skip_unavailable_shards", |v: &mut Checkbox| {
            v.is_checked()
        })
        .unwrap();

    let delay_str = app
        .call_on_name("set_delay_interval", |v: &mut EditView| {
            v.get_content().trim().to_string()
        })
        .unwrap();
    let group_by = app
        .call_on_name("set_group_by", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let no_subqueries = app
        .call_on_name("set_no_subqueries", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let summary_per_host = app
        .call_on_name("set_summary_per_host", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let wrap = app
        .call_on_name("set_wrap", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let align_log_columns = app
        .call_on_name("set_align_log_columns", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let logs_in_dialog = app
        .call_on_name("set_logs_in_dialog", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let flamelens_pane_str = app
        .call_on_name("set_flamelens_pane", |v: &mut EditView| {
            v.get_content().trim().to_string()
        })
        .unwrap();
    let no_strip = app
        .call_on_name("set_no_strip_hostname_suffix", |v: &mut Checkbox| {
            v.is_checked()
        })
        .unwrap();
    let no_color = app
        .call_on_name("set_no_color", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let queries_limit_str = app
        .call_on_name("set_queries_limit", |v: &mut EditView| {
            v.get_content().trim().to_string()
        })
        .unwrap();
    let start_str = app
        .call_on_name("set_start", |v: &mut EditView| {
            v.get_content().trim().to_string()
        })
        .unwrap();
    let end_str = app
        .call_on_name("set_end", |v: &mut EditView| {
            v.get_content().trim().to_string()
        })
        .unwrap();

    let otel = app
        .call_on_name("set_otel", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let trace_log = app
        .call_on_name("set_trace_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let query_metric = app
        .call_on_name("set_query_metric_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let part_log = app
        .call_on_name("set_part_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let query_thread = app
        .call_on_name("set_query_thread_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let text_log = app
        .call_on_name("set_text_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let text_log_android = app
        .call_on_name("set_text_log_android", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let per_server = app
        .call_on_name("set_per_server", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let stack_traces_by_thread = app
        .call_on_name("set_stack_traces_by_thread", |v: &mut Checkbox| {
            v.is_checked()
        })
        .unwrap();

    let metric_log = app
        .call_on_name("set_metric_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let async_metric_log = app
        .call_on_name("set_async_metric_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let async_insert_log = app
        .call_on_name("set_async_insert_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let error_log = app
        .call_on_name("set_error_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let s3_queue_log = app
        .call_on_name("set_s3_queue_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let azure_queue_log = app
        .call_on_name("set_azure_queue_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let blob_storage_log = app
        .call_on_name("set_blob_storage_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let bg_pool_log = app
        .call_on_name("set_bg_pool_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let session_log = app
        .call_on_name("set_session_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let zk_log = app
        .call_on_name("set_zk_log", |v: &mut Checkbox| v.is_checked())
        .unwrap();
    let compress = app
        .call_on_name("set_compress", |v: &mut Checkbox| v.is_checked())
        .unwrap();

    let pastila_clickhouse_host = app
        .call_on_name("set_pastila_clickhouse_host", |v: &mut EditView| {
            v.get_content()
        })
        .unwrap();
    let pastila_url = app
        .call_on_name("set_pastila_url", |v: &mut EditView| v.get_content())
        .unwrap();
    let pastila_compression = app
        .call_on_name("set_pastila_compression", |v: &mut Checkbox| v.is_checked())
        .unwrap();

    let limit: u64 = match limit_str.parse() {
        Ok(v) => v,
        Err(_) => {
            app.add_layer(Dialog::info("Invalid limit value"));
            return;
        }
    };
    let delay_ms: u64 = match delay_str.parse() {
        Ok(v) => v,
        Err(_) => {
            app.add_layer(Dialog::info("Invalid delay_interval value"));
            return;
        }
    };
    let queries_limit: u64 = match queries_limit_str.parse() {
        Ok(v) => v,
        Err(_) => {
            app.add_layer(Dialog::info("Invalid queries_limit value"));
            return;
        }
    };
    let new_start = match start_str.parse::<crate::common::RelativeDateTime>() {
        Ok(v) => v,
        Err(err) => {
            app.add_layer(Dialog::info(format!("Invalid start: {}", err)));
            return;
        }
    };
    let new_end = match end_str.parse::<crate::common::RelativeDateTime>() {
        Ok(v) => v,
        Err(err) => {
            app.add_layer(Dialog::info(format!("Invalid end: {}", err)));
            return;
        }
    };
    let flamelens_pane = match flamelens_pane_str.parse::<FlamelensPane>() {
        Ok(v) => v,
        Err(err) => {
            app.add_layer(Dialog::info(format!("Invalid flamelens_pane: {}", err)));
            return;
        }
    };
    let focused = focused_view(app, context);
    let existing_settings = |key: &str| {
        context
            .lock()
            .unwrap()
            .options
            .views
            .get(key)
            .map(|instance| instance.settings.clone())
            .unwrap_or_default()
    };
    // The Views tab: one editor per view
    let views = configurable_views(&context.lock().unwrap());
    let mut view_settings = Vec::with_capacity(views.len());
    for view in views {
        let existing = existing_settings(&view.key);
        let fields = ViewFields::of(view.view_type);
        let prefix = view_prefix(&view.key);
        let mut settings = match read_view_settings(app, &prefix, &fields, existing) {
            Ok(settings) => settings,
            Err(err) => {
                app.add_layer(Dialog::info(format!("{}: {}", view.key, err)));
                return;
            }
        };
        if view.view_type.is_queries() {
            let (current, global) = {
                let ctx = context.lock().unwrap();
                (
                    ctx.queries_columns(&view.view_name),
                    ctx.options.view.query_columns.clone(),
                )
            };
            let columns = match read_queries_columns(app, &prefix, &current) {
                Ok(columns) => columns,
                Err(err) => {
                    app.add_layer(Dialog::info(format!("{}: {}", view.key, err)));
                    return;
                }
            };
            // The global list applies unless the view's differs from it
            settings.columns = if columns == global {
                Vec::new()
            } else {
                columns
            };
        }
        view_settings.push((view, settings));
    }

    let mut changed_filters: Vec<(String, String)> = Vec::new();
    {
        let mut ctx = context.lock().unwrap();
        ctx.options.clickhouse.history = history;
        ctx.options.clickhouse.internal_queries = internal_queries;
        ctx.options.clickhouse.limit = limit;
        ctx.options.clickhouse.logs_order = if logs_order_desc {
            crate::interpreter::options::LogsOrder::Desc
        } else {
            crate::interpreter::options::LogsOrder::Asc
        };
        ctx.options.clickhouse.skip_unavailable_shards = skip_unavailable_shards;
        // The client keeps its own copy of the options (read per query)
        ctx.clickhouse.set_options(ctx.options.clickhouse.clone());

        ctx.options.view.delay_interval = std::time::Duration::from_millis(delay_ms);
        ctx.options.view.group_by = group_by;
        ctx.options.view.no_subqueries = no_subqueries;
        ctx.options.view.wrap = wrap;
        ctx.options.view.align_log_columns = align_log_columns;
        ctx.options.view.logs_in_dialog = logs_in_dialog;
        ctx.options.view.flamelens_pane = flamelens_pane;
        ctx.options.view.no_strip_hostname_suffix = no_strip;
        ctx.options.view.no_color = no_color;
        ctx.options.view.summary_per_host = summary_per_host;
        ctx.options.view.queries_limit = queries_limit;
        *ctx.queries_limit.lock().unwrap() = queries_limit;
        ctx.options.view.start = new_start;
        ctx.options.view.end = new_end;
        for (view, settings) in view_settings {
            // A queries view keeps its filter outside the settings (seeded
            // from them on first use): the live one is set below, through
            // the view when it exists (its parsed copy and refresh)
            if view.view_type.is_queries() {
                let filter = settings.filter.clone().unwrap_or_default();
                if *ctx.queries_filter(&view.view_name).lock().unwrap() != filter {
                    changed_filters.push((view.view_name.clone(), filter));
                }
            }
            // Untouched views get no `views:` entry
            if settings.is_empty() && !ctx.options.views.contains_key(&view.key) {
                continue;
            }
            *ctx.view_settings_mut(&view.key, view.view_type) = settings;
        }

        ctx.options.perfetto.opentelemetry_span_log = otel;
        ctx.options.perfetto.trace_log = trace_log;
        ctx.options.perfetto.query_metric_log = query_metric;
        ctx.options.perfetto.part_log = part_log;
        ctx.options.perfetto.query_thread_log = query_thread;
        ctx.options.perfetto.text_log = text_log;
        ctx.options.perfetto.text_log_android = text_log_android;
        ctx.options.perfetto.per_server = per_server;
        ctx.options.perfetto.stack_traces_by_thread = stack_traces_by_thread;
        ctx.options.perfetto.metric_log = metric_log;
        ctx.options.perfetto.asynchronous_metric_log = async_metric_log;
        ctx.options.perfetto.asynchronous_insert_log = async_insert_log;
        ctx.options.perfetto.error_log = error_log;
        ctx.options.perfetto.s3_queue_log = s3_queue_log;
        ctx.options.perfetto.azure_queue_log = azure_queue_log;
        ctx.options.perfetto.blob_storage_log = blob_storage_log;
        ctx.options.perfetto.background_schedule_pool_log = bg_pool_log;
        ctx.options.perfetto.session_log = session_log;
        ctx.options.perfetto.aggregated_zookeeper_log = zk_log;
        ctx.options.perfetto.compress = compress;

        ctx.options.service.pastila_clickhouse_host = pastila_clickhouse_host;
        ctx.options.service.pastila_url = pastila_url;
        ctx.options.service.pastila_compression = pastila_compression;

        ctx.trigger_view_refresh();
    }
    app.call_on_name("summary", |view: &mut SummaryView| {
        view.set_per_host(summary_per_host)
    });
    for (view_name, filter) in changed_filters {
        let shown = app.call_on_name(&view_name, |v: &mut OnEventView<QueriesView>| {
            v.get_inner_mut().set_filter(&filter)
        });
        if shown.is_none() {
            *context
                .lock()
                .unwrap()
                .queries_filter(&view_name)
                .lock()
                .unwrap() = filter;
        }
    }

    // Re-create the focused pane's view so option changes that only take
    // effect at view construction time (query_columns, the per-view
    // settings) are picked up immediately.
    let Some(focused) = focused else {
        // Nothing rebuildable in the focused pane (an ad-hoc flamegraph pane
        // and the like): just close the dialog.
        app.pop_layer();
        return;
    };
    let provider = context
        .lock()
        .unwrap()
        .view_registry
        .get_by_view_type(focused.view_type);
    log::info!(
        "Reopen {} view after settings change",
        focused
            .instance
            .as_deref()
            .unwrap_or_else(|| provider.name())
    );
    app.drop_main_view();
    provider.show(app, context.clone(), focused.instance.as_deref());
    context.lock().unwrap().trigger_view_refresh();
}

/// Width cap for read-only values, in line with the widest editable rows
const TEXT_VALUE_MAX_WIDTH: u16 = 50;

struct SearchTarget {
    label: String,
    section: usize,
    focus_name: String,
}

/// One tab of the dialog: side-by-side columns of sections (or a group
/// header in the tab list).
struct TabSpec {
    title: String,
    columns: Vec<LinearLayout>,
    /// A group header in the tab list (no content)
    group: bool,
}

impl TabSpec {
    fn new(title: &str) -> Self {
        Self {
            title: title.to_string(),
            columns: vec![LinearLayout::vertical()],
            group: false,
        }
    }

    /// The columns side by side; they can exceed narrow terminals, so the
    /// content scrolls horizontally too
    fn content(columns: Vec<LinearLayout>) -> impl Component {
        let mut layout = LinearLayout::horizontal();
        for (i, column) in columns.into_iter().enumerate() {
            if i > 0 {
                layout.add_child(DummyView.fixed_width(3));
            }
            layout.add_child(column);
        }
        ScrollView::new(layout).scroll_x(true)
    }
}

/// Settings laid out as tabs of side-by-side columns
/// of sections. Every option registers a search target, so that `/` can
/// focus it (switching the tabs as needed).
struct SearchableLayout {
    tabs: Vec<TabSpec>,
    sections: Vec<String>,
    targets: Vec<SearchTarget>,
}

impl SearchableLayout {
    fn new() -> Self {
        Self {
            tabs: Vec::new(),
            sections: Vec::new(),
            targets: Vec::new(),
        }
    }

    /// Subsequent sections go into a new tab
    fn tab(&mut self, title: &str) {
        self.tabs.push(TabSpec::new(title));
    }

    /// A header in the tab list; the tabs under it are indented
    fn group(&mut self, title: &str) {
        let mut tab = TabSpec::new(title);
        tab.group = true;
        self.tabs.push(tab);
    }

    fn current(&mut self) -> &mut TabSpec {
        let tab = self.tabs.last_mut().expect("tab() first");
        assert!(!tab.group, "tab() first");
        tab
    }

    /// Subsequent sections go into a new column (columns are laid out side by side)
    fn column(&mut self) {
        self.current().columns.push(LinearLayout::vertical());
    }

    fn layout(&mut self) -> &mut LinearLayout {
        self.current().columns.last_mut().unwrap()
    }

    fn into_tabs(self) -> (Tabs, Vec<String>, Vec<SearchTarget>) {
        let mut tabs = Tabs::new();
        for tab in self.tabs {
            if tab.group {
                tabs.add_group(tab.title);
            } else {
                tabs.add_tab(tab.title, TabSpec::content(tab.columns));
            }
        }
        (tabs, self.sections, self.targets)
    }

    fn target(&mut self, label: &str, focus_name: &str) {
        self.targets.push(SearchTarget {
            label: label.to_string(),
            section: self.sections.len().saturating_sub(1),
            focus_name: focus_name.to_string(),
        });
    }

    fn section(&mut self, title: &str) {
        // The tab title (and its group's) is searchable through its sections
        let group = self
            .tabs
            .iter()
            .rev()
            .find(|tab| tab.group)
            .map(|tab| tab.title.as_str())
            .unwrap_or("");
        let tab = self.tabs.last().map(|tab| tab.title.trim()).unwrap_or("");
        self.sections
            .push(format!("{} {} {}", group, tab, title.trim_end_matches(':')));
        let title = TextView::new(StyledString::styled(
            title,
            Style::default().add_modifier(Modifier::BOLD),
        ));
        self.layout().add_child(title);
    }

    fn separator(&mut self) {
        self.layout().add_child(DummyView);
    }

    fn note(&mut self, text: &str) {
        self.layout()
            .add_child(TextView::new(format!("  {}", text)));
    }

    fn text(&mut self, label: &str, value: impl std::fmt::Display) {
        let name = format!("settings_row_{}", self.targets.len());
        // A read-only input scrolls long values (e.g. the connection URL)
        // within the cap instead of widening the column and pushing the other
        // columns off-screen (the scroll view offers unbounded width).
        let row = LinearLayout::horizontal()
            .child(TextView::new(format!("  {}: ", label)))
            .child(
                EditView::new()
                    .content(value.to_string())
                    .readonly()
                    .with_name(name.clone())
                    .max_width(TEXT_VALUE_MAX_WIDTH),
            );
        self.layout().add_child(row);
        self.target(label, &name);
    }

    fn checkbox(&mut self, label: &str, name: &str, checked: bool) {
        let row = LinearLayout::horizontal()
            .child(DummyView.fixed_width(2))
            .child(Checkbox::new().checked(checked).with_name(name))
            .child(TextView::new(format!(" {}", label)));
        self.layout().add_child(row);
        self.target(label, name);
    }

    fn edit(&mut self, label: &str, name: &str, value: &str, width: u16) {
        let row = LinearLayout::horizontal()
            .child(TextView::new(format!("  {}: ", label)))
            .child(
                EditView::new()
                    .content(value)
                    .with_name(name)
                    .fixed_width(width),
            );
        self.layout().add_child(row);
        self.target(label, name);
    }

    /// An edit field of column labels: Tab opens the completing prompt.
    fn edit_columns(
        &mut self,
        label: &str,
        name: &str,
        value: &str,
        width: u16,
        context: ContextArc,
        source: ColumnSource,
    ) {
        let field = name.to_string();
        let edit = OnEventView::new(EditView::new().content(value).with_name(name))
            .on_pre_event(Key::Tab, move |app| {
                complete_column_field(app, &context, &field, source.clone())
            });
        let row = LinearLayout::horizontal()
            .child(TextView::new(format!("  {} (Tab completes): ", label)))
            .child(edit.fixed_width(width));
        self.layout().add_child(row);
        self.target(label, name);
    }

    /// The editor of one view's `views:` settings, widgets named
    /// `{prefix}{field}`. `filter` is the value shown for the filter (a
    /// queries view keeps the live one outside the settings).
    fn view_settings(
        &mut self,
        prefix: &str,
        fields: &ViewFields,
        settings: &ChDigViewSettings,
        filter: &str,
        context: &ContextArc,
    ) {
        let name = |field: &str| format!("{}{}", prefix, field);
        if !fields.any() {
            self.note("(no per-view settings)");
        }
        if fields.filter {
            self.edit("filter", &name("filter"), filter, 30);
        }
        if fields.query_kind {
            self.edit(
                "query_kind (comma separated, empty = all)",
                &name("query_kind"),
                &settings.query_kind.join(", "),
                30,
            );
        }
        if fields.interval {
            self.edit(
                "start (empty = global)",
                &name("start"),
                &settings
                    .start
                    .as_ref()
                    .map(|start| start.to_editable_string())
                    .unwrap_or_default(),
                22,
            );
            self.edit(
                "end (empty = global)",
                &name("end"),
                &settings
                    .end
                    .as_ref()
                    .map(|end| end.to_editable_string())
                    .unwrap_or_default(),
                22,
            );
        }
        if fields.limit {
            self.edit(
                "limit (empty = global)",
                &name("limit"),
                &settings
                    .limit
                    .map(|limit| limit.to_string())
                    .unwrap_or_default(),
                12,
            );
        }
        if fields.level {
            self.edit(
                "level (fatal..test, empty = all)",
                &name("level"),
                &settings
                    .level
                    .map(|level| level.as_str().to_lowercase())
                    .unwrap_or_default(),
                12,
            );
        }
        if fields.columns {
            // The default list is spelled out, so that adding appends to it
            let columns = if settings.columns.is_empty() {
                part_log::column_labels().collect::<Vec<_>>().join(" ")
            } else {
                settings.columns.join(" ")
            };
            self.edit_columns(
                "columns (in display order)",
                &name("columns"),
                &columns,
                60,
                context.clone(),
                ColumnSource::PartLog,
            );
        }
    }
}

#[derive(Default)]
struct SearchState {
    query: String,
    position: usize,
}

struct SettingsSearch {
    sections: Vec<String>,
    targets: Vec<SearchTarget>,
    state: Mutex<SearchState>,
}

fn apply_settings_search(app: &mut App, search: &SettingsSearch, query: &str) {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return;
    }

    // Exact label matches go first, then substring matches; a section title
    // match (e.g. "perfetto") cycles through all its options
    let labels: Vec<String> = search
        .targets
        .iter()
        .map(|target| target.label.to_lowercase())
        .collect();
    let mut matches: Vec<usize> = (0..labels.len()).filter(|&i| labels[i] == query).collect();
    matches.extend((0..labels.len()).filter(|&i| {
        labels[i] != query
            && (labels[i].contains(&query)
                || search.sections[search.targets[i].section]
                    .to_lowercase()
                    .contains(&query))
    }));
    if matches.is_empty() {
        return;
    }

    let next = {
        let mut state = search.state.lock().unwrap();
        // Repeating the same query advances to the next match (wrapping around)
        let position = if state.query == query {
            (state.position + 1) % matches.len()
        } else {
            0
        };
        state.query = query;
        state.position = position;
        matches[position]
    };

    app.focus_name(&search.targets[next].focus_name);
}

pub fn show_settings_dialog(app: &mut App) {
    if app.call_on_name("settings", |_: &mut Dialog| ()).is_some() {
        app.pop_layer();
        return;
    }

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let (opts, server_version, selected_host, current_view, views) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.clone(),
            ctx.server_version.clone(),
            ctx.selected_host.clone(),
            ctx.current_view,
            configurable_views(&ctx),
        )
    };

    let mut layout = SearchableLayout::new();

    layout.tab("ClickHouse");
    layout.section("ClickHouse:");
    layout.text("url", &opts.clickhouse.url_safe);
    if let Some(ref cluster) = opts.clickhouse.cluster {
        layout.text("cluster", cluster);
    }
    layout.checkbox("history", "set_history", opts.clickhouse.history);
    layout.checkbox(
        "internal_queries",
        "set_internal_queries",
        opts.clickhouse.internal_queries,
    );
    layout.edit("limit", "set_limit", &opts.clickhouse.limit.to_string(), 12);
    layout.checkbox(
        "logs_order=desc (newest first)",
        "set_logs_order_desc",
        opts.clickhouse.logs_order == crate::interpreter::options::LogsOrder::Desc,
    );
    layout.checkbox(
        "skip_unavailable_shards",
        "set_skip_unavailable_shards",
        opts.clickhouse.skip_unavailable_shards,
    );
    layout.text("server_version", &server_version);

    layout.tab("General");
    layout.section("View:");
    layout.edit(
        "delay_interval (ms)",
        "set_delay_interval",
        &opts.view.delay_interval.as_millis().to_string(),
        12,
    );
    layout.checkbox("wrap", "set_wrap", opts.view.wrap);
    layout.checkbox(
        "align_log_columns",
        "set_align_log_columns",
        opts.view.align_log_columns,
    );
    layout.checkbox(
        "logs_in_dialog",
        "set_logs_in_dialog",
        opts.view.logs_in_dialog,
    );
    layout.edit(
        "flamelens_pane (off/below/above)",
        "set_flamelens_pane",
        &opts.view.flamelens_pane.to_string(),
        12,
    );
    layout.checkbox(
        "no_strip_hostname_suffix",
        "set_no_strip_hostname_suffix",
        opts.view.no_strip_hostname_suffix,
    );
    layout.checkbox("no_color", "set_no_color", opts.view.no_color);
    layout.checkbox(
        "summary_per_host (cluster)",
        "set_summary_per_host",
        opts.view.summary_per_host,
    );
    layout.edit(
        "start (datetime/offset, empty=now)",
        "set_start",
        &opts.view.start.to_editable_string(),
        22,
    );
    layout.edit(
        "end (datetime/offset, empty=now)",
        "set_end",
        &opts.view.end.to_editable_string(),
        22,
    );
    layout.separator();

    layout.section("Queries:");
    layout.checkbox("group_by", "set_group_by", opts.view.group_by);
    layout.checkbox(
        "no_subqueries",
        "set_no_subqueries",
        opts.view.no_subqueries,
    );
    layout.edit(
        "queries_limit",
        "set_queries_limit",
        &opts.view.queries_limit.to_string(),
        12,
    );
    layout.separator();

    layout.section("Service:");
    layout.text("log", opts.service.log.as_deref().unwrap_or("(none)"));
    layout.text(
        "chdig_config",
        opts.service.chdig_config.as_deref().unwrap_or("(none)"),
    );
    layout.edit(
        "pastila_clickhouse_host",
        "set_pastila_clickhouse_host",
        &opts.service.pastila_clickhouse_host,
        35,
    );
    layout.edit(
        "pastila_url",
        "set_pastila_url",
        &opts.service.pastila_url,
        35,
    );
    layout.checkbox(
        "pastila_compression",
        "set_pastila_compression",
        opts.service.pastila_compression,
    );
    layout.separator();

    layout.section("Runtime:");
    layout.text("selected_host", selected_host.as_deref().unwrap_or("(all)"));
    layout.text(
        "current_view",
        format!("{:?}", current_view.unwrap_or(ChDigViews::Queries)),
    );

    // One tab per view (builtins, then the named instances)
    layout.group("Views");
    for view in &views {
        layout.tab(&format!("  {}", view.key));
        layout.section(&format!("{}:", view.key));
        if view.instance {
            layout.text("view", view.view_type.config_name());
        }
        let settings = opts
            .views
            .get(&view.key)
            .map(|instance| instance.settings.clone())
            .unwrap_or_default();
        // A queries view keeps its live filter outside the settings
        let filter = if view.view_type.is_queries() {
            let mut ctx = context.lock().unwrap();
            ctx.queries_filter(&view.view_name).lock().unwrap().clone()
        } else {
            settings.filter.clone().unwrap_or_default()
        };
        let prefix = view_prefix(&view.key);
        layout.view_settings(
            &prefix,
            &ViewFields::of(view.view_type),
            &settings,
            &filter,
            &context,
        );
        if view.view_type.is_queries() {
            // The columns the view shows (its own `columns:` or the global
            // list); changes become the view's own list
            let columns = context.lock().unwrap().queries_columns(&view.view_name);
            layout.column();
            layout.section("columns (in display order):");
            for col in ordered_query_columns(&columns) {
                let Some(label) = query_column_id(col) else {
                    continue;
                };
                let visible = columns.contains(&label);
                layout.checkbox(&label, &format!("{}qcol_{}", prefix, label), visible);
            }
            layout.edit_columns(
                "add (ProfileEvents.<Name> Settings.<name>)",
                &format!("{}qcol_extra", prefix),
                "",
                40,
                context.clone(),
                ColumnSource::Queries(view.view_name.clone()),
            );
        }
    }

    layout.tab("Perfetto");
    layout.section("Perfetto (query):");
    layout.checkbox(
        "opentelemetry_span_log",
        "set_otel",
        opts.perfetto.opentelemetry_span_log,
    );
    layout.checkbox("trace_log", "set_trace_log", opts.perfetto.trace_log);
    layout.checkbox(
        "query_metric_log",
        "set_query_metric_log",
        opts.perfetto.query_metric_log,
    );
    layout.checkbox("part_log", "set_part_log", opts.perfetto.part_log);
    layout.checkbox(
        "query_thread_log",
        "set_query_thread_log",
        opts.perfetto.query_thread_log,
    );
    layout.checkbox("text_log", "set_text_log", opts.perfetto.text_log);
    layout.checkbox(
        "text_log_android",
        "set_text_log_android",
        opts.perfetto.text_log_android,
    );
    layout.checkbox("per_server", "set_per_server", opts.perfetto.per_server);
    layout.checkbox(
        "stack_traces_by_thread",
        "set_stack_traces_by_thread",
        opts.perfetto.stack_traces_by_thread,
    );

    layout.column();
    layout.section("Perfetto (server):");
    layout.checkbox("metric_log", "set_metric_log", opts.perfetto.metric_log);
    layout.checkbox(
        "asynchronous_metric_log",
        "set_async_metric_log",
        opts.perfetto.asynchronous_metric_log,
    );
    layout.checkbox(
        "asynchronous_insert_log",
        "set_async_insert_log",
        opts.perfetto.asynchronous_insert_log,
    );
    layout.checkbox("error_log", "set_error_log", opts.perfetto.error_log);
    layout.checkbox(
        "s3_queue_log",
        "set_s3_queue_log",
        opts.perfetto.s3_queue_log,
    );
    layout.checkbox(
        "azure_queue_log",
        "set_azure_queue_log",
        opts.perfetto.azure_queue_log,
    );
    layout.checkbox(
        "blob_storage_log",
        "set_blob_storage_log",
        opts.perfetto.blob_storage_log,
    );
    layout.checkbox(
        "background_schedule_pool_log",
        "set_bg_pool_log",
        opts.perfetto.background_schedule_pool_log,
    );
    layout.checkbox("session_log", "set_session_log", opts.perfetto.session_log);
    layout.checkbox(
        "aggregated_zookeeper_log",
        "set_zk_log",
        opts.perfetto.aggregated_zookeeper_log,
    );
    layout.separator();

    layout.section("Perfetto (export):");
    layout.checkbox("compress", "set_compress", opts.perfetto.compress);

    let (tabs, sections, targets) = layout.into_tabs();
    let search = Arc::new(SettingsSearch {
        sections,
        targets,
        state: Mutex::new(SearchState::default()),
    });

    let context_for_apply = context.clone();
    let context_for_enter = context;

    let content = submit_on_enter(tabs, move |app| {
        apply_settings(app, &context_for_enter);
    });

    let dialog = Dialog::new()
        .title("Settings")
        .content(content)
        .button("Apply", move |app| {
            apply_settings(app, &context_for_apply);
        })
        .button("Cancel", |app| {
            app.pop_layer();
        });

    // '/' opens a search prompt that focuses the matching option; EditViews
    // consume characters first, so typing '/' inside one still works.
    let dialog = OnEventView::new(dialog.with_name("settings")).on_event('/', move |app| {
        let search = search.clone();
        show_bottom_prompt(app, "/", move |app, text| {
            app.pop_layer();
            apply_settings_search(app, &search, text);
        });
    });
    app.add_layer(dialog);
}
