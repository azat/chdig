use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{
        ContextArc, PROFILE_EVENTS_PREFIX, TextLogArguments, options::ChDigViews,
        queries_filter::sql_quote,
    },
    tui::{
        App, Dialog, DummyView, Event, LinearLayout, Nameable, NamedView, OnEventView, Resizable,
        SizeConstraint, TextView, ViewProvider,
        actions::ActionDescription,
        fuzzy_actions,
        views::sql_query_view::{Field, Row as QueryResultRow, Unit},
        views::table_view::TableView,
        views::text_log_view::TextLogView,
    },
    utils::intern,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PartLogViewProvider;

impl ViewProvider for PartLogViewProvider {
    fn name(&self) -> &'static str {
        "Part Log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::PartLog
    }

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        show_part_log(app, context, None, None, None, Presentation::FullScreen);
    }
}

const COLUMNS: &[&str] = &[
    "event_time",
    "event_type",
    "database",
    "table",
    "part_name",
    "merge_algorithm",
    "part_type",
    "rows",
    "size_in_bytes",
    "duration_ms",
    "peak_memory_usage",
    "exception",
    "table_uuid _table_uuid",
    "event_time_microseconds _event_time_microseconds",
    // clickhouse-rs cannot decode LowCardinality map keys.
    "CAST(ProfileEvents, 'Map(String, UInt64)') _profile_events",
];

/// Always selected (row identity, the actions), whatever `columns:` lists
const REQUIRED_COLUMNS: &[&str] = &["event_time", "event_type", "part_name"];

/// The rendered name of a `COLUMNS` entry (its alias).
fn column_alias(column: &str) -> &str {
    column.rsplit(' ').next().unwrap()
}

/// Names the `columns:` setting may list (the hidden `_` columns are always
/// selected), besides `ProfileEvents.<Name>`.
pub fn column_labels() -> impl Iterator<Item = &'static str> {
    COLUMNS
        .iter()
        .map(|column| column_alias(column))
        .filter(|alias| !alias.starts_with('_'))
}

pub fn is_column(label: &str) -> bool {
    label
        .strip_prefix(PROFILE_EVENTS_PREFIX)
        .is_some_and(|name| !name.is_empty())
        || column_labels().any(|alias| alias == label)
}

/// `COLUMNS` reduced/reordered to `configured` (empty = all), with the
/// `ProfileEvents.<Name>` entries as extra columns and the hidden `_` columns
/// kept; plus the value units of the event columns.
fn configured_columns(configured: &[String]) -> (Vec<&'static str>, Vec<(&'static str, Unit)>) {
    if configured.is_empty() {
        return (COLUMNS.to_vec(), Vec::new());
    }
    let mut columns = Vec::new();
    let mut units = Vec::new();
    for label in configured {
        if let Some(name) = label
            .strip_prefix(PROFILE_EVENTS_PREFIX)
            .filter(|name| !name.is_empty())
        {
            let name = intern(name);
            columns.push(intern(&format!(
                "ProfileEvents[{}] {}",
                sql_quote(name),
                name
            )));
            units.push((name, Unit::for_profile_event(name)));
        } else if let Some(column) = COLUMNS.iter().find(|c| column_alias(c) == label) {
            columns.push(*column);
        } else {
            log::warn!("part_log: unknown column '{}'", label);
        }
    }
    // The row identity and the actions need these, the hidden ones always
    columns.extend(
        COLUMNS
            .iter()
            .copied()
            .filter(|column| {
                let alias = column_alias(column);
                (alias.starts_with('_') || REQUIRED_COLUMNS.contains(&alias))
                    && !columns.contains(column)
            })
            .collect::<Vec<_>>(),
    );
    (columns, units)
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
            ctx.clickhouse.get_log_table_name("part_log"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let (with_prelude, mut where_clauses) = super::log_time_window(context, view_name);
    // Useful only for merge vizualization
    where_clauses.push("event_type != 'MergePartsStart'".to_string());
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

fn show_part_logs(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let mut map = HashMap::new();
    columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
        map.insert(c.to_string(), r);
    });

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.lock().unwrap().options.view.clone();
    app.add_layer(Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Logs:").center())
            .child(DummyView.fixed_height(1))
            .child(NamedView::new(
                "part_logs",
                TextLogView::new(
                    "part_logs",
                    context,
                    TextLogArguments {
                        query_ids_subquery: None,
                        query_ids: Some(vec![format!(
                            "{}::{}",
                            map["_table_uuid"].to_string(),
                            map["part_name"].to_string()
                        )]),
                        logger_names: None,
                        hostname: None,
                        message_filter: None,
                        max_level: None,
                        limit: None,
                        start: map["event_time"].as_datetime().unwrap(),
                        end: view_options.end,
                    },
                ),
            )),
    ));
    app.focus_name("part_logs");
}

fn show_part_details(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let width = columns.iter().map(|c| c.len()).max().unwrap_or_default();
    let info = columns
        .iter()
        .filter(|c| **c != "_profile_events")
        .filter_map(|c| map.get(*c).map(|v| (*c, v)))
        .map(|(c, v)| format!("{:<width$}: {}", c, v, width = width))
        .collect::<Vec<_>>()
        .join("\n");

    app.add_layer(Dialog::info(info).title("Part Log Details"));
}

fn show_part_profile_events(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let fields: HashMap<_, _> = columns.iter().copied().zip(row.0.iter()).collect();
    let Field::UInt64Map(events) = fields["_profile_events"] else {
        unreachable!("ProfileEvents must be Map(String, UInt64)");
    };
    let items: Arc<Vec<_>> = Arc::new(
        events
            .iter()
            .filter(|(_, value)| **value != 0)
            .map(|(name, value)| {
                let mut row = QueryResultRow::default();
                row.0 = vec![Field::String(name.clone()), Field::UInt64(*value)];
                row
            })
            .collect(),
    );
    let mut table = TableView::<QueryResultRow, u8>::new();
    table.add_column(0, "name", |c| c.width_min(20));
    table.add_column(1, "value", |c| c.width_min_max(5, 20));
    table.set_items((*items).clone());
    table.sort_by(1, Ordering::Greater);
    table.set_title(format!(
        "{} {} profile events",
        fields["part_name"], fields["event_type"]
    ));
    let view_name = "part_profile_events";
    let view = OnEventView::new(table.with_name(view_name)).on_event('/', move |app| {
        let items = items.clone();
        crate::tui::show_bottom_prompt(app, "/", move |app, text| {
            let filter = text.to_lowercase();
            app.call_on_name(view_name, |table: &mut TableView<QueryResultRow, u8>| {
                table.set_items(
                    items
                        .iter()
                        .filter(|row| {
                            row.0
                                .iter()
                                .any(|field| field.to_string().to_lowercase().contains(&filter))
                        })
                        .cloned()
                        .collect(),
                );
            });
            app.pop_layer();
        });
    });
    app.add_layer(Dialog::around(
        view.resized(SizeConstraint::AtLeast(80), SizeConstraint::AtLeast(30)),
    ));
}

fn part_log_action_callback(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let actions = vec![
        ActionDescription {
            text: "Show part logs",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show part details",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show part profile events",
            event: Event::Unknown(vec![]),
        },
    ];

    let columns_clone = columns.clone();
    let row_clone = row.clone();

    fuzzy_actions(app, actions, move |app, selected| match selected.as_str() {
        "Show part logs" => {
            show_part_logs(app, columns_clone.clone(), row_clone.clone());
        }
        "Show part details" => {
            show_part_details(app, columns_clone.clone(), row_clone.clone());
        }
        "Show part profile events" => {
            show_part_profile_events(app, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

pub fn show_part_log(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    table_uuid: Option<String>,
    presentation: Presentation,
) {
    let filters = TableFilterParams::new(database, table, "part_log", "Part Log")
        .with_eq("table_uuid", table_uuid);

    let view_name = filters.view_name(presentation);
    let configured = context.lock().unwrap().view_columns(&view_name);
    let (base_columns, units) = configured_columns(&configured);
    let mut columns = if presentation.is_dialog() {
        super::dialog_columns(&base_columns)
    } else {
        base_columns
    };
    columns.push(
        match context.lock().unwrap().clickhouse.get_log_hostname_column() {
            "hostname" => "hostname _hostname",
            _ => "hostName() _hostname",
        },
    );

    let wide_columns: Vec<&'static str> = ["exception"]
        .into_iter()
        .filter(|wide| columns.contains(wide))
        .collect();
    let spec = QueryTableSpec {
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Part Log".to_string(),
        sort_by: "event_time",
        query: build_query(&context, &view_name, &filters, &columns),
        view_name,
        columns,
        columns_to_compare: vec![
            "_event_time_microseconds",
            "_hostname",
            "_table_uuid",
            "event_type",
            "part_name",
        ],
        wide_columns,
    };
    super::present_query_table_configured(
        app,
        context,
        spec,
        part_log_action_callback,
        presentation,
        move |view| {
            for (column, unit) in units {
                view.set_value_unit(column, unit);
            }
        },
    );
}
