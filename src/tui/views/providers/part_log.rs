use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, TextLogArguments, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, Event, LinearLayout, NamedView, Resizable, TextView, ViewProvider,
        actions::ActionDescription, fuzzy_actions, views::sql_query_view::Row as QueryResultRow,
        views::text_log_view::TextLogView,
    },
};
use std::collections::HashMap;

pub struct PartLogViewProvider;

impl ViewProvider for PartLogViewProvider {
    fn name(&self) -> &'static str {
        "Part Log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::PartLog
    }

    fn show(&self, app: &mut App, context: ContextArc) {
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
        .filter_map(|c| map.get(*c).map(|v| (*c, v)))
        .map(|(c, v)| format!("{:<width$}: {}", c, v, width = width))
        .collect::<Vec<_>>()
        .join("\n");

    app.add_layer(Dialog::info(info).title("Part Log Details"));
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

    let columns = if presentation.is_dialog() {
        super::dialog_columns(COLUMNS)
    } else {
        COLUMNS.to_vec()
    };

    let view_name = filters.view_name(presentation);
    let spec = QueryTableSpec {
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Part Log".to_string(),
        sort_by: "event_time",
        query: build_query(&context, &view_name, &filters, &columns),
        view_name,
        columns,
        columns_to_compare: vec!["event_time", "event_type", "part_name"],
        wide_columns: vec!["exception"],
    };
    super::present_query_table(app, context, spec, part_log_action_callback, presentation);
}
