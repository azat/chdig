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

pub struct TablePartsViewProvider;

impl ViewProvider for TablePartsViewProvider {
    fn name(&self) -> &'static str {
        "Table Parts"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::TableParts
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_table_parts(app, context, None, None, Presentation::FullScreen);
    }
}

// database/name are qualified (and re-aliased) since the tables subquery
// exposes columns with the same names.
const COLUMNS: &[&str] = &[
    "parts.database database",
    "table",
    "parts.name name",
    "partition",
    "rows",
    "bytes_on_disk",
    "data_compressed_bytes",
    "data_uncompressed_bytes",
    "modification_time",
    "active",
    "tables.uuid _table_uuid",
];

fn build_query(context: &ContextArc, filters: &TableFilterParams, columns: &[&str]) -> String {
    let (limit, parts_dbtable, tables_dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.clickhouse.limit,
            ctx.clickhouse.get_table_name("parts"),
            ctx.clickhouse.get_table_name("tables"),
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
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    format!(
        r#"
        SELECT
            {select_clause}
        FROM {parts_dbtable} as parts
        LEFT JOIN (SELECT DISTINCT ON (database, name) database, name, uuid FROM {tables_dbtable}) tables
            ON parts.database = tables.database AND parts.table = tables.name
        {where_clause}
        ORDER BY parts.modification_time DESC
        LIMIT {limit}
        "#,
        select_clause = columns.join(",\n            "),
        parts_dbtable = parts_dbtable,
        tables_dbtable = tables_dbtable,
        where_clause = where_clause,
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
                            map["name"].to_string()
                        )]),
                        logger_names: None,
                        hostname: None,
                        message_filter: None,
                        max_level: None,
                        start: map["modification_time"].as_datetime().unwrap(),
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

    app.add_layer(Dialog::info(info).title("Part Details"));
}

fn table_parts_action_callback(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
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

pub fn show_table_parts(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    presentation: Presentation,
) {
    let filters = TableFilterParams::new(database, table, "table_parts", "Table Parts");

    let columns = if presentation.is_dialog() {
        super::dialog_columns(COLUMNS)
    } else {
        COLUMNS.to_vec()
    };

    let spec = QueryTableSpec {
        view_name: filters.view_name(presentation),
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Table Parts".to_string(),
        sort_by: "modification_time",
        query: build_query(&context, &filters, &columns),
        columns,
        columns_to_compare: vec!["name"],
    };
    super::present_query_table(
        app,
        context,
        spec,
        table_parts_action_callback,
        presentation,
    );
}
