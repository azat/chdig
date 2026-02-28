use crate::{
    actions::ActionDescription,
    common::RelativeDateTime,
    interpreter::{ContextArc, TextLogArguments, options::ChDigViews},
    utils::fuzzy_actions,
    view::{self, Navigation, TextLogView, ViewProvider},
};
use cursive::{
    Cursive,
    event::Event,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
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

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_table_parts(siv, context, None, None);
    }
}

fn build_query(
    context: &ContextArc,
    filters: &super::TableFilterParams,
    is_dialog: bool,
) -> String {
    let (limit, parts_dbtable, tables_dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.clickhouse.limit,
            ctx.clickhouse.get_table_name("system", "parts"),
            ctx.clickhouse.get_table_name("system", "tables"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let mut where_clauses = filters.build_where_clauses();

    let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
    if !host_filter.is_empty() {
        where_clauses.push(format!("1 {}", host_filter));
    }

    let where_clause = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    let select_clause = if is_dialog {
        r#"parts.name,
            parts.partition,
            parts.rows,
            parts.bytes_on_disk,
            parts.data_compressed_bytes,
            parts.data_uncompressed_bytes,
            parts.modification_time,
            parts.active,
            tables.uuid::String _table_uuid"#
    } else {
        r#"parts.database,
            parts.table,
            parts.name,
            parts.partition,
            parts.rows,
            parts.bytes_on_disk,
            parts.data_compressed_bytes,
            parts.data_uncompressed_bytes,
            parts.modification_time,
            parts.active,
            tables.uuid::String _table_uuid"#
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
        SETTINGS allow_experimental_analyzer=1
        "#,
        select_clause = select_clause,
        parts_dbtable = parts_dbtable,
        tables_dbtable = tables_dbtable,
        where_clause = where_clause,
        limit = limit,
    )
}

fn get_columns(is_dialog: bool) -> (Vec<&'static str>, Vec<&'static str>) {
    let columns = if is_dialog {
        vec![
            "name",
            "partition",
            "rows",
            "bytes_on_disk",
            "data_compressed_bytes",
            "data_uncompressed_bytes",
            "modification_time",
            "active",
            "_table_uuid",
        ]
    } else {
        vec![
            "database",
            "table",
            "name",
            "partition",
            "rows",
            "bytes_on_disk",
            "data_compressed_bytes",
            "data_uncompressed_bytes",
            "modification_time",
            "active",
            "_table_uuid",
        ]
    };
    let columns_to_compare = vec!["name"];
    (columns, columns_to_compare)
}

fn show_part_logs(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let mut map = HashMap::new();
    columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
        map.insert(c.to_string(), r);
    });

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    siv.add_layer(Dialog::around(
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
                        end: RelativeDateTime::new(None),
                    },
                ),
            )),
    ));
    siv.focus_name("part_logs").unwrap();
}

fn show_part_details(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
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

    siv.add_layer(Dialog::info(info).title("Part Details"));
}

fn table_parts_action_callback(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
) {
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

    fuzzy_actions(siv, actions, move |siv, selected| match selected.as_str() {
        "Show part logs" => {
            show_part_logs(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show part details" => {
            show_part_details(siv, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

pub fn show_table_parts(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let view_name = "table_parts";

    if siv.has_view(view_name) {
        return;
    }

    let filters = super::TableFilterParams::new(database, table, "table_parts", "Table Parts");

    let query = build_query(&context, &filters, false);
    let (columns, columns_to_compare) = get_columns(false);

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "modification_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut()
        .set_on_submit(table_parts_action_callback);

    view.get_inner_mut().set_title(filters.build_title(false));

    siv.drop_main_view();
    siv.set_main_view(view.with_name(view_name).full_screen());
    siv.focus_name(view_name).unwrap();
}

pub fn show_table_parts_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = super::TableFilterParams::new(database, table, "table_parts", "Table Parts");

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let query = build_query(&context, &filters, true);
    let (columns, columns_to_compare) = get_columns(true);

    let mut sql_view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "modification_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view
        .get_inner_mut()
        .set_on_submit(table_parts_action_callback);
    sql_view
        .get_inner_mut()
        .set_title(filters.build_title(true));

    siv.add_layer(
        Dialog::around(sql_view.with_name(view_name).min_size((140, 30))).title("Table Parts"),
    );
}
