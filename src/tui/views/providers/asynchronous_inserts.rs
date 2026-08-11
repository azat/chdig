use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{App, Dialog, ViewProvider, views::sql_query_view::Row as QueryResultRow},
};
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
        .filter_map(|c| map.get(*c).map(|v| (*c, v)))
        .map(|(c, v)| format!("{:<width$}: {}", c, v, width = width))
        .collect::<Vec<_>>()
        .join("\n");

    app.add_layer(Dialog::info(info).title("Asynchronous Insert Details"));
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
    super::present_query_table(app, context, spec, show_insert_details, presentation);
}
