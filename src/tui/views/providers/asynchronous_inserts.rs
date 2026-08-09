// Registered in providers::register() once the views menu wiring is ported.

use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Dialog, Nameable, Navigation, Resizable, SizeConstraint, ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
    },
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

    fn show(&self, app: &mut App, context: ContextArc) {
        show_asynchronous_inserts(app, context, None, None);
    }
}

fn build_query(
    context: &ContextArc,
    filters: &super::TableFilterParams,
    is_dialog: bool,
) -> String {
    let (limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.clickhouse.limit,
            ctx.clickhouse.get_table_name("asynchronous_inserts"),
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
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    let select_clause = if is_dialog {
        r#"query,
            total_bytes,
            format,
            first_update::DateTime first_update"#
    } else {
        r#"database,
            table,
            query,
            total_bytes,
            format,
            first_update::DateTime first_update"#
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
        select_clause = select_clause,
        dbtable = dbtable,
        where_clause = where_clause,
        limit = limit,
    )
}

fn get_columns(is_dialog: bool) -> (Vec<&'static str>, Vec<&'static str>) {
    let columns = if is_dialog {
        vec!["query", "total_bytes", "format", "first_update"]
    } else {
        vec![
            "database",
            "table",
            "query",
            "total_bytes",
            "format",
            "first_update",
        ]
    };
    let columns_to_compare = vec!["first_update"];
    (columns, columns_to_compare)
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
) {
    let view_name = "asynchronous_inserts";

    if app.has_view(view_name) {
        return;
    }

    let filters = super::TableFilterParams::new(
        database,
        table,
        "asynchronous_inserts",
        "Asynchronous Inserts",
    );

    let query = build_query(&context, &filters, false);
    let (columns, columns_to_compare) = get_columns(false);

    let mut view = SQLQueryView::new(
        context.clone(),
        view_name,
        "first_update",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut().set_on_submit(show_insert_details);

    view.get_inner_mut().set_title(filters.build_title(false));

    app.present_view(view_name, view.with_name(view_name).full_screen());
}

pub fn show_asynchronous_inserts_dialog(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = super::TableFilterParams::new(
        database,
        table,
        "asynchronous_inserts",
        "Asynchronous Inserts",
    );

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let query = build_query(&context, &filters, true);
    let (columns, columns_to_compare) = get_columns(true);

    let mut sql_view = SQLQueryView::new(
        context.clone(),
        view_name,
        "first_update",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view.get_inner_mut().set_on_submit(show_insert_details);
    sql_view
        .get_inner_mut()
        .set_title(filters.build_title(true));

    app.add_layer(
        Dialog::around(
            sql_view
                .with_name(view_name)
                .resized(SizeConstraint::AtLeast(140), SizeConstraint::AtLeast(30)),
        )
        .title("Asynchronous Inserts"),
    );
}
