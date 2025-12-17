use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, TextView},
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

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_asynchronous_inserts(siv, context, None, None);
    }
}

struct FilterParams {
    database: Option<String>,
    table: Option<String>,
}

impl FilterParams {
    fn build_where_clauses(&self) -> Vec<String> {
        let mut clauses = vec![];

        if let Some(ref database) = self.database {
            clauses.push(format!("database = '{}'", database.replace('\'', "''")));
        }
        if let Some(ref table) = self.table {
            clauses.push(format!("table = '{}'", table.replace('\'', "''")));
        }

        clauses
    }

    fn build_title(&self, for_dialog: bool) -> String {
        match (&self.database, &self.table) {
            (Some(db), Some(tbl)) => {
                if for_dialog {
                    format!("Asynchronous inserts for: {}.{}", db, tbl)
                } else {
                    format!("Asynchronous Inserts: {}.{}", db, tbl)
                }
            }
            (Some(db), None) => {
                if for_dialog {
                    format!("Asynchronous inserts for database: {}", db)
                } else {
                    format!("Asynchronous Inserts: {}", db)
                }
            }
            (None, Some(tbl)) => {
                if for_dialog {
                    format!("Asynchronous inserts for table: {}", tbl)
                } else {
                    format!("Asynchronous Inserts: table {}", tbl)
                }
            }
            (None, None) => "Asynchronous Inserts".to_string(),
        }
    }

    fn generate_view_name(&self) -> String {
        format!(
            "asynchronous_inserts_{}_{}",
            self.database.as_deref().unwrap_or("any"),
            self.table.as_deref().unwrap_or("any"),
        )
    }
}

fn build_query(context: &ContextArc, filters: &FilterParams, is_dialog: bool) -> String {
    let limit = context.lock().unwrap().options.clickhouse.limit;

    let dbtable = context
        .lock()
        .unwrap()
        .clickhouse
        .get_table_name("system", "asynchronous_inserts");

    let where_clause = if filters.build_where_clauses().is_empty() {
        String::new()
    } else {
        format!("WHERE {}", filters.build_where_clauses().join(" AND "))
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

fn show_insert_details(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
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

    siv.add_layer(Dialog::info(info).title("Asynchronous Insert Details"));
}

pub fn show_asynchronous_inserts(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let view_name = "asynchronous_inserts";

    if siv.has_view(view_name) {
        return;
    }

    let filters = FilterParams { database, table };

    let query = build_query(&context, &filters, false);
    let (columns, columns_to_compare) = get_columns(false);

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "first_update",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut().set_on_submit(show_insert_details);

    let title = filters.build_title(false);

    siv.drop_main_view();
    siv.set_main_view(Dialog::around(view.with_name(view_name).full_screen()).title(title));
    siv.focus_name(view_name).unwrap();
}

pub fn show_asynchronous_inserts_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = FilterParams { database, table };

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let query = build_query(&context, &filters, true);
    let (columns, columns_to_compare) = get_columns(true);

    let mut sql_view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "first_update",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view.get_inner_mut().set_on_submit(show_insert_details);

    let title = filters.build_title(true);

    siv.add_layer(
        Dialog::around(
            LinearLayout::vertical()
                .child(TextView::new(title).center())
                .child(DummyView.fixed_height(1))
                .child(sql_view.with_name(view_name).min_size((140, 30))),
        )
        .title("Asynchronous Inserts"),
    );
}
