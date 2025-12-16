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
                    format!("Parts for: {}.{}", db, tbl)
                } else {
                    format!("Table Parts: {}.{}", db, tbl)
                }
            }
            (Some(db), None) => {
                if for_dialog {
                    format!("Parts for database: {}", db)
                } else {
                    format!("Table Parts: {}", db)
                }
            }
            (None, Some(tbl)) => {
                if for_dialog {
                    format!("Parts for table: {}", tbl)
                } else {
                    format!("Table Parts: table {}", tbl)
                }
            }
            (None, None) => "Table Parts".to_string(),
        }
    }

    fn generate_view_name(&self) -> String {
        format!(
            "table_parts_{}_{}",
            self.database.as_deref().unwrap_or("any"),
            self.table.as_deref().unwrap_or("any"),
        )
    }
}

fn build_query(context: &ContextArc, filters: &FilterParams) -> String {
    let limit = context.lock().unwrap().options.clickhouse.limit;

    let dbtable = context
        .lock()
        .unwrap()
        .clickhouse
        .get_table_name("system", "parts");

    let where_clause = if filters.build_where_clauses().is_empty() {
        String::new()
    } else {
        format!("WHERE {}", filters.build_where_clauses().join(" AND "))
    };

    format!(
        r#"
        SELECT
            name,
            partition,
            rows,
            bytes_on_disk,
            data_compressed_bytes,
            data_uncompressed_bytes,
            modification_time,
            active
        FROM {dbtable}
        {where_clause}
        ORDER BY modification_time DESC
        LIMIT {limit}
        "#,
        dbtable = dbtable,
        where_clause = where_clause,
        limit = limit,
    )
}

fn get_columns() -> (Vec<&'static str>, Vec<&'static str>) {
    let columns = vec![
        "name",
        "partition",
        "rows",
        "bytes_on_disk",
        "data_compressed_bytes",
        "data_uncompressed_bytes",
        "modification_time",
        "active",
    ];
    let columns_to_compare = vec!["name"];
    (columns, columns_to_compare)
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

    let filters = FilterParams { database, table };

    let query = build_query(&context, &filters);
    let (columns, columns_to_compare) = get_columns();

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "modification_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut().set_on_submit(show_part_details);

    let title = filters.build_title(false);

    siv.drop_main_view();
    siv.set_main_view(Dialog::around(view.with_name(view_name).full_screen()).title(title));
    siv.focus_name(view_name).unwrap();
}

pub fn show_table_parts_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = FilterParams { database, table };

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let query = build_query(&context, &filters);
    let (columns, columns_to_compare) = get_columns();

    let mut sql_view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "modification_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view.get_inner_mut().set_on_submit(show_part_details);

    let title = filters.build_title(true);

    siv.add_layer(
        Dialog::around(
            LinearLayout::vertical()
                .child(TextView::new(title).center())
                .child(DummyView.fixed_height(1))
                .child(sql_view.with_name(view_name).min_size((140, 30))),
        )
        .title("Table Parts"),
    );
}
