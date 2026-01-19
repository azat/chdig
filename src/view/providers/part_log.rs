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

pub struct PartLogViewProvider;

impl ViewProvider for PartLogViewProvider {
    fn name(&self) -> &'static str {
        "Part Log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::PartLog
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_part_log(siv, context, None, None, None);
    }
}

struct FilterParams {
    database: Option<String>,
    table: Option<String>,
    table_uuid: Option<String>,
}

impl FilterParams {
    fn build_where_clauses(&self) -> Vec<String> {
        let mut clauses = vec![
            "event_date BETWEEN toDate(start_) AND toDate(end_)".to_string(),
            "event_time BETWEEN toDateTime(start_) AND toDateTime(end_)".to_string(),
            // Useful only for merge vizualization
            "event_type != 'MergePartsStart'".to_string(),
        ];

        if let Some(ref database) = self.database {
            clauses.push(format!("database = '{}'", database.replace('\'', "''")));
        }
        if let Some(ref table) = self.table {
            clauses.push(format!("table = '{}'", table.replace('\'', "''")));
        }
        if let Some(ref table_uuid) = self.table_uuid {
            clauses.push(format!("table_uuid = '{}'", table_uuid.replace('\'', "''")));
        }

        clauses
    }

    fn build_title(&self, for_dialog: bool) -> String {
        match (&self.database, &self.table) {
            (Some(db), Some(tbl)) => {
                if for_dialog {
                    format!("Part log for: {}.{}", db, tbl)
                } else {
                    format!("Part Log: {}.{}", db, tbl)
                }
            }
            (Some(db), None) => {
                if for_dialog {
                    format!("Part log for database: {}", db)
                } else {
                    format!("Part Log: {}", db)
                }
            }
            (None, Some(tbl)) => {
                if for_dialog {
                    format!("Part log for table: {}", tbl)
                } else {
                    format!("Part Log: table {}", tbl)
                }
            }
            (None, None) => "Part Log".to_string(),
        }
    }

    fn generate_view_name(&self) -> String {
        format!(
            "part_log_{}_{}_{}",
            self.database.as_deref().unwrap_or("any"),
            self.table.as_deref().unwrap_or("any"),
            self.table_uuid.as_deref().unwrap_or("any")
        )
    }
}

fn build_query(context: &ContextArc, filters: &FilterParams, is_dialog: bool) -> String {
    let (view_options, limit, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.options.clickhouse.limit,
            ctx.clickhouse.get_table_name("system", "part_log"),
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

    let mut where_clauses = filters.build_where_clauses();

    let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
    if !host_filter.is_empty() {
        where_clauses.push(format!("1 {}", host_filter));
    }

    let select_clause = if is_dialog {
        r#"event_time,
            event_type::String event_type,
            part_name,
            merge_algorithm::String merge_algorithm,
            part_type,
            rows,
            size_in_bytes,
            duration_ms,
            peak_memory_usage,
            exception"#
    } else {
        r#"event_time,
            event_type::String event_type,
            database,
            table,
            part_name,
            merge_algorithm::String merge_algorithm,
            part_type,
            rows,
            size_in_bytes,
            duration_ms,
            peak_memory_usage,
            exception"#
    };

    format!(
        r#"
        WITH {start} AS start_, {end} AS end_
        SELECT
            {select_clause}
        FROM {dbtable}
        WHERE
            {where_clause}
        ORDER BY event_time DESC
        LIMIT {limit}
        "#,
        start = start_sql,
        end = end_sql,
        select_clause = select_clause,
        dbtable = dbtable,
        where_clause = where_clauses.join(" AND "),
        limit = limit,
    )
}

fn get_columns(is_dialog: bool) -> (Vec<&'static str>, Vec<&'static str>) {
    let columns = if is_dialog {
        vec![
            "event_time",
            "event_type",
            "part_name",
            "merge_algorithm",
            "part_type",
            "rows",
            "size_in_bytes",
            "duration_ms",
            "peak_memory_usage",
            "exception",
        ]
    } else {
        vec![
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
        ]
    };
    let columns_to_compare = vec!["event_time", "event_type", "part_name"];
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

    siv.add_layer(Dialog::info(info).title("Part Log Details"));
}

pub fn show_part_log(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    table_uuid: Option<String>,
) {
    let view_name = "part_log";

    if siv.has_view(view_name) {
        return;
    }

    let filters = FilterParams {
        database,
        table,
        table_uuid,
    };

    let query = build_query(&context, &filters, false);
    let (columns, columns_to_compare) = get_columns(false);

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "event_time",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut().set_on_submit(show_part_details);

    let title = filters.build_title(false);

    siv.drop_main_view();
    siv.set_main_view(
        LinearLayout::vertical()
            .child(TextView::new(super::styled_title(&title)).center())
            .child(view.with_name(view_name).full_screen()),
    );
    siv.focus_name(view_name).unwrap();
}

pub fn show_part_log_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    table_uuid: Option<String>,
) {
    let filters = FilterParams {
        database,
        table,
        table_uuid,
    };

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let query = build_query(&context, &filters, true);
    let (columns, columns_to_compare) = get_columns(true);

    let mut sql_view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "event_time",
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
        .title("Part Log"),
    );
}
