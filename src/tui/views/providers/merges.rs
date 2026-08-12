use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, LinearLayout, NamedView, Resizable, TextView, ViewProvider,
        views::sql_query_view::Row as QueryResultRow, views::text_log_view::TextLogView,
    },
};
use std::collections::HashMap;

pub struct MergesViewProvider;

impl ViewProvider for MergesViewProvider {
    fn name(&self) -> &'static str {
        "Merges"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Merges
    }

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        show_merges(app, context, None, None, Presentation::FullScreen);
    }
}

const COLUMNS: &[&str] = &[
    "database",
    "table",
    "result_part_name part",
    "elapsed",
    "progress",
    "num_parts parts",
    "is_mutation mutation",
    "total_size_bytes_compressed size",
    "rows_read",
    "rows_written",
    "memory_usage memory",
    "now()-elapsed _create_time",
    "tables.uuid _table_uuid",
];

fn build_query(context: &ContextArc, filters: &TableFilterParams, columns: &[&str]) -> String {
    let (tables_dbtable, merges_dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.clickhouse.get_table_name("tables"),
            ctx.clickhouse.get_table_name("merges"),
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
        "select {} from {} as merges left join (select distinct on (database, name) database, name, uuid from {}) tables on merges.database = tables.database and merges.table = tables.name{}",
        columns.join(", "),
        merges_dbtable,
        tables_dbtable,
        where_clause,
    )
}

fn merges_logs_callback(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
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
                "merge_logs",
                TextLogView::new(
                    "merge_logs",
                    context,
                    crate::interpreter::TextLogArguments {
                        query_ids_subquery: None,
                        query_ids: Some(vec![format!(
                            "{}::{}",
                            map["_table_uuid"].to_string(),
                            map["part"].to_string()
                        )]),
                        logger_names: None,
                        hostname: None,
                        message_filter: None,
                        max_level: None,
                        limit: None,
                        start: map["_create_time"].as_datetime().unwrap(),
                        end: view_options.end,
                    },
                ),
            )),
    ));
    app.focus_name("merge_logs");
}

pub fn show_merges(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    presentation: Presentation,
) {
    let filters =
        TableFilterParams::new(database, table, "merges", "Merges").with_table_prefix("merges");

    let columns = if presentation.is_dialog() {
        super::dialog_columns(COLUMNS)
    } else {
        COLUMNS.to_vec()
    };
    let columns_to_compare = if presentation.is_dialog() {
        vec!["part"]
    } else {
        vec!["database", "table", "part"]
    };

    let spec = QueryTableSpec {
        view_name: filters.view_name(presentation),
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Merges".to_string(),
        sort_by: "elapsed",
        query: build_query(&context, &filters, &columns),
        columns,
        columns_to_compare,
        wide_columns: vec!["part"],
    };
    super::present_query_table(app, context, spec, merges_logs_callback, presentation);
}
