use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, TextLogView, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
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

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_merges(siv, context, None, None);
    }
}

fn get_columns(is_dialog: bool) -> Vec<&'static str> {
    if is_dialog {
        vec![
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
            "tables.uuid::String _table_uuid",
        ]
    } else {
        vec![
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
            "tables.uuid::String _table_uuid",
        ]
    }
}

fn build_query(
    context: &ContextArc,
    filters: &super::TableFilterParams,
    is_dialog: bool,
) -> String {
    let columns = get_columns(is_dialog);
    let mut where_clauses = filters.build_where_clauses();

    let (tables_dbtable, merges_dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.clickhouse.get_table_name("system", "tables"),
            ctx.clickhouse.get_table_name("system", "merges"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
    if !host_filter.is_empty() {
        where_clauses.push(format!("1 {}", host_filter));
    }

    let where_clause = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    // NOTE: On 25.8 it fails with "No alias for subquery or table function in JOIN" w/ old analyzer
    format!(
        "select {} from {} as merges left join (select distinct on (database, name) database, name, uuid from {}) tables on merges.database = tables.database and merges.table = tables.name{} SETTINGS allow_experimental_analyzer=1",
        columns.join(", "),
        merges_dbtable,
        tables_dbtable,
        where_clause,
    )
}

fn get_merges_logs_callback()
-> impl Fn(&mut Cursive, Vec<&'static str>, view::QueryResultRow) + 'static {
    move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
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
                    "merge_logs",
                    TextLogView::new(
                        "merge_logs",
                        context,
                        crate::interpreter::TextLogArguments {
                            query_ids: Some(vec![format!(
                                "{}::{}",
                                map["_table_uuid"].to_string(),
                                map["part"].to_string()
                            )]),
                            logger_names: None,
                            hostname: None,
                            message_filter: None,
                            max_level: None,
                            start: map["_create_time"].as_datetime().unwrap(),
                            end: crate::common::RelativeDateTime::new(None),
                        },
                    ),
                )),
        ));
        siv.focus_name("merge_logs").unwrap();
    }
}

fn show_merges(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let view_name = "merges";

    if siv.has_view(view_name) {
        return;
    }

    let filters = super::TableFilterParams::new(database, table, "merges", "Merges")
        .with_table_prefix("merges");
    let columns = get_columns(false);
    let query = build_query(&context, &filters, false);

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "elapsed",
        columns.clone(),
        vec!["database", "table", "part"],
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut()
        .set_on_submit(get_merges_logs_callback());

    let title = filters.build_title(false);

    siv.drop_main_view();
    siv.set_main_view(
        LinearLayout::vertical()
            .child(TextView::new(super::styled_title(&title)).center())
            .child(view.with_name(view_name).full_screen()),
    );
    siv.focus_name(view_name).unwrap();
}

pub fn show_merges_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = super::TableFilterParams::new(database, table, "merges", "Merges")
        .with_table_prefix("merges");

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let columns = get_columns(true);
    let query = build_query(&context, &filters, true);

    let mut sql_view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "elapsed",
        columns,
        vec!["part"],
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view
        .get_inner_mut()
        .set_on_submit(get_merges_logs_callback());

    let title = filters.build_title(true);

    siv.add_layer(
        Dialog::around(
            LinearLayout::vertical()
                .child(TextView::new(title).center())
                .child(DummyView.fixed_height(1))
                .child(sql_view.with_name(view_name).min_size((140, 30))),
        )
        .title("Merges"),
    );
}
