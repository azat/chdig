// Registered in providers::register() once the views menu wiring is ported.

use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, LinearLayout, Nameable, NamedView, Navigation, Resizable,
        SizeConstraint, TextView, ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
        views::text_log_view::TextLogView,
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

    fn show(&self, app: &mut App, context: ContextArc) {
        show_merges(app, context, None, None);
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
            "tables.uuid _table_uuid",
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
            "tables.uuid _table_uuid",
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
            ctx.clickhouse.get_table_name("tables"),
            ctx.clickhouse.get_table_name("merges"),
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

    format!(
        "select {} from {} as merges left join (select distinct on (database, name) database, name, uuid from {}) tables on merges.database = tables.database and merges.table = tables.name{}",
        columns.join(", "),
        merges_dbtable,
        tables_dbtable,
        where_clause,
    )
}

fn get_merges_logs_callback()
-> impl Fn(&mut App, Vec<&'static str>, QueryResultRow) + Send + Sync + 'static {
    move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
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
                            end: view_options.end,
                        },
                    ),
                )),
        ));
        app.focus_name("merge_logs");
    }
}

fn show_merges(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let view_name = "merges";

    if app.has_view(view_name) {
        return;
    }

    let filters = super::TableFilterParams::new(database, table, "merges", "Merges")
        .with_table_prefix("merges");
    let columns = get_columns(false);
    let query = build_query(&context, &filters, false);

    let mut view = SQLQueryView::new(
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

    view.get_inner_mut().set_title(filters.build_title(false));

    app.present_view(view_name, view.with_name(view_name).full_screen());
}

pub fn show_merges_dialog(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = super::TableFilterParams::new(database, table, "merges", "Merges")
        .with_table_prefix("merges");

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let columns = get_columns(true);
    let query = build_query(&context, &filters, true);

    let mut sql_view = SQLQueryView::new(
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
    sql_view
        .get_inner_mut()
        .set_title(filters.build_title(true));

    app.add_layer(
        Dialog::around(
            sql_view
                .with_name(view_name)
                .resized(SizeConstraint::AtLeast(140), SizeConstraint::AtLeast(30)),
        )
        .title("Merges"),
    );
}
