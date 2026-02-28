use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::Dialog,
};

pub struct MutationsViewProvider;

impl ViewProvider for MutationsViewProvider {
    fn name(&self) -> &'static str {
        "Mutations"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Mutations
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_mutations(siv, context, None, None);
    }
}

fn get_columns(is_dialog: bool) -> Vec<&'static str> {
    if is_dialog {
        vec![
            "mutation_id",
            "command",
            "create_time",
            "parts_to_do parts",
            "is_done",
            "latest_fail_reason",
            "latest_fail_time",
        ]
    } else {
        vec![
            "database",
            "table",
            "mutation_id",
            "command",
            "create_time",
            "parts_to_do parts",
            "is_done",
            "latest_fail_reason",
            "latest_fail_time",
        ]
    }
}

fn build_query(
    context: &ContextArc,
    filters: &super::TableFilterParams,
    is_dialog: bool,
) -> String {
    let columns = get_columns(is_dialog);
    let mut where_clauses = vec!["is_done = 0".to_string()];
    where_clauses.extend(filters.build_where_clauses());

    let (mutations_dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.clickhouse.get_table_name("system", "mutations"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
    if !host_filter.is_empty() {
        where_clauses.push(format!("1 {}", host_filter));
    }

    format!(
        "select {} from {} as mutations WHERE {}",
        columns.join(", "),
        mutations_dbtable,
        where_clauses.join(" AND "),
    )
}

fn show_mutations(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let view_name = "mutations";

    if siv.has_view(view_name) {
        return;
    }

    let filters = super::TableFilterParams::new(database, table, "mutations", "Mutations");
    let columns = get_columns(false);
    let query = build_query(&context, &filters, false);

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "latest_fail_time",
        columns,
        vec!["database", "table", "mutation_id"],
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    // TODO:
    // - on_submit show assigned merges (but first, need to expose enough info in system tables)
    // - sort by create_time OR latest_fail_time
    view.get_inner_mut()
        .set_on_submit(super::query_result_show_row);

    view.get_inner_mut().set_title(filters.build_title(false));

    siv.drop_main_view();
    siv.set_main_view(view.with_name(view_name).full_screen());
    siv.focus_name(view_name).unwrap();
}

pub fn show_mutations_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = super::TableFilterParams::new(database, table, "mutations", "Mutations");

    let view_name: &'static str = Box::leak(filters.generate_view_name().into_boxed_str());
    let columns = get_columns(true);
    let query = build_query(&context, &filters, true);

    let mut sql_view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "latest_fail_time",
        columns,
        vec!["mutation_id"],
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    sql_view
        .get_inner_mut()
        .set_on_submit(super::query_result_show_row);
    sql_view
        .get_inner_mut()
        .set_title(filters.build_title(true));

    siv.add_layer(
        Dialog::around(sql_view.with_name(view_name).min_size((140, 30))).title("Mutations"),
    );
}
