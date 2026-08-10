use super::{Presentation, QueryTableSpec, TableFilterParams};
use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{App, ViewProvider},
};

pub struct MutationsViewProvider;

impl ViewProvider for MutationsViewProvider {
    fn name(&self) -> &'static str {
        "Mutations"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Mutations
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_mutations(app, context, None, None, Presentation::FullScreen);
    }
}

const COLUMNS: &[&str] = &[
    "database",
    "table",
    "mutation_id",
    "command",
    "create_time",
    "parts_to_do parts",
    "is_done",
    "latest_fail_reason",
    "latest_fail_time",
];

fn build_query(context: &ContextArc, filters: &TableFilterParams, columns: &[&str]) -> String {
    let (mutations_dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.clickhouse.get_table_name("mutations"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let mut where_clauses = vec!["is_done = 0".to_string()];
    where_clauses.extend(filters.build_where_clauses());
    super::push_host_filter(
        &mut where_clauses,
        &clickhouse,
        selected_host.as_ref(),
        false,
    );

    format!(
        "select {} from {} as mutations WHERE {}",
        columns.join(", "),
        mutations_dbtable,
        where_clauses.join(" AND "),
    )
}

pub fn show_mutations(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
    presentation: Presentation,
) {
    let filters = TableFilterParams::new(database, table, "mutations", "Mutations");

    let columns = if presentation.is_dialog() {
        super::dialog_columns(COLUMNS)
    } else {
        COLUMNS.to_vec()
    };
    let columns_to_compare = if presentation.is_dialog() {
        vec!["mutation_id"]
    } else {
        vec!["database", "table", "mutation_id"]
    };

    // TODO:
    // - on_submit show assigned merges (but first, need to expose enough info in system tables)
    // - sort by create_time OR latest_fail_time
    let spec = QueryTableSpec {
        view_name: filters.view_name(presentation),
        title: filters.build_title(presentation.is_dialog()),
        dialog_title: "Mutations".to_string(),
        sort_by: "latest_fail_time",
        query: build_query(&context, &filters, &columns),
        columns,
        columns_to_compare,
    };
    super::present_query_table(
        app,
        context,
        spec,
        super::query_result_show_row,
        presentation,
    );
}
