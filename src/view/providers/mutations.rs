use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, TextView},
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

struct FilterParams {
    database: Option<String>,
    table: Option<String>,
}

impl FilterParams {
    fn build_where_clauses(&self) -> Vec<String> {
        let mut clauses = vec!["is_done = 0".to_string()];

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
                    format!("Mutations for: {}.{}", db, tbl)
                } else {
                    format!("Mutations: {}.{}", db, tbl)
                }
            }
            (Some(db), None) => {
                if for_dialog {
                    format!("Mutations for database: {}", db)
                } else {
                    format!("Mutations: {}", db)
                }
            }
            (None, Some(tbl)) => {
                if for_dialog {
                    format!("Mutations for table: {}", tbl)
                } else {
                    format!("Mutations: table {}", tbl)
                }
            }
            (None, None) => "Mutations".to_string(),
        }
    }

    fn generate_view_name(&self) -> String {
        format!(
            "mutations_{}_{}",
            self.database.as_deref().unwrap_or("any"),
            self.table.as_deref().unwrap_or("any"),
        )
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

fn build_query(context: &ContextArc, filters: &FilterParams, is_dialog: bool) -> String {
    let columns = get_columns(is_dialog);
    let where_clauses = filters.build_where_clauses();

    let mutations_dbtable = context
        .lock()
        .unwrap()
        .clickhouse
        .get_table_name("system", "mutations");

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

    let filters = FilterParams { database, table };
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

    let title = filters.build_title(false);

    siv.drop_main_view();
    siv.set_main_view(Dialog::around(view.with_name(view_name).full_screen()).title(title));
    siv.focus_name(view_name).unwrap();
}

pub fn show_mutations_dialog(
    siv: &mut Cursive,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let filters = FilterParams { database, table };

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

    let title = filters.build_title(true);

    siv.add_layer(
        Dialog::around(
            LinearLayout::vertical()
                .child(TextView::new(title).center())
                .child(DummyView.fixed_height(1))
                .child(sql_view.with_name(view_name).min_size((140, 30))),
        )
        .title("Mutations"),
    );
}
