use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::Dialog,
};

pub struct TablesViewProvider;

impl ViewProvider for TablesViewProvider {
    fn name(&self) -> &'static str {
        "Tables"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Tables
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("tables") {
            return;
        }

        let mut columns = vec![
            "database",
            "table",
            "uuid::String _uuid",
            "assumeNotNull(total_bytes) total_bytes",
            "assumeNotNull(total_rows) total_rows",
            // TODO: support number of background jobs counter in ClickHouse
        ];

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let columns_to_compare = if cluster {
            columns.insert(0, "hostName() host");
            vec!["host", "database", "table"]
        } else {
            vec!["database", "table"]
        };

        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "tables");

        let query = format!(
            "SELECT DISTINCT ON (database, table, uuid) {} FROM {} WHERE engine NOT LIKE 'System%' AND database NOT IN ('INFORMATION_SCHEMA', 'information_schema') ORDER BY database, table, total_bytes DESC",
            columns.join(", "),
            dbtable,
        );

        siv.drop_main_view();

        let mut view = view::SQLQueryView::new(
            context.clone(),
            "tables",
            "total_bytes",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get tables"));

        // TODO: proper escape of _/%
        let logger_names_patterns = vec!["%{database}.{table}%", "%{_uuid}%"];
        let tables_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                super::query_result_show_logs_for_row(
                    siv,
                    columns,
                    row,
                    &logger_names_patterns,
                    "table_logs",
                );
            };
        view.set_on_submit(tables_logs_callback);

        let view = view.with_name("tables").full_screen();
        siv.set_main_view(Dialog::around(view).title("Tables"));
    }
}
