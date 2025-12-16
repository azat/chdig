use crate::{
    interpreter::{ClickHouseAvailableQuirks, ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::Dialog,
};

pub struct ReplicasViewProvider;

impl ViewProvider for ReplicasViewProvider {
    fn name(&self) -> &'static str {
        "Replicas"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Replicas
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("replicas") {
            return;
        }

        let has_uuid = context
            .clone()
            .lock()
            .unwrap()
            .clickhouse
            .quirks
            .has(ClickHouseAvailableQuirks::SystemReplicasUUID);
        let mut columns = vec![
            "database",
            "table",
            "is_readonly readonly",
            "parts_to_check",
            "queue_size queue",
            "absolute_delay delay",
            "last_queue_update last_update",
        ];

        if has_uuid {
            columns.push("uuid::String _uuid");
        }

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
            .get_table_name("system", "replicas");

        let query = format!(
            "SELECT DISTINCT ON (database, table, zookeeper_path) {} FROM {} ORDER BY queue_size DESC, database, table",
            columns.join(", "),
            dbtable,
        );

        siv.drop_main_view();

        let mut view = view::SQLQueryView::new(
            context.clone(),
            "replicas",
            "queue",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get replicas"));

        let logger_names_patterns = if has_uuid {
            vec!["{database}.{table} ({_uuid_raw})"]
        } else {
            vec!["{database}.{table} %"]
        };
        let replicas_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                super::query_result_show_logs_for_row(
                    siv,
                    columns,
                    row,
                    &logger_names_patterns,
                    "replica_logs",
                );
            };
        view.get_inner_mut().set_on_submit(replicas_logs_callback);

        let view = view.with_name("replicas").full_screen();
        siv.set_main_view(Dialog::around(view).title("Replicas"));
        siv.focus_name("replicas").unwrap();
    }
}
