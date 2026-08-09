use crate::{
    interpreter::{ClickHouseAvailableQuirks, ContextArc, options::ChDigViews},
    tui::{
        App, Nameable, Navigation, Resizable, ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
    },
};

pub struct ReplicasViewProvider;

impl ViewProvider for ReplicasViewProvider {
    fn name(&self) -> &'static str {
        "Replicas"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Replicas
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.has_view("replicas") {
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
            columns.push("uuid _uuid");
        }

        let (cluster, dbtable, clickhouse, selected_host) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.clickhouse.cluster.is_some(),
                ctx.clickhouse.get_table_name("replicas"),
                ctx.clickhouse.clone(),
                ctx.selected_host.clone(),
            )
        };

        // Only show hostname column when in cluster mode AND no host filter is active
        let columns_to_compare = if cluster && selected_host.is_none() {
            columns.insert(0, "hostName() host");
            vec!["host", "database", "table"]
        } else {
            vec!["database", "table"]
        };

        let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
        let where_clause = if host_filter.is_empty() {
            String::new()
        } else {
            format!("WHERE 1 {}", host_filter)
        };

        let query = format!(
            "SELECT DISTINCT ON (database, table, zookeeper_path) {} FROM {} {} ORDER BY queue_size DESC, database, table",
            columns.join(", "),
            dbtable,
            where_clause,
        );

        let mut view = SQLQueryView::new(
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
            move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                super::query_result_show_logs_for_row(
                    app,
                    columns,
                    row,
                    &logger_names_patterns,
                    "replica_logs",
                );
            };
        view.get_inner_mut().set_on_submit(replicas_logs_callback);
        view.get_inner_mut().set_title("Replicas");

        app.present_view("replicas", view.with_name("replicas").full_screen());
    }
}
