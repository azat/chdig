use crate::{
    interpreter::{ClickHouseAvailableQuirks, ContextArc, options::ChDigViews},
    tui::{
        App, Event, Nameable, Navigation, Resizable, ViewProvider,
        actions::ActionDescription,
        fuzzy_actions,
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

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        if app.focus_name("replicas") {
            return;
        }

        let (has_uuid, has_zookeeper_name) = {
            let quirks = &context.lock().unwrap().clickhouse.quirks;
            (
                quirks.has(ClickHouseAvailableQuirks::SystemReplicasUUID),
                quirks.has(ClickHouseAvailableQuirks::SystemReplicasZooKeeperName),
            )
        };
        let mut columns = vec![
            "database",
            "table",
            "is_readonly readonly",
            "parts_to_check",
            "queue_size queue",
            "absolute_delay delay",
            "last_queue_update last_update",
            "zookeeper_path _zookeeper_path",
            "replica_path _replica_path",
        ];

        if has_uuid {
            columns.push("uuid _uuid");
        }
        if has_zookeeper_name {
            columns.push("zookeeper_name _zookeeper_name");
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
            vec!["database", "table"],
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get replicas"));

        let logger_names_patterns = if has_uuid {
            vec!["{database}.{table} ({_uuid_raw})"]
        } else {
            vec!["{database}.{table} %"]
        };
        let replicas_actions_callback =
            move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                show_replica_actions(app, columns, row, &logger_names_patterns);
            };
        view.get_inner_mut()
            .set_on_submit(replicas_actions_callback);
        view.get_inner_mut().set_title("Replicas");

        app.present_view("replicas", view.with_name("replicas").full_screen());
    }
}

fn show_replica_actions(
    app: &mut App,
    columns: Vec<&'static str>,
    row: QueryResultRow,
    logger_names_patterns: &[&'static str],
) {
    let actions = [
        "Show replica logs",
        "Open replica_path in ZooKeeper",
        "Open zookeeper_path in ZooKeeper",
    ]
    .into_iter()
    .map(|text| ActionDescription {
        text,
        event: Event::Unknown(vec![]),
    })
    .collect();
    let logger_names_patterns = logger_names_patterns.to_vec();
    fuzzy_actions(app, actions, move |app, selected| {
        let field = |name: &str| {
            columns
                .iter()
                .zip(row.0.iter())
                .find_map(|(c, r)| (*c == name).then(|| r.to_string()))
                .unwrap_or_default()
        };
        let context = app.user_data::<ContextArc>().unwrap().clone();
        match selected.as_str() {
            "Show replica logs" => super::query_result_show_logs_for_row(
                app,
                columns.clone(),
                row.clone(),
                &logger_names_patterns,
                "replica_logs",
            ),
            // No zookeeper_name column (before 23.5) = the default ZooKeeper
            "Open replica_path in ZooKeeper" => super::zookeeper::show_zookeeper(
                app,
                context,
                &field("_zookeeper_name"),
                &field("_replica_path"),
            ),
            "Open zookeeper_path in ZooKeeper" => super::zookeeper::show_zookeeper(
                app,
                context,
                &field("_zookeeper_name"),
                &field("_zookeeper_path"),
            ),
            _ => {}
        }
    });
}
