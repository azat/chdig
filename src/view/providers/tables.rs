use crate::{
    actions::ActionDescription,
    interpreter::{ClickHouseAvailableQuirks, ContextArc, WorkerEvent, options::ChDigViews},
    utils::fuzzy_actions,
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    event::Event,
    view::{Nameable, Resizable},
    views::Dialog,
};
use std::collections::HashMap;

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
            "engine",
            "uuid::String _uuid",
            "assumeNotNull(total_bytes) total_bytes",
            "assumeNotNull(total_rows) total_rows",
        ];

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let columns_to_compare = if cluster {
            columns.insert(0, "hostName() host");
            vec!["host", "database", "table"]
        } else {
            vec!["database", "table"]
        };

        let has_background_schedule_pool = context
            .lock()
            .unwrap()
            .clickhouse
            .quirks
            .has(ClickHouseAvailableQuirks::SystemBackgroundSchedulePool);
        if has_background_schedule_pool {
            columns.push("tasks");
        }

        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "tables");

        let query = if has_background_schedule_pool {
            format!(
                r#"
                SELECT DISTINCT ON (tables.database, tables.table, tables.uuid) {}
                FROM {} tables
                JOIN (SELECT table_uuid, count() tasks FROM system.background_schedule_pool GROUP BY table_uuid) bg ON tables.uuid = bg.table_uuid
                WHERE
                    engine NOT LIKE 'System%'
                    AND tables.database NOT IN ('INFORMATION_SCHEMA', 'information_schema')
                ORDER BY database, table, total_bytes DESC
                "#,
                columns.join(", "),
                dbtable,
            )
        } else {
            format!(
                r#"
                SELECT DISTINCT ON (database, table, uuid) {}
                FROM {}
                WHERE
                    engine NOT LIKE 'System%'
                    AND database NOT IN ('INFORMATION_SCHEMA', 'information_schema')
                ORDER BY database, table, total_bytes DESC
                "#,
                columns.join(", "),
                dbtable,
            )
        };

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

        let logger_names_patterns = vec!["%{database}.{table}%", "%{_uuid_raw}%"];
        let tables_action_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                show_table_actions(siv, columns, row, &logger_names_patterns);
            };
        view.get_inner_mut().set_on_submit(tables_action_callback);

        let view = view.with_name("tables").full_screen();
        siv.set_main_view(Dialog::around(view).title("Tables"));
        siv.focus_name("tables").unwrap();
    }
}

fn show_table_actions(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
    logger_names_patterns: &[&'static str],
) {
    let actions = vec![
        ActionDescription {
            text: "Show table logs",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show table background tasks",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show table parts",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show asynchronous inserts",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show table merges",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show table mutations",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show table part log",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "SHOW CREATE TABLE",
            event: Event::Unknown(vec![]),
        },
    ];

    let logger_names_patterns = logger_names_patterns.to_vec();
    let columns_clone = columns.clone();
    let row_clone = row.clone();

    // TODO: Almost all table table from this list can be implemented:
    //
    //   select table from system.columns where name = 'table' and database = 'system'
    //
    fuzzy_actions(siv, actions, move |siv, selected| match selected.as_str() {
        "Show table logs" => {
            show_table_logs(
                siv,
                columns_clone.clone(),
                row_clone.clone(),
                &logger_names_patterns,
            );
        }
        "Show table parts" => {
            show_table_parts(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show asynchronous inserts" => {
            show_table_asynchronous_inserts(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show table merges" => {
            show_table_merges(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show table mutations" => {
            show_table_mutations(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show table background tasks" => {
            show_table_background_tasks_logs(siv, columns_clone.clone(), row_clone.clone());
        }
        "Show table part log" => {
            show_table_part_log(siv, columns_clone.clone(), row_clone.clone());
        }
        "SHOW CREATE TABLE" => {
            show_create_table(siv, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

fn show_create_table(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map
        .get("database")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let table = map.get("table").map(|s| s.to_owned()).unwrap_or_default();

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    context
        .lock()
        .unwrap()
        .worker
        .send(true, WorkerEvent::ShowCreateTable(database, table));
}

fn show_table_logs(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
    logger_names_patterns: &[&'static str],
) {
    super::query_result_show_logs_for_row(siv, columns, row, logger_names_patterns, "table_logs");
}

fn show_table_background_tasks_logs(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());

    let context = siv.user_data::<ContextArc>().unwrap().clone();

    super::background_schedule_pool_log::show_background_schedule_pool_log_dialog(
        siv, context, None, database, table,
    );
}

fn show_table_parts(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map
        .get("database")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let table = map.get("table").map(|s| s.to_owned()).unwrap_or_default();

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    context
        .lock()
        .unwrap()
        .worker
        .send(true, WorkerEvent::TableParts(database, table));
}

fn show_table_asynchronous_inserts(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map
        .get("database")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let table = map.get("table").map(|s| s.to_owned()).unwrap_or_default();

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    context
        .lock()
        .unwrap()
        .worker
        .send(true, WorkerEvent::AsynchronousInserts(database, table));
}

fn show_table_merges(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());

    let context = siv.user_data::<ContextArc>().unwrap().clone();

    super::merges::show_merges_dialog(siv, context, database, table);
}

fn show_table_mutations(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());

    let context = siv.user_data::<ContextArc>().unwrap().clone();

    super::mutations::show_mutations_dialog(siv, context, database, table);
}

fn show_table_part_log(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());
    let table_uuid = map.get("_uuid").map(|s| s.to_owned());

    let context = siv.user_data::<ContextArc>().unwrap().clone();

    super::part_log::show_part_log_dialog(siv, context, database, table, table_uuid);
}
