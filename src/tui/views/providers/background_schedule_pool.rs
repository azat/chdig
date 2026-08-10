use crate::{
    interpreter::{ContextArc, WorkerEvent, options::ChDigViews},
    tui::{
        App, Dialog, Event, Nameable, Navigation, Resizable, SizeConstraint, ViewProvider,
        actions::ActionDescription,
        fuzzy_actions,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
    },
};
use std::collections::HashMap;

pub struct BackgroundSchedulePoolViewProvider;

impl ViewProvider for BackgroundSchedulePoolViewProvider {
    fn name(&self) -> &'static str {
        "Background Tasks"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::BackgroundSchedulePool
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.focus_name("background_schedule_pool") {
            return;
        }

        let mut columns = vec![
            "pool",
            "database",
            "table",
            "log_name",
            "query_id",
            "elapsed_ms",
            "executing",
            "scheduled",
            "delayed",
        ];

        let (cluster, dbtable, clickhouse, selected_host) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.clickhouse.cluster.is_some(),
                ctx.clickhouse
                    .get_table_name_no_history("background_schedule_pool"),
                ctx.clickhouse.clone(),
                ctx.selected_host.clone(),
            )
        };

        // Only show hostname column when in cluster mode AND no host filter is active
        let columns_to_compare = if cluster && selected_host.is_none() {
            columns.insert(0, "hostName() host");
            vec!["host", "pool", "database", "table", "log_name"]
        } else {
            vec!["pool", "database", "table", "log_name"]
        };

        let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
        let where_clause = if host_filter.is_empty() {
            String::new()
        } else {
            format!("WHERE 1 {}", host_filter)
        };

        let query = format!(
            "SELECT {} FROM {} {} ORDER BY pool, database, table, log_name",
            columns.join(", "),
            dbtable,
            where_clause,
        );

        let mut view = SQLQueryView::new(
            context.clone(),
            "background_schedule_pool",
            "elapsed_ms",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get background_schedule_pool"));

        let background_schedule_pool_action_callback =
            move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                show_background_schedule_pool_actions(app, columns, row);
            };
        view.get_inner_mut()
            .set_on_submit(background_schedule_pool_action_callback);
        view.get_inner_mut().set_title("Background Schedule Pool");

        app.present_view(
            "background_schedule_pool",
            view.with_name("background_schedule_pool").full_screen(),
        );
    }
}

fn show_background_schedule_pool_actions(
    app: &mut App,
    columns: Vec<&'static str>,
    row: QueryResultRow,
) {
    let actions = vec![
        ActionDescription {
            text: "Show tasks logs",
            event: Event::Unknown(vec![]),
        },
        ActionDescription {
            text: "Show tasks",
            event: Event::Unknown(vec![]),
        },
    ];

    let columns_clone = columns.clone();
    let row_clone = row.clone();

    fuzzy_actions(app, actions, move |app, selected| match selected.as_str() {
        "Show tasks logs" => {
            show_tasks_logs(app, columns_clone.clone(), row_clone.clone());
        }
        "Show tasks" => {
            show_tasks_summary(app, columns_clone.clone(), row_clone.clone());
        }
        _ => {}
    });
}

fn show_tasks_logs(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let log_name = map
        .get("log_name")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let database = map
        .get("database")
        .map(|s| s.to_owned())
        .unwrap_or_default();
    let table = map.get("table").map(|s| s.to_owned()).unwrap_or_default();

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.clone().lock().unwrap().options.view.clone();

    context.lock().unwrap().worker.send(
        true,
        WorkerEvent::BackgroundSchedulePoolLogs(
            Some(log_name),
            database,
            table,
            view_options.start,
            view_options.end,
        ),
    );
}

fn show_tasks_summary(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let row_data = row.0;
    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
        let value = r.to_string();
        map.insert(c.to_string(), value);
    });

    let log_name = map.get("log_name").map(|s| s.to_owned());
    let database = map.get("database").map(|s| s.to_owned());
    let table = map.get("table").map(|s| s.to_owned());

    let context = app.user_data::<ContextArc>().unwrap().clone();

    super::background_schedule_pool_log::show_background_schedule_pool_log_dialog(
        app, context, log_name, database, table,
    );
}

pub fn show_background_schedule_pool_dialog(
    app: &mut App,
    context: ContextArc,
    database: Option<String>,
    table: Option<String>,
) {
    let columns = vec![
        "pool",
        "database",
        "table",
        "log_name",
        "query_id",
        "elapsed_ms",
        "executing",
        "scheduled",
        "delayed",
    ];
    let columns_to_compare = vec!["pool", "database", "table", "log_name"];

    let (dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.clickhouse
                .get_table_name_no_history("background_schedule_pool"),
            ctx.clickhouse.clone(),
            ctx.selected_host.clone(),
        )
    };

    let mut where_clauses: Vec<String> = Vec::new();

    if let Some(ref db) = database {
        where_clauses.push(format!("database = '{}'", db.replace('\'', "''")));
    }
    if let Some(ref tbl) = table {
        where_clauses.push(format!("table = '{}'", tbl.replace('\'', "''")));
    }

    let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
    if !host_filter.is_empty() {
        where_clauses.push(format!("1 {}", host_filter));
    }

    let where_clause = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    let query = format!(
        "SELECT {} FROM {} {} ORDER BY pool, database, table, log_name",
        columns.join(", "),
        dbtable,
        where_clause,
    );

    let title = match (&database, &table) {
        (Some(db), Some(tbl)) => format!("Running tasks: {}.{}", db, tbl),
        (Some(db), None) => format!("Running tasks: {}", db),
        (None, Some(tbl)) => format!("Running tasks: table {}", tbl),
        (None, None) => "Running tasks".to_string(),
    };

    let view_name: &'static str = Box::leak(
        format!(
            "background_schedule_pool_{}_{}",
            database.as_deref().unwrap_or("any"),
            table.as_deref().unwrap_or("any")
        )
        .into_boxed_str(),
    );

    let mut sql_view = SQLQueryView::new(
        context.clone(),
        view_name,
        "elapsed_ms",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    let action_callback = move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
        show_background_schedule_pool_actions(app, columns, row);
    };
    sql_view.get_inner_mut().set_on_submit(action_callback);
    sql_view.get_inner_mut().set_title(title.as_str());

    app.add_layer(
        Dialog::around(
            sql_view
                .with_name(view_name)
                .resized(SizeConstraint::AtLeast(140), SizeConstraint::AtLeast(30)),
        )
        .title(title),
    );
}
