use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, TextLogView, ViewProvider},
};
use cursive::{
    Cursive,
    view::Resizable,
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
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

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let columns = vec![
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
            "tables.uuid::String _table_uuid",
        ];

        let merges_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                let mut map = HashMap::new();
                columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r);
                });

                let context = siv.user_data::<ContextArc>().unwrap().clone();
                siv.add_layer(Dialog::around(
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
                                    message_filter: None,
                                    max_level: None,
                                    start: map["_create_time"].as_datetime().unwrap(),
                                    end: crate::common::RelativeDateTime::new(None),
                                },
                            ),
                        )),
                ));
                siv.focus_name("merge_logs").unwrap();
            };

        let tables_dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "tables");
        super::render_from_clickhouse_query(
            siv,
            super::RenderFromClickHouseQueryArguments {
                context,
                table: "merges",
                join: Some(format!(
                    "left join (select distinct on (database, name) database, name, uuid from {}) tables on merges.database = tables.database and merges.table = tables.name",
                    tables_dbtable
                )),
                filter: None,
                sort_by: "elapsed",
                columns,
                columns_to_compare: 3,
                on_submit: Some(merges_logs_callback),
                settings: HashMap::new(),
            },
        );
    }
}
