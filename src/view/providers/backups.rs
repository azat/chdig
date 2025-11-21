use crate::{
    common::RelativeDateTime,
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, TextLogView, ViewProvider},
};
use cursive::{
    Cursive,
    view::Resizable,
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
};
use std::collections::HashMap;

pub struct BackupsViewProvider;

impl ViewProvider for BackupsViewProvider {
    fn name(&self) -> &'static str {
        "Backups"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Backups
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let mut columns = vec![
            "name",
            "status::String status",
            "error",
            "start_time",
            "end_time",
            "total_size",
            "query_id _query_id",
        ];

        let backups_logs_callback =
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
                            "backups_logs",
                            TextLogView::new(
                                "backups_logs",
                                context,
                                map["start_time"].as_datetime().unwrap(),
                                RelativeDateTime::from(map["end_time"].as_datetime()),
                                Some(vec![map["_query_id"].to_string()]),
                                None,
                                None,
                                None,
                            ),
                        )),
                ));
                siv.focus_name("backups_logs").unwrap();
            };

        // TODO:
        // - order by elapsed time
        super::show_query_result_view(
            siv,
            context,
            "backups",
            None,
            None,
            "total_size",
            &mut columns,
            1,
            Some(backups_logs_callback),
            &HashMap::new(),
        );
    }
}
