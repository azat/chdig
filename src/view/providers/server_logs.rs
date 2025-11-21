use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{Navigation, TextLogView, ViewProvider},
};
use chrono::{DateTime, Local};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{DummyView, LinearLayout, TextView},
};

pub struct ServerLogsViewProvider;

impl ViewProvider for ServerLogsViewProvider {
    fn name(&self) -> &'static str {
        "Server logs"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::ServerLogs
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("server_logs") {
            return;
        }

        let view_options = context.clone().lock().unwrap().options.view.clone();

        siv.drop_main_view();
        siv.set_main_view(
            LinearLayout::vertical()
                .child(TextView::new("Server logs:").center())
                .child(DummyView.fixed_height(1))
                .child(
                    TextLogView::new(
                        "server_logs",
                        context,
                        DateTime::<Local>::from(view_options.start),
                        view_options.end,
                        None,
                        None,
                        None,
                        None,
                    )
                    .with_name("server_logs")
                    .full_screen(),
                ),
        );
        siv.focus_name("server_logs").unwrap();
    }
}
