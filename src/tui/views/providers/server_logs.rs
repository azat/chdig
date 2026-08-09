use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, DummyView, LinearLayout, Nameable, Navigation, Resizable, TextView, ViewProvider,
        views::text_log_view::TextLogView,
    },
};
use chrono::{DateTime, Local};

pub struct ServerLogsViewProvider;

impl ViewProvider for ServerLogsViewProvider {
    fn name(&self) -> &'static str {
        "Server logs"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::ServerLogs
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.has_view("server_logs") {
            return;
        }

        let (view_options, selected_host) = {
            let ctx = context.lock().unwrap();
            (ctx.options.view.clone(), ctx.selected_host.clone())
        };

        app.present_view(
            "server_logs",
            LinearLayout::vertical()
                .child(TextView::new("Server logs:").center())
                .child(DummyView.fixed_height(1))
                .child(
                    TextLogView::new(
                        "server_logs",
                        context,
                        crate::interpreter::TextLogArguments {
                            query_ids: None,
                            logger_names: None,
                            hostname: selected_host,
                            message_filter: None,
                            max_level: None,
                            start: DateTime::<Local>::from(view_options.start),
                            end: view_options.end,
                        },
                    )
                    .with_name("server_logs")
                    .full_screen(),
                ),
        );
    }
}
