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

    fn view_name(&self) -> Option<&'static str> {
        Some("server_logs")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::ServerLogs
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.focus_name("server_logs") {
            return;
        }

        let (selected_host, message_filter, (start, end), limit) = {
            let ctx = context.lock().unwrap();
            (
                ctx.selected_host.clone(),
                ctx.view_filter_seed("server_logs"),
                ctx.view_interval("server_logs"),
                ctx.view_limit_override("server_logs"),
            )
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
                            message_filter,
                            max_level: None,
                            start: DateTime::<Local>::from(start),
                            end,
                            limit,
                        },
                    )
                    .with_name("server_logs")
                    .full_screen(),
                ),
        );
    }
}
