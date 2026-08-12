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

    fn show(&self, app: &mut App, context: ContextArc, instance: Option<&str>) {
        let name = instance.unwrap_or("server_logs");
        if app.focus_name(name) {
            return;
        }

        let (selected_host, message_filter, (start, end), limit, max_level) = {
            let ctx = context.lock().unwrap();
            (
                ctx.selected_host.clone(),
                ctx.view_filter_seed(name),
                ctx.view_interval(name),
                ctx.view_limit_override(name),
                ctx.view_level(name).map(|level| level.as_str().to_string()),
            )
        };

        app.present_view(
            name,
            LinearLayout::vertical()
                .child(TextView::new(format!("{}:", instance.unwrap_or("Server logs"))).center())
                .child(DummyView.fixed_height(1))
                .child(
                    TextLogView::new(
                        name,
                        context,
                        crate::interpreter::TextLogArguments {
                            query_ids_subquery: None,
                            query_ids: None,
                            logger_names: None,
                            hostname: selected_host,
                            message_filter,
                            max_level,
                            start: DateTime::<Local>::from(start),
                            end,
                            limit,
                        },
                    )
                    .with_name(name)
                    .full_screen(),
                ),
        );
    }
}
