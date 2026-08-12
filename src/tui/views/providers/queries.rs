use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Nameable, Navigation, Resizable, ViewProvider,
        views::queries_view::{QueriesView, Type as ProcessesType},
    },
};

pub struct ProcessesViewProvider;

impl ViewProvider for ProcessesViewProvider {
    fn name(&self) -> &'static str {
        "Processes"
    }

    fn view_name(&self) -> Option<&'static str> {
        Some("processes")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Queries
    }

    fn show(&self, app: &mut App, context: ContextArc, instance: Option<&str>) {
        show_queries_view(
            app,
            context,
            ProcessesType::ProcessList,
            instance.unwrap_or("processes"),
            instance.unwrap_or("Queries"),
        );
    }
}

/// The instance name doubles as the pane title, to tell the panes apart.
fn show_queries_view(
    app: &mut App,
    context: ContextArc,
    processes_type: ProcessesType,
    name: &str,
    title: &str,
) {
    if app.focus_name(name) {
        return;
    }

    app.present_view(
        name,
        QueriesView::new(context, processes_type, name, title)
            .with_name(name)
            .full_screen(),
    );
}

pub struct SlowQueryLogViewProvider;

impl ViewProvider for SlowQueryLogViewProvider {
    fn name(&self) -> &'static str {
        "Slow queries"
    }

    fn view_name(&self) -> Option<&'static str> {
        Some("slow_query_log")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::SlowQueries
    }

    fn show(&self, app: &mut App, context: ContextArc, instance: Option<&str>) {
        show_queries_view(
            app,
            context,
            ProcessesType::SlowQueryLog,
            instance.unwrap_or("slow_query_log"),
            instance.unwrap_or("Slow queries"),
        );
    }
}

pub struct LastQueryLogViewProvider;

impl ViewProvider for LastQueryLogViewProvider {
    fn name(&self) -> &'static str {
        "Last queries"
    }

    fn view_name(&self) -> Option<&'static str> {
        Some("last_query_log")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::LastQueries
    }

    fn show(&self, app: &mut App, context: ContextArc, instance: Option<&str>) {
        show_queries_view(
            app,
            context,
            ProcessesType::LastQueryLog,
            instance.unwrap_or("last_query_log"),
            instance.unwrap_or("Last queries"),
        );
    }
}
