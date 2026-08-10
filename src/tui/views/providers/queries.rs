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

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Queries
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.focus_name("processes") {
            return;
        }

        app.present_view(
            "processes",
            QueriesView::new(
                context.clone(),
                ProcessesType::ProcessList,
                "processes",
                "Queries",
            )
            .with_name("processes")
            .full_screen(),
        );
    }
}

pub struct SlowQueryLogViewProvider;

impl ViewProvider for SlowQueryLogViewProvider {
    fn name(&self) -> &'static str {
        "Slow queries"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::SlowQueries
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.focus_name("slow_query_log") {
            return;
        }

        app.present_view(
            "slow_query_log",
            QueriesView::new(
                context.clone(),
                ProcessesType::SlowQueryLog,
                "slow_query_log",
                "Slow queries",
            )
            .with_name("slow_query_log")
            .full_screen(),
        );
    }
}

pub struct LastQueryLogViewProvider;

impl ViewProvider for LastQueryLogViewProvider {
    fn name(&self) -> &'static str {
        "Last queries"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::LastQueries
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.focus_name("last_query_log") {
            return;
        }

        app.present_view(
            "last_query_log",
            QueriesView::new(
                context.clone(),
                ProcessesType::LastQueryLog,
                "last_query_log",
                "Last queries",
            )
            .with_name("last_query_log")
            .full_screen(),
        );
    }
}
