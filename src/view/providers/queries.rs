use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ProcessesType, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
};

pub struct ProcessesViewProvider;

impl ViewProvider for ProcessesViewProvider {
    fn name(&self) -> &'static str {
        "Processes"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Queries
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("processes") {
            return;
        }

        siv.drop_main_view();
        siv.set_main_view(
            view::QueriesView::new(
                context.clone(),
                ProcessesType::ProcessList,
                "processes",
                "Queries",
            )
            .with_name("processes")
            .full_screen(),
        );
        siv.focus_name("processes").unwrap();
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

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("slow_query_log") {
            return;
        }

        siv.drop_main_view();
        siv.set_main_view(
            view::QueriesView::new(
                context.clone(),
                ProcessesType::SlowQueryLog,
                "slow_query_log",
                "Slow queries",
            )
            .with_name("slow_query_log")
            .full_screen(),
        );
        siv.focus_name("slow_query_log").unwrap();
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

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("last_query_log") {
            return;
        }

        siv.drop_main_view();
        siv.set_main_view(
            view::QueriesView::new(
                context.clone(),
                ProcessesType::LastQueryLog,
                "last_query_log",
                "Last queries",
            )
            .with_name("last_query_log")
            .full_screen(),
        );
        siv.focus_name("last_query_log").unwrap();
    }
}
