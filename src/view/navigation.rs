use crate::{interpreter::ContextArc, view};
use cursive::{
    view::{Nameable, Resizable},
    views::{Dialog, LinearLayout},
    Cursive,
};

pub trait Navigation {
    fn show_clickhouse_processes(&mut self, context: ContextArc);
    fn show_clickhouse_merges(&mut self, context: ContextArc);
}

impl Navigation for Cursive {
    fn show_clickhouse_processes(&mut self, context: ContextArc) {
        if self.find_name::<view::ProcessesView>("processes").is_some() {
            return;
        }

        while !self.screen_mut().is_empty() {
            self.pop_layer();
        }

        self.add_fullscreen_layer(
            LinearLayout::vertical()
                // TODO: show summary for all views
                .child(view::SummaryView::new().with_name("summary"))
                .child(
                    Dialog::around(
                        view::ProcessesView::new(context.clone())
                            .expect("Cannot get processlist")
                            .with_name("processes")
                            .min_size((500, 200)),
                    )
                    .title(format!(
                        "processlist ({})",
                        context.lock().unwrap().server_version
                    )),
                ),
        );
    }

    fn show_clickhouse_merges(&mut self, context: ContextArc) {
        if self.find_name::<view::MergesView>("merges").is_some() {
            return;
        }

        while !self.screen_mut().is_empty() {
            self.pop_layer();
        }

        self.add_fullscreen_layer(
            Dialog::around(
                view::MergesView::new(context.clone())
                    .expect("Cannot get merges")
                    .with_name("merges")
                    .min_size((500, 200)),
            )
            .title(format!(
                "merges ({})",
                context.lock().unwrap().server_version
            )),
        );
    }
}
