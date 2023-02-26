use crate::{interpreter::ContextArc, view};
use cursive::{
    view::{Nameable, Resizable},
    views::{Dialog, LinearLayout},
    Cursive,
};

pub trait Navigation {
    fn show_clickhouse_processes(&mut self, context: ContextArc);
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
}
