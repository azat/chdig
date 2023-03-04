use crate::{interpreter::ContextArc, view};
use cursive::{
    theme::Style,
    utils::span::SpannedString,
    view::View as _,
    view::{Nameable, Resizable},
    views::{
        Dialog, DummyView, FixedLayout, Layer, LinearLayout, OnLayoutView, TextContent, TextView,
    },
    Cursive, {Rect, Vec2},
};

pub trait Navigation {
    fn statusbar(&mut self, main_content: impl Into<SpannedString<Style>>);
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>);

    fn show_clickhouse_processes(&mut self, context: ContextArc);
    fn show_clickhouse_merges(&mut self, context: ContextArc);
    fn show_clickhouse_replication_queue(&mut self, context: ContextArc);
    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc);
}

impl Navigation for Cursive {
    fn statusbar(&mut self, main_content: impl Into<SpannedString<Style>>) {
        // NOTE: This is a copy-paste from cursive examples
        let main_text_content = TextContent::new(main_content);
        self.screen_mut().add_transparent_layer(
            OnLayoutView::new(
                FixedLayout::new().child(
                    Rect::from_point(Vec2::zero()),
                    Layer::new(
                        LinearLayout::horizontal()
                            .child(
                                TextView::new_with_content(main_text_content.clone())
                                    .with_name("main_status"),
                            )
                            .child(DummyView.fixed_width(1))
                            .child(TextView::new("").with_name("status")),
                    )
                    .full_width(),
                ),
                |layout, size| {
                    layout.set_child_position(0, Rect::from_size((0, size.y - 1), (size.x, 1)));
                    layout.layout(size);
                },
            )
            .full_screen(),
        );
    }
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>) {
        self.call_on_name("status", |text_view: &mut TextView| {
            text_view.set_content(content);
        })
        .expect("set_status")
    }

    fn show_clickhouse_processes(&mut self, context: ContextArc) {
        if self.find_name::<view::ProcessesView>("processes").is_some() {
            return;
        }

        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        self.add_layer(
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
                    .title("Queries"),
                ),
        );
    }

    fn show_clickhouse_merges(&mut self, context: ContextArc) {
        if self.find_name::<view::MergesView>("merges").is_some() {
            return;
        }

        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        self.add_layer(
            Dialog::around(
                view::MergesView::new(context.clone())
                    .expect("Cannot get merges")
                    .with_name("merges")
                    .min_size((500, 200)),
            )
            .title("Merges"),
        );
    }

    fn show_clickhouse_replication_queue(&mut self, context: ContextArc) {
        if self
            .find_name::<view::ReplicationQueueView>("replication_queue")
            .is_some()
        {
            return;
        }

        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        self.add_layer(
            Dialog::around(
                view::ReplicationQueueView::new(context.clone())
                    .expect("Cannot get replication_queue")
                    .with_name("replication_queue")
                    .min_size((500, 200)),
            )
            .title("Replication queue"),
        );
    }

    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc) {
        if self
            .find_name::<view::ReplicatedFetchesView>("replicated_fetches")
            .is_some()
        {
            return;
        }

        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        self.add_layer(
            Dialog::around(
                view::ReplicatedFetchesView::new(context.clone())
                    .expect("Cannot get replicated_fetches")
                    .with_name("replicated_fetches")
                    .min_size((500, 200)),
            )
            .title("Fetches"),
        );
    }
}
