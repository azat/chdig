use cursive::{
    event::{AnyCb, Event, EventResult},
    theme::BaseColor,
    utils::markup::StyledString,
    view::{Nameable, Resizable, Selector, View},
    views, Printer, Vec2,
};

pub struct SummaryView {
    layout: views::LinearLayout,
}

// TODO add new information:
// - page cache usage (should be diffed)
impl SummaryView {
    pub fn new() -> Self {
        let layout = views::LinearLayout::vertical()
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Uptime:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("uptime"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "CPU:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("cpu"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Queries:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("queries")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Net recv:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("net_recv"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Net sent:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("net_sent"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Read:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("disk_read"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Write:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("disk_write")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Threads:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("threads"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Pools:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("pools")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Memory:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("mem")),
            );

        return Self { layout };
    }
}

impl View for SummaryView {
    fn draw(&self, printer: &Printer) {
        self.layout.draw(printer);
    }

    fn needs_relayout(&self) -> bool {
        return self.layout.needs_relayout();
    }

    fn layout(&mut self, size: Vec2) {
        self.layout.layout(size);
    }

    fn required_size(&mut self, req: Vec2) -> Vec2 {
        return self.layout.required_size(req);
    }

    fn on_event(&mut self, event: Event) -> EventResult {
        return self.layout.on_event(event);
    }

    fn call_on_any(&mut self, selector: &Selector, callback: AnyCb) {
        self.layout.call_on_any(selector, callback);
    }

    // FIXME: do we need other methods?
}
