use cursive::{
    event::{AnyCb, Event, EventResult},
    view::{Nameable, Selector, View},
    views, Printer, Vec2,
};

pub struct SummaryView {
    layout: views::LinearLayout,
}

impl SummaryView {
    pub fn new() -> Self {
        let layout = views::LinearLayout::horizontal()
            .child(views::Dialog::around(views::TextView::new("").with_name("mem")).title("MEM:"))
            .child(views::Dialog::around(views::TextView::new("").with_name("cpu")).title("CPU:"))
            .child(views::Dialog::around(views::TextView::new("").with_name("net")).title("NET:"))
            .child(views::Dialog::around(views::TextView::new("").with_name("disk")).title("DSK:"));

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
