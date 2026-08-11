use crate::{
    interpreter::{ContextArc, WorkerEvent, clickhouse::TraceType, options::ChDigViews},
    tui::{self, App, Component, Nameable, Navigation, TextView, ViewProvider},
};
use ratatui::layout::{Rect, Size};

/// Focusable placeholder shown until the flamegraph arrives: it is named
/// "flamelens", so show_flamelens() renders the result into this pane in
/// place (instead of a fullscreen takeover or splitting another pane).
struct FlamegraphStub {
    inner: TextView,
}

impl FlamegraphStub {
    fn new() -> Self {
        Self {
            inner: TextView::new("Loading server CPU flamegraph ...").center(),
        }
    }
}

impl Component for FlamegraphStub {
    fn draw(&mut self, canvas: &mut tui::Canvas<'_>, area: Rect, focused: bool) {
        self.inner.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        max
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}

pub struct CpuFlamegraphViewProvider;

impl ViewProvider for CpuFlamegraphViewProvider {
    fn name(&self) -> &'static str {
        "CPU Flamegraph"
    }

    fn view_name(&self) -> Option<&'static str> {
        // The name FlamelensView is shown under, wherever it comes from
        // (this provider or the flamegraph global actions).
        Some("flamelens")
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::CpuFlamegraph
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.focus_name("flamelens") {
            return;
        }

        app.present_view("flamelens", FlamegraphStub::new().with_name("flamelens"));

        let mut ctx = context.lock().unwrap();
        let (start, end) = ctx.view_interval("flamelens");
        ctx.worker.send(
            true,
            WorkerEvent::ServerFlameGraph(true, TraceType::CPU, start, end),
        );
    }
}
