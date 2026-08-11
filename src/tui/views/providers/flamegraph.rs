use crate::{
    interpreter::{ContextArc, WorkerEvent, clickhouse::TraceType, options::ChDigViews},
    tui::{self, App, Component, Nameable, Navigation, TextView, ViewProvider},
};
use ratatui::layout::{Rect, Size};

/// Focusable placeholder shown until the flamegraph arrives: it carries the
/// view's slot name, so show_flamelens() renders the result into this pane in
/// place (instead of a fullscreen takeover or splitting another pane).
struct FlamegraphStub {
    inner: TextView,
}

impl FlamegraphStub {
    fn new(what: &str) -> Self {
        Self {
            inner: TextView::new(format!("Loading {} ...", what)).center(),
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

#[derive(Clone)]
enum Source {
    Trace(TraceType),
    Live,
    Jemalloc,
}

pub struct FlamegraphViewProvider {
    view: ChDigViews,
    menu_name: &'static str,
    source: Source,
}

/// Every server flamegraph flavor as a view (placeable in the layout). Each
/// gets its own pane slot (the stable view name), so several can be shown
/// side by side; ad-hoc flamegraph actions use the separate "flamelens" slot.
pub const PROVIDERS: &[FlamegraphViewProvider] = &[
    FlamegraphViewProvider {
        view: ChDigViews::CpuFlamegraph,
        menu_name: "CPU Flamegraph",
        source: Source::Trace(TraceType::CPU),
    },
    FlamegraphViewProvider {
        view: ChDigViews::RealFlamegraph,
        menu_name: "Real Flamegraph",
        source: Source::Trace(TraceType::Real),
    },
    FlamegraphViewProvider {
        view: ChDigViews::MemoryFlamegraph,
        menu_name: "Memory Flamegraph",
        source: Source::Trace(TraceType::Memory),
    },
    FlamegraphViewProvider {
        view: ChDigViews::MemorySampleFlamegraph,
        menu_name: "Memory Sample Flamegraph",
        source: Source::Trace(TraceType::MemorySample),
    },
    FlamegraphViewProvider {
        view: ChDigViews::JemallocSampleFlamegraph,
        menu_name: "Jemalloc Sample Flamegraph",
        source: Source::Trace(TraceType::JemallocSample),
    },
    FlamegraphViewProvider {
        view: ChDigViews::MemoryAllocatedWithoutCheckFlamegraph,
        menu_name: "MemoryAllocatedWithoutCheck Flamegraph",
        source: Source::Trace(TraceType::MemoryAllocatedWithoutCheck),
    },
    FlamegraphViewProvider {
        view: ChDigViews::EventsFlamegraph,
        menu_name: "Events Flamegraph",
        source: Source::Trace(TraceType::ProfileEvent),
    },
    FlamegraphViewProvider {
        view: ChDigViews::LiveFlamegraph,
        menu_name: "Live Flamegraph",
        source: Source::Live,
    },
    FlamegraphViewProvider {
        view: ChDigViews::JemallocFlamegraph,
        menu_name: "Jemalloc Flamegraph",
        source: Source::Jemalloc,
    },
];

impl ViewProvider for &'static FlamegraphViewProvider {
    fn name(&self) -> &'static str {
        self.menu_name
    }

    fn view_type(&self) -> ChDigViews {
        self.view
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        let slot = self.view.config_name();
        if app.focus_name(slot) {
            return;
        }

        app.present_view(slot, FlamegraphStub::new(self.menu_name).with_name(slot));

        let mut ctx = context.lock().unwrap();
        let event = match &self.source {
            Source::Trace(trace_type) => {
                let (start, end) = ctx.view_interval(slot);
                WorkerEvent::ServerFlameGraph(true, trace_type.clone(), start, end, Some(slot))
            }
            Source::Live => WorkerEvent::LiveQueryFlameGraph(true, None, Some(slot)),
            Source::Jemalloc => WorkerEvent::JemallocFlameGraph(true, Some(slot)),
        };
        ctx.worker.send(true, event);
    }
}
