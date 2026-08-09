//! Retained-component TUI framework over ratatui.
//!
//! Components own their state and form a tree searchable by name, so
//! background workers can update views through `App::call_on_name` callbacks
//! delivered over the `UiSink` channel. Rendering is immediate-mode ratatui;
//! events are dispatched down the focused path, unconsumed events fall back
//! to global callbacks.

pub mod actions;
pub mod app;
pub mod checkbox;
pub mod component;
pub mod dialog;
pub mod edit;
pub mod event;
pub mod fuzzy;
pub mod highlight;
pub mod linear;
pub mod logger;
pub mod mux;
pub mod navigation;
pub mod panel;
pub mod prompt;
pub mod provider;
pub mod resize;
pub mod scroll;
pub mod select;
pub mod style;
pub mod text;
pub mod views;

pub use app::{App, TerminalGuard, UiCallback, UiSink};
pub use component::{
    Boxed, Canvas, Component, DummyView, Nameable, NamedView, OnEventView, call_on_any,
    call_on_name,
};
pub use dialog::Dialog;
pub use edit::EditView;
pub use event::{Callback, Event, EventResult, Key, MouseButton, MouseEvent};
pub use fuzzy::{fuzzy_actions, fuzzy_select_strings};
pub use highlight::highlight_sql;
pub use linear::LinearLayout;
pub use mux::Mux;
pub use navigation::Navigation;
pub use panel::Panel;
pub use prompt::{show_bottom_prompt, submit_on_enter};
pub use provider::{ViewProvider, ViewRegistry};
pub use resize::{Resizable, ResizedView, SizeConstraint};
pub use scroll::{ScrollView, Scrollable};
pub use select::SelectView;
pub use style::StyledString;
pub use text::TextView;
