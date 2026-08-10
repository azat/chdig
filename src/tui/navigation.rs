use crate::common::parse_datetime_or_date;
use crate::interpreter::{
    ContextArc, WorkerEvent,
    clickhouse::TraceType,
    options::{ChDigViews, FlamelensPane},
};
use crate::tui::{
    self, App, Component, Dialog, DummyView, EditView, Event, EventResult, Key, LinearLayout,
    Nameable, NamedView, OnEventView, Resizable, SelectView, TextView,
    component::call_on_any,
    mux::Mux,
    style::{Color, Modifier, Style, StyledString},
    views::flamelens_view::FlamelensView,
};
use anyhow::Result;
use chrono::{DateTime, Local};
use ratatui::layout::{Rect, Size};
use std::collections::HashSet;

/// Placeholder content of a freshly split pane. It must be focusable so that
/// the view selected in the views menu replaces this pane (present_view
/// targets the focused pane).
struct PaneStub {
    inner: TextView,
}

impl PaneStub {
    fn new() -> Self {
        Self {
            inner: TextView::new("Press F2 to choose a view").center(),
        }
    }
}

impl Component for PaneStub {
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

/// Whether the focused pane subtree contains a view named `name`.
fn focused_pane_contains(app: &mut App, name: &str) -> bool {
    app.call_on_name("panes", |mux: &mut Mux| {
        let mut found = false;
        if let Some(v) = mux.active_view_mut() {
            call_on_any(v, name, &mut |_| found = true);
        }
        found
    })
    .unwrap_or(false)
}

/// Owners of view actions that live in the focused pane. Actions of views in
/// other panes are hidden until those panes are focused (their key bindings
/// only fire there anyway, since events follow the focus path).
fn focused_action_owners(app: &mut App) -> HashSet<&'static str> {
    let context = app.user_data::<ContextArc>().unwrap().clone();
    let owners: HashSet<&'static str> = {
        let ctx = context.lock().unwrap();
        ctx.view_actions.iter().map(|a| a.owner).collect()
    };
    owners
        .into_iter()
        .filter(|o| focused_pane_contains(app, o))
        .collect()
}

fn toggle_debug_metrics(app: &mut App) {
    let ctx = app.user_data::<ContextArc>().unwrap().clone();
    let metrics = ctx.lock().unwrap().debug_metrics.clone();
    let shown = metrics.toggle_shown();
    // Paint immediately on both transitions so the user sees the toggle take effect
    // without waiting for the next refresh tick (and so stale numbers don't linger on hide).
    if shown {
        app.set_statusbar_debug(metrics.snapshot().to_string());
    } else {
        app.set_statusbar_debug("");
    }
}

/// Left-menu select list with vim-style j/k navigation.
fn menu_select(select: SelectView) -> NamedView<OnEventView<SelectView>> {
    OnEventView::new(select)
        .on_pre_event_inner('k', |s: &mut SelectView, _| {
            s.select_up(1);
            Some(EventResult::consumed())
        })
        .on_pre_event_inner('j', |s: &mut SelectView, _| {
            s.select_down(1);
            Some(EventResult::consumed())
        })
        .with_name("actions_select")
}

fn make_menu_text() -> StyledString {
    let mut text = StyledString::new();
    let highlight = tui::style::highlight();

    text.append_plain("F1");
    text.append_styled("Help", highlight);
    text.append_plain("F2");
    text.append_styled("Views", highlight);
    text.append_plain("F3");
    text.append_styled("Settings", highlight);
    text.append_plain("F8");
    text.append_styled("Actions", highlight);

    text
}

pub trait Navigation {
    /// Closes the left menu or the top layer. Returns false if there was
    /// nothing to close (i.e. only the main view is shown).
    fn pop_ui(&mut self) -> bool;
    /// Adds a new pane next to (or below) the focused one and opens the views
    /// menu to fill it.
    fn split_pane(&mut self, below: bool);
    /// Closes the focused pane. Returns false if it is the only one.
    fn close_pane(&mut self) -> bool;
    fn toggle_pause_updates(&mut self, reason: Option<&str>);
    fn refresh_view(&mut self);
    fn refresh_all(&mut self);
    fn seek_time_frame(&mut self, is_sub: bool);
    fn select_time_frame(&mut self);

    fn initialize_global_shortcuts(&mut self, context: ContextArc);
    fn initialize_views_menu(&mut self, context: ContextArc);
    fn chdig(&mut self, context: ContextArc);

    fn show_help_dialog(&mut self);
    fn show_settings_dialog(&mut self);
    fn show_views(&mut self);
    fn show_actions(&mut self);
    fn show_fuzzy_actions(&mut self);
    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>);
    fn show_jemalloc_flamegraph(&mut self, tui: bool);
    /// Renders a flamegraph in the TUI: fullscreen flamelens takeover, or a
    /// pane below/above the focused one (view.flamelens_pane).
    fn show_flamelens(&mut self, fl: flamelens::app::App);
    fn show_server_perfetto(&mut self);
    fn show_connection_dialog(&mut self);

    fn show_previous_view(&mut self);

    fn drop_main_view(&mut self);
    /// Replaces the focused pane content with `view` and focuses `focus` in it.
    fn present_view<V: Component + 'static>(&mut self, focus: &str, view: V);
    /// Shows a log view in a new pane to the right (default) or in a dialog
    /// (--logs-in-dialog). `view` must contain a view named `view_name`.
    fn present_logs<V: Component + 'static>(
        &mut self,
        view_name: &'static str,
        title: &str,
        view: V,
    );

    fn set_statusbar_version(&mut self, main_content: impl Into<StyledString>);
    fn set_statusbar_content(&mut self, content: impl Into<StyledString>);
    fn set_statusbar_connection(&mut self, content: impl Into<StyledString>);
    fn set_statusbar_debug(&mut self, content: impl Into<StyledString>);

    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: Component,
        F: FnOnce(&mut V) -> Result<()>;
}

impl Navigation for App {
    fn pop_ui(&mut self) -> bool {
        // Close left menu
        let mut has_left_menu = false;
        self.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
            if !left_menu_view.is_empty() {
                left_menu_view
                    .remove_child(left_menu_view.len() - 1)
                    .expect("No child view to remove");
                has_left_menu = true;
            }
        });
        // Once at a time
        if has_left_menu {
            self.focus_name("main");
            return true;
        }

        if self.screen_len() == 1 {
            return false;
        }

        self.pop_layer();
        true
    }

    fn show_previous_view(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let provider = {
            let mut ctx = context.lock().unwrap();
            let Some(previous_view) = ctx.view_history.pop() else {
                return;
            };
            // NOTE: not set_current_view(), otherwise Backspace will cycle
            // between the last two views instead of going back.
            ctx.current_view = Some(previous_view);
            ctx.view_registry.get_by_view_type(previous_view)
        };
        provider.show(self, context);
    }

    fn toggle_pause_updates(&mut self, reason: Option<&str>) {
        let is_paused;
        {
            let context = self.user_data::<ContextArc>().unwrap().clone();
            let mut context = context.lock().unwrap();
            context.worker.toggle_pause();
            is_paused = context.worker.is_paused();
        }

        let reason = reason.map(str::to_string);
        self.call_on_name("is_paused", |v: &mut TextView| {
            let mut text = StyledString::new();
            if is_paused {
                let bold = Style::default().add_modifier(Modifier::BOLD);
                text.append_styled(" PAUSED", bold);
                if let Some(reason) = reason {
                    text.append_styled(format!(" ({})", reason), bold);
                }
                text.append_styled(
                    " press P to resume",
                    Style::default().add_modifier(Modifier::ITALIC),
                );
            }
            v.set_content(text);
        });
    }

    fn refresh_view(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let context = context.lock().unwrap();
        log::trace!("Toggle refresh");
        context.trigger_view_refresh();
    }

    fn refresh_all(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let context = context.lock().unwrap();
        log::trace!("Toggle full refresh");
        context.trigger_full_refresh();
    }

    fn seek_time_frame(&mut self, is_sub: bool) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let mut context = context.lock().unwrap();
        context.shift_time_interval(is_sub, 10);
        context.trigger_view_refresh();
    }

    fn select_time_frame(&mut self) {
        let on_submit = move |app: &mut App| {
            let start = app
                .call_on_name("start", |view: &mut EditView| view.get_content())
                .unwrap();
            let end = app
                .call_on_name("end", |view: &mut EditView| view.get_content())
                .unwrap();

            app.pop_layer();

            let new_begin = match parse_datetime_or_date(&start) {
                Ok(new) => new,
                Err(err) => {
                    app.add_layer(Dialog::info(err));
                    return;
                }
            };
            let new_end = match parse_datetime_or_date(&end) {
                Ok(new) => new,
                Err(err) => {
                    app.add_layer(Dialog::info(err));
                    return;
                }
            };
            log::debug!("Set time frame to ({}, {})", new_begin, new_end);
            let context = app.user_data::<ContextArc>().unwrap().clone();
            let mut context = context.lock().unwrap();
            context.options.view.start = new_begin.into();
            context.options.view.end = new_end.into();
            context.trigger_view_refresh();
        };

        let view = Dialog::new()
            .title("Set the time interval")
            .content(tui::submit_on_enter(
                LinearLayout::vertical()
                    .child(TextView::new(
                        "format: YYYY-MM-DDTHH:MM:SS[.ssssss][±hh:mm|Z]",
                    ))
                    .child(DummyView)
                    .child(TextView::new("start:"))
                    .child(EditView::new().with_name("start"))
                    .child(DummyView)
                    .child(TextView::new("end:"))
                    .child(EditView::new().with_name("end")),
                on_submit,
            ))
            .button("Submit", on_submit);
        self.add_layer(view);
    }

    fn chdig(&mut self, context: ContextArc) {
        self.set_user_data(context.clone());
        self.initialize_global_shortcuts(context.clone());
        self.initialize_views_menu(context.clone());

        self.add_fullscreen_layer(
            LinearLayout::horizontal()
                .child(LinearLayout::vertical().with_name("left_menu"))
                .child(
                    LinearLayout::vertical()
                        .child(
                            LinearLayout::horizontal()
                                .child(TextView::new(make_menu_text()).no_wrap())
                                .child(TextView::empty().no_wrap().with_name("is_paused"))
                                // Align status to the right
                                .child(DummyView.full_width())
                                // Empty until `!` toggles it — no visual cost when hidden.
                                .child(TextView::empty().no_wrap().with_name("debug_status"))
                                .child(TextView::empty().no_wrap().with_name("status"))
                                .child(DummyView.fixed_width(1))
                                .child(TextView::empty().no_wrap().with_name("connection"))
                                .child(DummyView.fixed_width(1))
                                .child(TextView::empty().no_wrap().with_name("version"))
                                .fixed_height(1),
                        )
                        .child(
                            crate::tui::views::summary_view::SummaryView::new(context.clone())
                                .with_name("summary"),
                        )
                        .child(Mux::new().with_name("panes"))
                        .with_name("main"),
                ),
        );

        {
            let ctx = context.lock().unwrap();
            self.set_statusbar_version(ctx.server_version.clone());
            self.set_statusbar_connection(ctx.options.clickhouse.connection_info());
        }

        let provider = {
            let mut ctx = context.lock().unwrap();
            let start_view = ctx.options.start_view().unwrap_or(ChDigViews::Queries);
            ctx.set_current_view(start_view);
            ctx.view_registry.get_by_view_type(start_view)
        };
        provider.show(self, context.clone());
    }

    /// Ignore rustfmt max_width, otherwise callback actions looks ugly
    #[rustfmt::skip]
    fn initialize_global_shortcuts(&mut self, context: ContextArc) {
        let mut context = context.lock().unwrap();

        context.add_global_action(self, "Show help", Key::F1, |app| app.show_help_dialog());
        context.add_global_action(self, "Settings", Key::F3, |app| app.show_settings_dialog());

        context.add_global_action(self, "Views", Key::F2, |app| app.show_views());
        context.add_global_action(self, "Show actions", Key::F8, |app| app.show_actions());
        context.add_global_action(self, "Fuzzy actions", Event::CtrlChar('p'), |app| app.show_fuzzy_actions());

        if context.options.clickhouse.cluster.is_some() {
            context.add_global_action(self, "Filter by host", Event::CtrlChar('h'), |app| app.show_connection_dialog());
        }

        context.add_global_action(self, "Server CPU Flamegraph", 'F', |app| app.show_server_flamegraph(true, Some(TraceType::CPU)));
        context.add_global_action_without_shortcut(self, "Server Real Flamegraph", |app| app.show_server_flamegraph(true, Some(TraceType::Real)));
        context.add_global_action_without_shortcut(self, "Server Memory Flamegraph", |app| app.show_server_flamegraph(true, Some(TraceType::Memory)));
        context.add_global_action_without_shortcut(self, "Server Memory Sample Flamegraph", |app| app.show_server_flamegraph(true, Some(TraceType::MemorySample)));
        context.add_global_action_without_shortcut(self, "Server Jemalloc Sample Flamegraph", |app| app.show_server_flamegraph(true, Some(TraceType::JemallocSample)));
        context.add_global_action_without_shortcut(self, "Server MemoryAllocatedWithoutCheck Flamegraph", |app| app.show_server_flamegraph(true, Some(TraceType::MemoryAllocatedWithoutCheck)));
        context.add_global_action_without_shortcut(self, "Server Events Flamegraph", |app| app.show_server_flamegraph(true, Some(TraceType::ProfileEvent)));
        context.add_global_action_without_shortcut(self, "Server Live Flamegraph", |app| app.show_server_flamegraph(true, None));
        context.add_global_action_without_shortcut(self, "Share Server CPU Flamegraph", |app| app.show_server_flamegraph(false, Some(TraceType::CPU)));
        context.add_global_action_without_shortcut(self, "Share Server Real Flamegraph", |app| app.show_server_flamegraph(false, Some(TraceType::Real)));
        context.add_global_action_without_shortcut(self, "Share Server Memory Flamegraph", |app| app.show_server_flamegraph(false, Some(TraceType::Memory)));
        context.add_global_action_without_shortcut(self, "Share Server Memory Sample Flamegraph", |app| app.show_server_flamegraph(false, Some(TraceType::MemorySample)));
        context.add_global_action_without_shortcut(self, "Share Server MemoryAllocatedWithoutCheck Flamegraph", |app| app.show_server_flamegraph(false, Some(TraceType::MemoryAllocatedWithoutCheck)));
        context.add_global_action_without_shortcut(self, "Share Server Events Flamegraph", |app| app.show_server_flamegraph(false, Some(TraceType::ProfileEvent)));
        context.add_global_action_without_shortcut(self, "Share Server Live Flamegraph", |app| app.show_server_flamegraph(false, None));
        context.add_global_action_without_shortcut(self, "Jemalloc", |app| app.show_jemalloc_flamegraph(true));
        context.add_global_action_without_shortcut(self, "Share Jemalloc", |app| app.show_jemalloc_flamegraph(false));
        context.add_global_action_without_shortcut(self, "Server Perfetto Export", |app| app.show_server_perfetto());

        // If logging is done to file, console is always empty
        if context.options.service.log.is_none() {
            context.add_global_action(
                self,
                "chdig debug console",
                '~',
                crate::tui::logger::toggle_debug_console,
            );
        }
        context.add_global_action(self, "Toggle debug metrics", '!', toggle_debug_metrics);
        context.add_global_action(self, "Back/Close pane", Key::Esc, |app| { if !app.pop_ui() { app.close_pane(); } });
        context.add_global_action(self, "Back/Close pane/Quit", 'q', |app| { if !app.pop_ui() && !app.close_pane() { app.quit(); } });
        context.add_global_action(self, "Split pane (right)", Event::AltChar('='), |app| app.split_pane(false));
        context.add_global_action(self, "Split pane (below)", Event::AltChar('-'), |app| app.split_pane(true));
        context.add_global_action(self, "Quit forcefully", 'Q', |app| app.quit());
        context.add_global_action(self, "Back", Key::Backspace, |app| {
            if !app.pop_ui() {
                app.show_previous_view();
            }
        });
        context.add_global_action(self, "Toggle pause", 'p', |app| app.toggle_pause_updates(None));
        context.add_global_action(self, "Refresh", 'r', |app| app.refresh_view());
        context.add_global_action(self, "Refresh all (with summary)", 'R', |app| app.refresh_all());

        // Bindings T/t inspiried by atop(1) (so as this functionality)
        context.add_global_action(self, "Seek 10 mins backward", 'T', |app| app.seek_time_frame(true));
        context.add_global_action(self, "Seek 10 mins forward", 't', |app| app.seek_time_frame(false));
        context.add_global_action(self, "Set time interval", Event::AltChar('t'), |app| app.select_time_frame());
    }

    fn initialize_views_menu(&mut self, context: ContextArc) {
        crate::tui::views::providers::register(&mut context.lock().unwrap());
    }

    fn show_help_dialog(&mut self) {
        if self.has_view("help") {
            self.pop_layer();
            return;
        }

        let bold = Style::default().add_modifier(Modifier::BOLD);
        let mut text = StyledString::new();

        text.append_styled(
            format!("chdig v{version}\n", version = env!("CARGO_PKG_VERSION")),
            bold,
        );

        {
            let owners = focused_action_owners(self);
            let context = self.user_data::<ContextArc>().unwrap().clone();
            let context = context.lock().unwrap();

            text.append_styled("\nGlobal shortcuts:\n\n", bold);
            for shortcut in context.global_actions.iter() {
                text.append(shortcut.description.preview_styled());
            }

            text.append_styled("\nActions:\n\n", bold);
            for shortcut in context
                .view_actions
                .iter()
                .filter(|a| owners.contains(a.owner))
            {
                text.append(shortcut.description.preview_styled());
            }
        }

        text.append_styled("\nExtended navigation:\n\n", bold);
        text.append_styled(
            format!("{:>10} - reset selection/follow item in table\n", "Home"),
            bold,
        );
        text.append_styled(
            format!("{:>10} - move focus between panes\n", "Alt+Arrows"),
            bold,
        );
        text.append_styled(format!("{:>10} - resize panes\n", "Ctrl+Arrows"), bold);
        text.append_styled(
            format!(
                "{:>10} - zoom the focused pane (fullscreen on/off)\n",
                "Ctrl+x"
            ),
            bold,
        );

        text.append_plain(format!(
            "\nIssues and suggestions: {homepage}/issues",
            homepage = env!("CARGO_PKG_HOMEPAGE")
        ));

        self.add_layer(Dialog::info(text).with_name("help"));
    }

    fn show_settings_dialog(&mut self) {
        crate::tui::views::settings_view::show_settings_dialog(self);
    }

    fn show_views(&mut self) {
        let mut has_views = false;
        let context = self.user_data::<ContextArc>().unwrap().clone();
        self.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
            if !left_menu_view.is_empty() {
                left_menu_view
                    .remove_child(left_menu_view.len() - 1)
                    .expect("No child view to remove");
            } else {
                let mut select = SelectView::new().autojump();
                {
                    let context = context.clone();
                    select.set_on_submit(move |app: &mut App, selected_action: &String| {
                        log::trace!("Switching to {:?}", selected_action);

                        app.focus_name("main");
                        {
                            let action_callback = context
                                .lock()
                                .unwrap()
                                .views_menu_actions
                                .iter()
                                .find(|x| x.description.text == selected_action)
                                .unwrap()
                                .callback
                                .clone();
                            action_callback.as_ref()(app);
                        };

                        app.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
                            left_menu_view
                                .remove_child(left_menu_view.len() - 1)
                                .expect("No child view to remove");
                        });
                    });
                }

                {
                    let context = context.clone();
                    let context = context.lock().unwrap();
                    for action in context.views_menu_actions.iter() {
                        select.add_item_str(action.description.text);
                    }
                }

                left_menu_view.add_child(menu_select(select));

                has_views = true;
            }
        });

        if has_views {
            self.focus_name("left_menu");
        } else {
            self.focus_name("main");
        }
    }

    fn show_actions(&mut self) {
        let mut has_actions = false;
        let owners = focused_action_owners(self);
        let context = self.user_data::<ContextArc>().unwrap().clone();
        self.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
            if !left_menu_view.is_empty() {
                left_menu_view
                    .remove_child(left_menu_view.len() - 1)
                    .expect("No child view to remove");
            } else {
                let mut select = SelectView::new().autojump();
                {
                    let context = context.clone();
                    select.set_on_submit(move |app: &mut App, selected_action: &String| {
                        log::trace!("Triggering {:?} (from actions)", selected_action);

                        app.focus_name("main");
                        // Replay the action's event through the regular event
                        // flow: the handler lives in the owning view's
                        // OnEventView (same path as a shortcut press).
                        let event = {
                            let owners = focused_action_owners(app);
                            let context = context.lock().unwrap();
                            context
                                .view_actions
                                .iter()
                                .find(|x| {
                                    x.description.text == selected_action
                                        && owners.contains(x.owner)
                                })
                                .map(|x| x.description.event.clone())
                        };
                        if let Some(event) = event {
                            app.on_event(event);
                        }

                        app.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
                            left_menu_view
                                .remove_child(left_menu_view.len() - 1)
                                .expect("No child view to remove");
                        });
                    });
                }

                {
                    let context = context.clone();
                    let context = context.lock().unwrap();
                    let mut has_any = false;
                    for action in context
                        .view_actions
                        .iter()
                        .filter(|a| owners.contains(a.owner))
                    {
                        select.add_item_str(action.description.text);
                        has_any = true;
                    }
                    if !has_any {
                        return;
                    }
                }

                left_menu_view.add_child(menu_select(select));

                has_actions = true;
            }
        });

        if has_actions {
            self.focus_name("left_menu");
        } else {
            self.focus_name("main");
        }
    }

    fn show_fuzzy_actions(&mut self) {
        let owners = focused_action_owners(self);
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let all_actions = {
            let context = context.lock().unwrap();
            context
                .global_actions
                .iter()
                .map(|x| &x.description)
                .chain(
                    context
                        .view_actions
                        .iter()
                        .filter(|x| owners.contains(x.owner))
                        .map(|x| &x.description),
                )
                .chain(context.views_menu_actions.iter().map(|x| &x.description))
                .cloned()
                .collect()
        };

        tui::fuzzy_actions(self, all_actions, move |app, action_text| {
            log::trace!("Triggering {:?} (from fuzzy search)", action_text);

            // Global callbacks
            {
                let action_callback = context
                    .lock()
                    .unwrap()
                    .global_actions
                    .iter()
                    .find(|x| x.description.text == action_text)
                    .map(|a| a.callback.clone());
                if let Some(action_callback) = action_callback {
                    action_callback.as_ref()(app);
                }
            }

            // View callbacks: replay the action's event through the regular
            // event flow (the handler lives in the owning view's OnEventView).
            {
                let owners = focused_action_owners(app);
                let event = context
                    .lock()
                    .unwrap()
                    .view_actions
                    .iter()
                    .find(|x| x.description.text == action_text && owners.contains(x.owner))
                    .map(|x| x.description.event.clone());
                if let Some(event) = event {
                    app.on_event(event);
                }
            }

            // View menus
            {
                let action_callback = context
                    .lock()
                    .unwrap()
                    .views_menu_actions
                    .iter()
                    .find(|x| x.description.text == action_text)
                    .map(|a| a.callback.clone());
                if let Some(action_callback) = action_callback {
                    action_callback.as_ref()(app);
                }
            }

            app.on_event(Event::Refresh);
        });
    }

    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let mut context = context.lock().unwrap();
        let start: DateTime<Local> = context.options.view.start.clone().into();
        let end: DateTime<Local> = context.options.view.end.clone().into();
        if let Some(trace_type) = trace_type {
            context.worker.send(
                true,
                WorkerEvent::ServerFlameGraph(tui, trace_type, start, end),
            );
        } else {
            context
                .worker
                .send(true, WorkerEvent::LiveQueryFlameGraph(tui, None));
        }
    }

    fn show_jemalloc_flamegraph(&mut self, tui: bool) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let mut context = context.lock().unwrap();
        context
            .worker
            .send(true, WorkerEvent::JemallocFlameGraph(tui));
    }

    fn show_flamelens(&mut self, fl: flamelens::app::App) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let pane = context.lock().unwrap().options.view.flamelens_pane;
        if pane == FlamelensPane::Off {
            if let Err(err) = crate::interpreter::flamegraph::show(fl) {
                self.add_layer(Dialog::info(err.to_string()));
            }
            return;
        }

        let view = FlamelensView::new(fl).with_name("flamelens");
        if self.focus_name("flamelens") {
            // Replace the existing flamelens pane in place (present_view
            // replaces the focused pane).
            self.present_view("flamelens", view);
            return;
        }
        let mut view = Some(view);
        self.call_on_name("panes", |mux: &mut Mux| {
            let focused = mux.focus();
            let view = view.take().unwrap();
            if pane == FlamelensPane::Above {
                mux.add_above(view, focused).unwrap();
            } else {
                mux.add_below(view, focused).unwrap();
            }
        });
        self.focus_name("flamelens");
    }

    fn show_server_perfetto(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let (start_str, end_str) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.view.start.to_editable_string(),
                ctx.options.view.end.to_editable_string(),
            )
        };

        let on_submit = move |app: &mut App| {
            let start_str = app
                .call_on_name("perfetto_start", |view: &mut EditView| view.get_content())
                .unwrap();
            let end_str = app
                .call_on_name("perfetto_end", |view: &mut EditView| view.get_content())
                .unwrap();

            let start = match start_str.parse::<crate::common::RelativeDateTime>() {
                Ok(v) => v,
                Err(err) => {
                    app.add_layer(Dialog::info(format!("Invalid start: {}", err)));
                    return;
                }
            };
            let end = match end_str.parse::<crate::common::RelativeDateTime>() {
                Ok(v) => v,
                Err(err) => {
                    app.add_layer(Dialog::info(format!("Invalid end: {}", err)));
                    return;
                }
            };

            app.pop_layer();

            let start_dt: DateTime<Local> = start.into();
            let end_dt: DateTime<Local> = end.into();
            let context = app.user_data::<ContextArc>().unwrap().clone();
            let mut ctx = context.lock().unwrap();
            ctx.worker
                .send(true, WorkerEvent::ServerPerfettoExport(start_dt, end_dt));
        };

        let dialog = Dialog::new()
            .title("Server Perfetto Export")
            .content(tui::submit_on_enter(
                LinearLayout::vertical()
                    .child(TextView::new(
                        "Warning: server-wide export is heavy (~1.5 GiB/server\nfor 2 min). Consider reducing the time range.",
                    ))
                    .child(DummyView)
                    .child(TextView::new("start:"))
                    .child(
                        EditView::new()
                            .content(start_str)
                            .with_name("perfetto_start")
                            .fixed_width(30),
                    )
                    .child(DummyView)
                    .child(TextView::new("end:"))
                    .child(
                        EditView::new()
                            .content(end_str)
                            .with_name("perfetto_end")
                            .fixed_width(30),
                    ),
                on_submit,
            ))
            .button("Export", on_submit)
            .button("Cancel", |app| {
                app.pop_layer();
            });
        self.add_layer(dialog);
    }

    fn show_connection_dialog(&mut self) {
        let context_arc = self.user_data::<ContextArc>().unwrap().clone();
        let context = context_arc.lock().unwrap();

        let cluster = context.options.clickhouse.cluster.clone();
        if cluster.is_none() {
            drop(context);
            self.add_layer(Dialog::info(
                "Cluster mode is not enabled. Use --cluster option.",
            ));
            return;
        }

        let clickhouse = context.clickhouse.clone();
        let ui_sink = context.ui_sink.clone();
        drop(context);

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let hosts = runtime.block_on(async { clickhouse.get_cluster_hosts().await });

            ui_sink
                .send(Box::new(move |app: &mut App| match hosts {
                    Ok(hosts) if !hosts.is_empty() => {
                        let context_arc = app.user_data::<ContextArc>().unwrap().clone();
                        let mut items: Vec<(String, String)> = Vec::with_capacity(hosts.len() + 1);
                        items.push(("<All hosts (reset filter)>".to_string(), String::new()));
                        for host in hosts {
                            items.push((host.clone(), host));
                        }

                        tui::fuzzy_select_strings(
                            app,
                            "Filter by host",
                            items,
                            move |app, selected_host| {
                                let current_view = {
                                    let mut context = context_arc.lock().unwrap();

                                    let url_safe = context.options.clickhouse.url_safe.clone();
                                    if selected_host.is_empty() {
                                        context.selected_host = None;
                                        log::info!("Reset host filter");
                                        app.set_statusbar_connection(url_safe);
                                    } else {
                                        context.selected_host = Some(selected_host.clone());
                                        log::info!("Set host filter to: {}", selected_host);
                                        app.set_statusbar_connection(format!(
                                            "{url_safe} (host: {selected_host})"
                                        ));
                                    }

                                    context
                                        .current_view
                                        .or(context.options.start_view())
                                        .unwrap_or(ChDigViews::Queries)
                                };

                                log::info!("Reopen {:?} view", current_view);

                                let provider = context_arc
                                    .lock()
                                    .unwrap()
                                    .view_registry
                                    .get_by_view_type(current_view);

                                // Other panes keep queries built with the old
                                // host filter: collapse to a single pane.
                                app.call_on_name("panes", |mux: &mut Mux| {
                                    let focused = mux.focus();
                                    for id in mux.panes() {
                                        if id != focused {
                                            mux.remove_id(id).unwrap();
                                        }
                                    }
                                    mux.set_focus(focused);
                                });
                                app.drop_main_view();
                                provider.show(app, context_arc.clone());

                                context_arc.lock().unwrap().trigger_view_refresh();
                            },
                        );
                    }
                    Ok(_) => {
                        app.add_layer(Dialog::info("No hosts found in cluster"));
                    }
                    Err(err) => {
                        app.add_layer(Dialog::info(format!(
                            "Failed to fetch cluster hosts: {}",
                            err
                        )));
                    }
                }))
                .unwrap();
        });
    }

    fn drop_main_view(&mut self) {
        while self.screen_len() > 1 {
            self.pop_layer();
        }

        // The lone pane cannot be removed from the Mux, so "dropping" is
        // replacing the pane content with a placeholder (removal is
        // add-new-then-remove-old, as in present_view).
        self.call_on_name("panes", |mux: &mut Mux| {
            let old = mux.focus();
            if mux.active_view().is_some() {
                mux.add_right_of(DummyView, old).unwrap();
                mux.remove_id(old).unwrap();
            }
        });
    }

    fn present_view<V: Component + 'static>(&mut self, focus: &str, view: V) {
        while self.screen_len() > 1 {
            self.pop_layer();
        }

        let mut view = Some(view);
        self.call_on_name("panes", |mux: &mut Mux| {
            let old = mux.focus();
            let replace = mux.active_view().is_some();
            mux.add_right_of(view.take().unwrap(), old).unwrap();
            if replace {
                mux.remove_id(old).unwrap();
            }
        });
        self.focus_name(focus);
    }

    fn present_logs<V: Component + 'static>(
        &mut self,
        view_name: &'static str,
        title: &str,
        view: V,
    ) {
        let in_dialog = {
            let context = self.user_data::<ContextArc>().unwrap().clone();
            let ctx = context.lock().unwrap();
            ctx.options.view.logs_in_dialog
        };

        let content = LinearLayout::vertical()
            .child(TextView::new(title).center())
            .child(DummyView.fixed_height(1))
            .child(view);

        if in_dialog {
            self.add_layer(Dialog::around(content));
            self.focus_name(view_name);
        } else if self.focus_name(view_name) {
            // Two views with one name would both receive the worker updates:
            // replace the existing one in place (present_view replaces the
            // focused pane).
            self.present_view(view_name, content);
        } else {
            let mut content = Some(content);
            self.call_on_name("panes", |mux: &mut Mux| {
                let focused = mux.focus();
                mux.add_right_of(content.take().unwrap(), focused).unwrap();
            });
            self.focus_name(view_name);
        }
    }

    fn split_pane(&mut self, below: bool) {
        let added = self
            .call_on_name("panes", |mux: &mut Mux| {
                let focused = mux.focus();
                // Nothing to split while the first view is not shown yet
                if mux.active_view().is_none() {
                    return false;
                }
                if below {
                    mux.add_below(PaneStub::new(), focused).unwrap();
                } else {
                    mux.add_right_of(PaneStub::new(), focused).unwrap();
                }
                true
            })
            .unwrap_or(false);
        if added {
            self.show_views();
        }
    }

    fn close_pane(&mut self) -> bool {
        self.call_on_name("panes", |mux: &mut Mux| {
            let focused = mux.focus();
            mux.remove_id(focused).is_ok()
        })
        .unwrap_or(false)
    }

    fn set_statusbar_version(&mut self, main_content: impl Into<StyledString>) {
        let content: StyledString = main_content.into();
        self.call_on_name("version", |text_view: &mut TextView| {
            let mut styled = StyledString::new();
            // NOTE: may not work in some terminals
            styled.append_styled(
                content.source(),
                Style::default().add_modifier(Modifier::DIM),
            );
            text_view.set_content(styled);
        })
        .expect("version");
    }

    fn set_statusbar_content(&mut self, content: impl Into<StyledString>) {
        let content: StyledString = content.into();
        self.call_on_name("status", |text_view: &mut TextView| {
            text_view.set_content(content);
        })
        .expect("set_status")
    }

    fn set_statusbar_connection(&mut self, content: impl Into<StyledString>) {
        let content: StyledString = content.into();
        self.call_on_name("connection", |text_view: &mut TextView| {
            text_view.set_content(content);
        })
        .expect("connection");
    }

    fn set_statusbar_debug(&mut self, content: impl Into<StyledString>) {
        let content: StyledString = content.into();
        self.call_on_name("debug_status", |text_view: &mut TextView| {
            let src = content.source();
            if src.is_empty() {
                text_view.set_content("");
                return;
            }
            // Trailing space keeps the debug text from butting against the next
            // status-bar element; gray makes it visually distinct from the main
            // "status" message (which is full-intensity white).
            let mut styled = StyledString::new();
            styled.append_styled(format!("{} ", src), Style::default().fg(Color::DarkGray));
            text_view.set_content(styled);
        });
    }

    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: Component,
        F: FnOnce(&mut V) -> Result<()>,
    {
        let ret = self.call_on_name(name, callback);
        if let Some(Err(err)) = ret {
            self.add_layer(Dialog::info(err.to_string()));
        }
    }
}
