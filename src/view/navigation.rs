use crate::utils::fuzzy_actions;
use crate::{
    common::parse_datetime_or_date,
    interpreter::{ContextArc, WorkerEvent, clickhouse::TraceType, options::ChDigViews},
    view,
};
use anyhow::Result;
use chrono::{DateTime, Local};
use cursive::{
    Cursive, Rect, Vec2,
    event::{Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle, Effect, PaletteColor, Style, Theme},
    utils::{markup::StyledString, span::SpannedString},
    view::{IntoBoxedView, Nameable, Resizable, View},
    views::{
        Dialog, DummyView, EditView, FixedLayout, Layer, LinearLayout, OnEventView, OnLayoutView,
        SelectView, TextContent, TextView,
    },
};
use cursive_flexi_logger_view::toggle_flexi_logger_debug_console;

fn make_menu_text() -> StyledString {
    let mut text = StyledString::new();

    // F1
    text.append_plain("F1");
    text.append_styled("Help", ColorStyle::highlight());
    // F2
    text.append_plain("F2");
    text.append_styled("Views", ColorStyle::highlight());
    // F8
    text.append_plain("F8");
    text.append_styled("Actions", ColorStyle::highlight());

    return text;
}

pub trait Navigation {
    fn has_view(&mut self, name: &str) -> bool;

    fn make_theme_from_therminal(&mut self) -> Theme;
    fn pop_ui(&mut self, exit: bool);
    fn toggle_pause_updates(&mut self, reason: Option<&str>);
    fn refresh_view(&mut self);
    fn seek_time_frame(&mut self, is_sub: bool);
    fn select_time_frame(&mut self);

    fn initialize_global_shortcuts(&mut self, context: ContextArc);
    fn initialize_views_menu(&mut self, context: ContextArc);
    fn chdig(&mut self, context: ContextArc);

    fn show_help_dialog(&mut self);
    fn show_views(&mut self);
    fn show_actions(&mut self);
    fn show_fuzzy_actions(&mut self);
    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>);

    fn drop_main_view(&mut self);
    fn set_main_view<V: IntoBoxedView + 'static>(&mut self, view: V);

    fn statusbar(&mut self, main_content: impl Into<SpannedString<Style>>);
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>);

    // TODO: move into separate trait
    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: View,
        F: FnOnce(&mut V) -> Result<()>;
}

impl Navigation for Cursive {
    fn has_view(&mut self, name: &str) -> bool {
        return self.focus_name(name).is_ok();
    }

    fn make_theme_from_therminal(&mut self) -> Theme {
        let mut theme = self.current_theme().clone();
        theme.palette[PaletteColor::Background] = Color::TerminalDefault;
        theme.palette[PaletteColor::View] = Color::TerminalDefault;
        theme.palette[PaletteColor::Primary] = Color::TerminalDefault;
        theme.palette[PaletteColor::Highlight] = Color::Light(BaseColor::Cyan);
        theme.palette[PaletteColor::HighlightText] = Color::Dark(BaseColor::Black);
        theme.shadow = false;
        return theme;
    }

    fn pop_ui(&mut self, exit: bool) {
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
            self.focus_name("main").unwrap();
            return;
        }

        // - main view
        // - statusbar
        if self.screen_mut().len() == 2 {
            if exit {
                self.quit();
            }
        } else {
            self.pop_layer();
        }
    }

    fn toggle_pause_updates(&mut self, reason: Option<&str>) {
        let is_paused;
        {
            let mut context = self.user_data::<ContextArc>().unwrap().lock().unwrap();
            // NOTE: though it will be better to stop sending any message completely, instead of
            // simply ignoring them
            context.worker.toggle_pause();
            is_paused = context.worker.is_paused();
        }

        self.call_on_name("is_paused", |v: &mut TextView| {
            let mut text = StyledString::new();
            if is_paused {
                text.append_styled(" PAUSED", Effect::Bold);
                if let Some(reason) = reason {
                    text.append_styled(format!(" ({})", reason), Effect::Bold);
                }
                text.append_styled(" press P to resume", Effect::Italic);
            }
            v.set_content(text);
        });
    }

    fn refresh_view(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().lock().unwrap();
        log::trace!("Toggle refresh");
        context.trigger_view_refresh();
    }

    fn seek_time_frame(&mut self, is_sub: bool) {
        let mut context = self.user_data::<ContextArc>().unwrap().lock().unwrap();
        context.shift_time_interval(is_sub, 10);
        context.trigger_view_refresh();
    }

    fn select_time_frame(&mut self) {
        let on_submit = move |siv: &mut Cursive| {
            let start = siv
                .call_on_name("start", |view: &mut EditView| view.get_content())
                .unwrap();
            let end = siv
                .call_on_name("end", |view: &mut EditView| view.get_content())
                .unwrap();

            siv.pop_layer();

            let new_begin = match parse_datetime_or_date(&start) {
                Ok(new) => new,
                Err(err) => {
                    siv.add_layer(Dialog::info(err));
                    return;
                }
            };
            let new_end = match parse_datetime_or_date(&end) {
                Ok(new) => new,
                Err(err) => {
                    siv.add_layer(Dialog::info(err));
                    return;
                }
            };
            log::debug!("Set time frame to ({}, {})", new_begin, new_end);
            let mut context = siv.user_data::<ContextArc>().unwrap().lock().unwrap();
            context.options.view.start = new_begin.into();
            context.options.view.end = new_end.into();
            context.trigger_view_refresh();
        };

        let view = OnEventView::new(
            Dialog::new()
                .title("Set the time interval")
                .content(
                    LinearLayout::vertical()
                        .child(TextView::new("format: YYYY-MM-DD hh:mm:ss"))
                        .child(DummyView)
                        .child(TextView::new("start:"))
                        .child(EditView::new().with_name("start"))
                        .child(DummyView)
                        .child(TextView::new("end:"))
                        .child(EditView::new().with_name("end")),
                )
                .button("Submit", on_submit),
        );
        self.add_layer(view);
    }

    fn chdig(&mut self, context: ContextArc) {
        self.set_user_data(context.clone());
        self.initialize_global_shortcuts(context.clone());
        self.initialize_views_menu(context.clone());

        let theme = self.make_theme_from_therminal();
        self.set_theme(theme);

        self.statusbar(format!(
            "Connected to {}.",
            context.lock().unwrap().server_version
        ));

        self.add_fullscreen_layer(
            LinearLayout::horizontal()
                .child(LinearLayout::vertical().with_name("left_menu"))
                .child(
                    LinearLayout::vertical()
                        // FIXME: there is one extra line on top
                        .child(
                            LinearLayout::horizontal()
                                .child(TextView::new(make_menu_text()))
                                .child(TextView::new("").with_name("is_paused")),
                        )
                        .child(view::SummaryView::new(context.clone()).with_name("summary"))
                        .with_name("main"),
                ),
        );

        let start_view = context
            .lock()
            .unwrap()
            .options
            .start_view
            .unwrap_or(ChDigViews::Queries);

        let provider = context
            .lock()
            .unwrap()
            .view_registry
            .get_by_view_type(start_view);
        provider.show(self, context.clone());
    }

    /// Ignore rustfmt max_width, otherwise callback actions looks ugly
    #[rustfmt::skip]
    fn initialize_global_shortcuts(&mut self, context: ContextArc) {
        let mut context = context.lock().unwrap();

        context.add_global_action(self, "Show help", Key::F1, |siv| siv.show_help_dialog());

        context.add_global_action(self, "Views", Key::F2, |siv| siv.show_views());
        context.add_global_action(self, "Show actions", Key::F8, |siv| siv.show_actions());
        context.add_global_action(self, "Fuzzy actions", Event::CtrlChar('p'), |siv| siv.show_fuzzy_actions());

        context.add_global_action(self, "CPU Server Flamegraph", 'F', |siv| siv.show_server_flamegraph(true, Some(TraceType::CPU)));
        context.add_global_action_without_shortcut(self, "Real Server Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::Real)));
        context.add_global_action_without_shortcut(self, "Memory Server Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::Memory)));
        context.add_global_action_without_shortcut(self, "Memory Sample Server Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::MemorySample)));
        context.add_global_action_without_shortcut(self, "Jemalloc Sample Server Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::JemallocSample)));
        context.add_global_action_without_shortcut(self, "Events Server Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::ProfileEvents)));
        context.add_global_action_without_shortcut(self, "Live Server Flamegraph", |siv| siv.show_server_flamegraph(true, None));
        context.add_global_action_without_shortcut(self, "CPU Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, Some(TraceType::CPU)));
        context.add_global_action_without_shortcut(self, "Real Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, Some(TraceType::Real)));
        context.add_global_action_without_shortcut(self, "Memory Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, Some(TraceType::Memory)));
        context.add_global_action_without_shortcut(self, "Memory Sample Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, Some(TraceType::MemorySample)));
        context.add_global_action_without_shortcut(self, "Jemalloc Sample Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, Some(TraceType::JemallocSample)));
        context.add_global_action_without_shortcut(self, "Events Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, Some(TraceType::ProfileEvents)));
        context.add_global_action_without_shortcut(self, "Live Server Flamegraph in speedscope", |siv| siv.show_server_flamegraph(false, None));

        // If logging is done to file, console is always empty
        if context.options.service.log.is_none() {
            context.add_global_action(
                self,
                "chdig debug console",
                '~',
                toggle_flexi_logger_debug_console,
            );
        }
        context.add_global_action(self, "Back/Quit", Key::Esc, |siv| siv.pop_ui(false));
        context.add_global_action(self, "Back/Quit", 'q', |siv| siv.pop_ui(true));
        context.add_global_action(self, "Quit forcefully", 'Q', |siv| siv.quit());
        context.add_global_action(self, "Back", Key::Backspace, |siv| siv.pop_ui(false));
        context.add_global_action(self, "Toggle pause", 'p', |siv| siv.toggle_pause_updates(None));
        context.add_global_action(self, "Refresh", 'r', |siv| siv.refresh_view());

        // Bindings T/t inspiried by atop(1) (so as this functionality)
        context.add_global_action(self, "Seek 10 mins backward", 'T', |siv| siv.seek_time_frame(true));
        context.add_global_action(self, "Seek 10 mins forward", 't', |siv| siv.seek_time_frame(false));
        context.add_global_action(self, "Set time interval", Event::AltChar('t'), |siv| siv.select_time_frame());
    }

    fn initialize_views_menu(&mut self, context: ContextArc) {
        use crate::view::providers::*;
        use std::sync::Arc;

        let mut c = context.lock().unwrap();

        c.register_provider(Arc::new(ProcessesViewProvider));
        c.register_provider(Arc::new(SlowQueryLogViewProvider));
        c.register_provider(Arc::new(LastQueryLogViewProvider));
        c.register_provider(Arc::new(MergesViewProvider));
        c.register_provider(Arc::new(S3QueueViewProvider));
        c.register_provider(Arc::new(MutationsViewProvider));
        c.register_provider(Arc::new(ReplicatedFetchesViewProvider));
        c.register_provider(Arc::new(ReplicationQueueViewProvider));
        c.register_provider(Arc::new(ReplicasViewProvider));
        c.register_provider(Arc::new(TablesViewProvider));
        c.register_provider(Arc::new(BackupsViewProvider));
        c.register_provider(Arc::new(DictionariesViewProvider));
        c.register_provider(Arc::new(ServerLogsViewProvider));
        c.register_provider(Arc::new(LoggerNamesViewProvider));
        c.register_provider(Arc::new(ErrorsViewProvider));
        c.register_provider(Arc::new(ClientViewProvider));
    }

    fn show_help_dialog(&mut self) {
        if self.has_view("help") {
            self.pop_layer();
            return;
        }

        let mut text = StyledString::default();

        text.append_styled(
            format!("chdig v{version}\n", version = env!("CARGO_PKG_VERSION")),
            Effect::Bold,
        );

        {
            let context = self.user_data::<ContextArc>().unwrap().lock().unwrap();

            text.append_styled("\nGlobal shortcuts:\n\n", Effect::Bold);
            for shortcut in context.global_actions.iter() {
                text.append(shortcut.description.preview_styled());
            }

            text.append_styled("\nActions:\n\n", Effect::Bold);
            for shortcut in context.view_actions.iter() {
                text.append(shortcut.description.preview_styled());
            }
        }

        text.append_styled("\nExtended navigation:\n\n", Effect::Bold);
        text.append_styled(
            format!("{:>10} - reset selection/follow item in table\n", "Home"),
            Effect::Bold,
        );

        text.append_plain(format!(
            "\nIssues and suggestions: {homepage}/issues",
            homepage = env!("CARGO_PKG_HOMEPAGE")
        ));

        self.add_layer(Dialog::info(text).with_name("help"));
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
                    select.set_on_submit(move |siv, selected_action: &str| {
                        log::trace!("Switching to {:?}", selected_action);

                        siv.focus_name("main").unwrap();
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
                            action_callback.as_ref()(siv);
                        };

                        siv.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
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

                let select = OnEventView::new(select)
                    .on_pre_event_inner('k', |s, _| {
                        let cb = s.select_up(1);
                        Some(EventResult::Consumed(Some(cb)))
                    })
                    .on_pre_event_inner('j', |s, _| {
                        let cb = s.select_down(1);
                        Some(EventResult::Consumed(Some(cb)))
                    })
                    .with_name("actions_select");

                left_menu_view.add_child(select);

                has_views = true;
            }
        });

        if has_views {
            self.focus_name("left_menu").unwrap();
        } else {
            self.focus_name("main").unwrap();
        }
    }

    fn show_actions(&mut self) {
        let mut has_actions = false;
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
                    select.set_on_submit(move |siv, selected_action: &str| {
                        log::trace!("Triggering {:?} (from actions)", selected_action);

                        siv.focus_name("main").unwrap();
                        {
                            let mut context = context.lock().unwrap();
                            let action_callback = context
                                .view_actions
                                .iter()
                                .find(|x| x.description.text == selected_action)
                                .unwrap()
                                .callback
                                .clone();
                            context.pending_view_callback = Some(action_callback);
                        };
                        siv.on_event(Event::Refresh);

                        siv.call_on_name("left_menu", |left_menu_view: &mut LinearLayout| {
                            left_menu_view
                                .remove_child(left_menu_view.len() - 1)
                                .expect("No child view to remove");
                        });
                    });
                }

                {
                    let context = context.clone();
                    let context = context.lock().unwrap();
                    for action in context.view_actions.iter() {
                        select.add_item_str(action.description.text);
                    }
                    if context.view_actions.is_empty() {
                        return;
                    }
                }

                let select = OnEventView::new(select)
                    .on_pre_event_inner('k', |s, _| {
                        let cb = s.select_up(1);
                        Some(EventResult::Consumed(Some(cb)))
                    })
                    .on_pre_event_inner('j', |s, _| {
                        let cb = s.select_down(1);
                        Some(EventResult::Consumed(Some(cb)))
                    })
                    .with_name("actions_select");

                left_menu_view.add_child(select);

                has_actions = true;
            }
        });

        if has_actions {
            self.focus_name("left_menu").unwrap();
        } else {
            self.focus_name("main").unwrap();
        }
    }

    fn show_fuzzy_actions(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let all_actions = {
            let context = context.lock().unwrap();
            context
                .global_actions
                .iter()
                .map(|x| &x.description)
                .chain(context.view_actions.iter().map(|x| &x.description))
                .chain(context.views_menu_actions.iter().map(|x| &x.description))
                .cloned()
                .collect()
        };

        fuzzy_actions(self, all_actions, move |siv, action_text| {
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
                    action_callback.as_ref()(siv);
                }
            }

            // View callbacks
            {
                let mut context = context.lock().unwrap();
                if let Some(action) = context
                    .view_actions
                    .iter()
                    .find(|x| x.description.text == action_text)
                {
                    context.pending_view_callback = Some(action.callback.clone());
                    // The pending_view_callback handling is binded to Event::Refresh event, but it
                    // cannot be called with the context locked, so it will be called
                    // asynchronously after Event::Refresh below
                    //
                    // But, we also need it to cleanup the screen (to avoid any leftovers), so, it
                    // will be called always.
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
                    action_callback.as_ref()(siv);
                }
            }

            siv.on_event(Event::Refresh);
        });
    }

    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>) {
        let mut context = self.user_data::<ContextArc>().unwrap().lock().unwrap();
        let start: DateTime<Local> = context.options.view.start.clone().into();
        let end: DateTime<Local> = context.options.view.end.clone().into();
        if let Some(trace_type) = trace_type {
            context.worker.send(
                true,
                WorkerEvent::ShowServerFlameGraph(tui, trace_type, start, end),
            );
        } else {
            context
                .worker
                .send(true, WorkerEvent::ShowLiveQueryFlameGraph(tui, None));
        }
    }

    fn drop_main_view(&mut self) {
        while self.screen_mut().len() > 2 {
            self.pop_layer();
        }

        self.call_on_name("main", |main_view: &mut LinearLayout| {
            // Views that should not be touched:
            // - menu text
            // - summary
            if main_view.len() > 2 {
                main_view
                    .remove_child(main_view.len() - 1)
                    .expect("No child view to remove");
            }
        });
    }

    fn set_main_view<V: IntoBoxedView + 'static>(&mut self, view: V) {
        self.call_on_name("main", |main_view: &mut LinearLayout| {
            main_view.add_child(view);
        });
    }

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

    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: View,
        F: FnOnce(&mut V) -> Result<()>,
    {
        let ret = self.call_on_name(name, callback);
        if let Some(Err(err)) = ret {
            self.add_layer(Dialog::info(err.to_string()));
        }
    }
}
