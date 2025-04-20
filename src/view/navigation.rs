use crate::{
    interpreter::{
        clickhouse::TraceType,
        options::{parse_datetime_or_date, ChDigViews},
        ContextArc, WorkerEvent,
    },
    view::{self, TextLogView},
};
use anyhow::Result;
#[cfg(not(target_family = "windows"))]
use chdig::fuzzy_actions;
use cursive::{
    event::{Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle, Effect, PaletteColor, Style, Theme},
    utils::{markup::StyledString, span::SpannedString},
    view::View,
    view::{IntoBoxedView, Nameable, Resizable},
    views::{
        Dialog, DummyView, EditView, FixedLayout, Layer, LinearLayout, OnEventView, OnLayoutView,
        SelectView, TextContent, TextView,
    },
    Cursive, {Rect, Vec2},
};
use cursive_flexi_logger_view::toggle_flexi_logger_debug_console;
use std::collections::HashMap;

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
    fn toggle_pause_updates(&mut self);
    fn refresh_view(&mut self);
    fn seek_time_frame(&mut self, is_sub: bool);
    fn select_time_frame(&mut self);

    fn initialize_global_shortcuts(&mut self, context: ContextArc);
    fn initialize_views_menu(&mut self, context: ContextArc);
    fn chdig(&mut self, context: ContextArc);

    fn show_help_dialog(&mut self);
    fn show_views(&mut self);
    fn show_actions(&mut self);
    #[cfg(not(target_family = "windows"))]
    fn show_fuzzy_actions(&mut self);
    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>);

    fn drop_main_view(&mut self);
    fn set_main_view<V: IntoBoxedView + 'static>(&mut self, view: V);

    fn statusbar(&mut self, main_content: impl Into<SpannedString<Style>>);
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>);

    fn show_clickhouse_processes(&mut self, context: ContextArc);
    fn show_clickhouse_slow_query_log(&mut self, context: ContextArc);
    fn show_clickhouse_last_query_log(&mut self, context: ContextArc);
    fn show_clickhouse_merges(&mut self, context: ContextArc);
    fn show_clickhouse_mutations(&mut self, context: ContextArc);
    fn show_clickhouse_replication_queue(&mut self, context: ContextArc);
    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc);
    fn show_clickhouse_replicas(&mut self, context: ContextArc);
    fn show_clickhouse_errors(&mut self, context: ContextArc);
    fn show_clickhouse_backups(&mut self, context: ContextArc);
    fn show_clickhouse_dictionaries(&mut self, context: ContextArc);
    fn show_clickhouse_server_logs(&mut self, context: ContextArc);

    #[allow(clippy::too_many_arguments)]
    fn show_query_result_view<F>(
        &mut self,
        context: ContextArc,
        table: &'static str,
        filter: Option<&'static str>,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
        columns_to_compare: usize,
        on_submit: Option<F>,
        settings: &HashMap<&str, &str>,
    ) where
        F: Fn(&mut Cursive, view::QueryResultRow) + Send + Sync + 'static;

    // TODO: move into separate trait
    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: View,
        F: FnOnce(&mut V) -> Result<()>;
}

const QUERY_RESULT_VIEW_NOP_CALLBACK: Option<fn(&mut Cursive, view::QueryResultRow)> = None;

impl Navigation for Cursive {
    fn has_view(&mut self, name: &str) -> bool {
        return self.focus_name(name).is_ok();
    }

    // TODO: use the same color schema as in htop/csysdig
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

    fn toggle_pause_updates(&mut self) {
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
            context.options.view.start = new_begin;
            context.options.view.end = new_end;
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

        self.add_layer(
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
            .clone()
            .unwrap_or(ChDigViews::Queries);
        match start_view {
            ChDigViews::Queries => self.show_clickhouse_processes(context.clone()),
            ChDigViews::LastQueries => self.show_clickhouse_last_query_log(context.clone()),
            ChDigViews::SlowQueries => self.show_clickhouse_slow_query_log(context.clone()),
            ChDigViews::Merges => self.show_clickhouse_merges(context.clone()),
            ChDigViews::Mutations => self.show_clickhouse_mutations(context.clone()),
            ChDigViews::ReplicationQueue => self.show_clickhouse_replication_queue(context.clone()),
            ChDigViews::ReplicatedFetches => {
                self.show_clickhouse_replicated_fetches(context.clone())
            }
            ChDigViews::Replicas => self.show_clickhouse_replicas(context.clone()),
            ChDigViews::Errors => self.show_clickhouse_errors(context.clone()),
            ChDigViews::Backups => self.show_clickhouse_backups(context.clone()),
            ChDigViews::Dictionaries => self.show_clickhouse_dictionaries(context.clone()),
            ChDigViews::ServerLogs => self.show_clickhouse_server_logs(context.clone()),
        }
    }

    fn initialize_global_shortcuts(&mut self, context: ContextArc) {
        let mut context = context.lock().unwrap();

        context.add_global_action(self, "Show help", Key::F1, |siv| siv.show_help_dialog());

        context.add_global_action(self, "Views", Key::F2, |siv| siv.show_views());
        context.add_global_action(self, "Show actions", Key::F8, |siv| siv.show_actions());
        #[cfg(not(target_family = "windows"))]
        context.add_global_action(self, "Fuzzy actions", Event::CtrlChar('p'), |siv| {
            siv.show_fuzzy_actions()
        });

        context.add_global_action(self, "CPU Server Flamegraph", 'F', |siv| {
            siv.show_server_flamegraph(true, Some(TraceType::CPU))
        });
        context.add_global_action_without_shortcut(self, "Real Server Flamegraph", |siv| {
            siv.show_server_flamegraph(true, Some(TraceType::Real))
        });
        context.add_global_action_without_shortcut(self, "Live Server Flamegraph", |siv| {
            siv.show_server_flamegraph(true, None)
        });
        context.add_global_action_without_shortcut(
            self,
            "CPU Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, Some(TraceType::CPU)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Real Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, Some(TraceType::Real)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Live Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, None),
        );

        context.add_global_action(
            self,
            "chdig debug console",
            '~',
            toggle_flexi_logger_debug_console,
        );
        context.add_global_action(self, "Back/Quit", Key::Esc, |siv| siv.pop_ui(true));
        context.add_global_action(self, "Back/Quit", 'q', |siv| siv.pop_ui(true));
        context.add_global_action(self, "Quit forcefully", 'Q', |siv| siv.quit());
        context.add_global_action(self, "Back", Key::Backspace, |siv| siv.pop_ui(false));
        context.add_global_action(self, "Toggle pause", 'p', |siv| siv.toggle_pause_updates());
        context.add_global_action(self, "Refresh", 'r', |siv| siv.refresh_view());

        // Bindings T/t inspiried by atop(1) (so as this functionality)
        context.add_global_action(self, "Seek 10 mins backward", 'T', |siv| {
            siv.seek_time_frame(true)
        });
        context.add_global_action(self, "Seek 10 mins forward", 't', |siv| {
            siv.seek_time_frame(false)
        });
        context.add_global_action(self, "Set time interval", Event::AltChar('t'), |siv| {
            siv.select_time_frame()
        });
    }

    fn initialize_views_menu(&mut self, context: ContextArc) {
        let mut c = context.lock().unwrap();

        // TODO: macro
        {
            let ctx = context.clone();
            c.add_view("Processes", move |siv| {
                siv.show_clickhouse_processes(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Slow queries", move |siv| {
                siv.show_clickhouse_slow_query_log(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Last queries", move |siv| {
                siv.show_clickhouse_last_query_log(ctx.clone())
            });
        }

        {
            let ctx = context.clone();
            c.add_view("Merges", move |siv| siv.show_clickhouse_merges(ctx.clone()));
        }
        {
            let ctx = context.clone();
            c.add_view("Mutations", move |siv| {
                siv.show_clickhouse_mutations(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Fetches", move |siv| {
                siv.show_clickhouse_replicated_fetches(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Replication queue", move |siv| {
                siv.show_clickhouse_replication_queue(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Replicas", move |siv| {
                siv.show_clickhouse_replicas(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Backups", move |siv| {
                siv.show_clickhouse_backups(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Dictionaries", move |siv| {
                siv.show_clickhouse_dictionaries(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Server logs", move |siv| {
                siv.show_clickhouse_server_logs(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Errors", move |siv| siv.show_clickhouse_errors(ctx.clone()));
        }
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

    #[cfg(not(target_family = "windows"))]
    fn show_fuzzy_actions(&mut self) {
        let context = self.user_data::<ContextArc>().unwrap().clone();
        let actions;
        {
            let context = context.lock().unwrap();
            actions = context
                .global_actions
                .iter()
                .map(|x| &x.description)
                .chain(context.view_actions.iter().map(|x| &x.description))
                .cloned()
                .collect();
        }

        self.clear();
        let action_text = fuzzy_actions(actions);
        log::trace!("Triggering {:?} (from fuzzy search)", action_text);

        if let Some(action_text) = action_text {
            // Global callbacks
            {
                let mut action_callback = None;
                if let Some(action) = context
                    .lock()
                    .unwrap()
                    .global_actions
                    .iter()
                    .find(|x| x.description.text == action_text)
                {
                    action_callback = Some(action.callback.clone());
                }
                if let Some(action_callback) = action_callback {
                    action_callback.as_ref()(self);
                }
            }

            // View callbacks
            let mut need_refresh = false;
            {
                let mut context = context.lock().unwrap();
                if let Some(action) = context
                    .view_actions
                    .iter()
                    .find(|x| x.description.text == action_text)
                {
                    context.pending_view_callback = Some(action.callback.clone());
                    need_refresh = true;
                }
            }
            // The pending_view_callback handling is binded to Event::Refresh event, but it cannot
            // be called with the context locked, hence separate code path.
            if need_refresh {
                self.on_event(Event::Refresh);
            }
        } else {
            self.on_event(Event::WindowResize);
        }
    }

    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>) {
        let mut context = self.user_data::<ContextArc>().unwrap().lock().unwrap();
        let start = context.options.view.start;
        let end = context.options.view.end;
        if let Some(trace_type) = trace_type {
            context.worker.send(WorkerEvent::ShowServerFlameGraph(
                tui, trace_type, start, end,
            ));
        } else {
            context
                .worker
                .send(WorkerEvent::ShowLiveQueryFlameGraph(tui, None));
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

    fn show_clickhouse_processes(&mut self, context: ContextArc) {
        if self.has_view("processes") {
            return;
        }

        self.drop_main_view();
        self.set_main_view(
            Dialog::around(
                view::ProcessesView::new(
                    context.clone(),
                    view::ProcessesType::ProcessList,
                    "processes",
                )
                .with_name("processes")
                .full_screen(),
            )
            .title("Queries"),
        );
        self.focus_name("processes").unwrap();
    }

    fn show_clickhouse_slow_query_log(&mut self, context: ContextArc) {
        if self.has_view("slow_query_log") {
            return;
        }

        self.drop_main_view();
        self.set_main_view(
            Dialog::around(
                view::ProcessesView::new(
                    context.clone(),
                    view::ProcessesType::SlowQueryLog,
                    "slow_query_log",
                )
                .with_name("slow_query_log")
                .full_screen(),
            )
            .title("Slow queries"),
        );
        self.focus_name("slow_query_log").unwrap();
    }

    fn show_clickhouse_last_query_log(&mut self, context: ContextArc) {
        if self.has_view("last_query_log") {
            return;
        }

        self.drop_main_view();
        self.set_main_view(
            Dialog::around(
                view::ProcessesView::new(
                    context.clone(),
                    view::ProcessesType::LastQueryLog,
                    "last_query_log",
                )
                .with_name("last_query_log")
                .full_screen(),
            )
            .title("Last queries"),
        );
        self.focus_name("last_query_log").unwrap();
    }

    fn show_clickhouse_merges(&mut self, context: ContextArc) {
        let mut columns = vec![
            "database",
            "table",
            "result_part_name part",
            "elapsed",
            "progress",
            "num_parts parts",
            "is_mutation mutation",
            "total_size_bytes_compressed size",
            "rows_read",
            "rows_written",
            "memory_usage memory",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(
            context,
            "merges",
            None,
            "elapsed",
            &mut columns,
            3,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_mutations(&mut self, context: ContextArc) {
        let mut columns = vec![
            "database",
            "table",
            "mutation_id",
            "command",
            "create_time",
            "parts_to_do parts",
            "is_done",
            "latest_fail_reason",
            "latest_fail_time",
        ];

        // TODO:
        // - on_submit show last related log messages
        // - sort by create_time OR latest_fail_time
        self.show_query_result_view(
            context,
            "mutations",
            Some("is_done = 0"),
            "latest_fail_time",
            &mut columns,
            3,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_replication_queue(&mut self, context: ContextArc) {
        let mut columns = vec![
            "database",
            "table",
            "type",
            "new_part_name part",
            "create_time",
            "is_currently_executing executing",
            "num_tries tries",
            "last_exception exception",
            "num_postponed postponed",
            "postpone_reason reason",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(
            context,
            "replication_queue",
            None,
            "tries",
            &mut columns,
            3,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc) {
        let mut columns = vec![
            "database",
            "table",
            "result_part_name part",
            "elapsed",
            "progress",
            "total_size_bytes_compressed size",
            "bytes_read_compressed bytes",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(
            context,
            "replicated_fetches",
            None,
            "elapsed",
            &mut columns,
            3,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_replicas(&mut self, context: ContextArc) {
        let mut columns = vec![
            "database",
            "table",
            "is_readonly readonly",
            "parts_to_check",
            "queue_size queue",
            "absolute_delay delay",
            "last_queue_update last_update",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(
            context,
            "replicas",
            None,
            "queue",
            &mut columns,
            2,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_errors(&mut self, context: ContextArc) {
        let mut columns = vec![
            "name",
            "value",
            "last_error_time error_time",
            // "toValidUTF8(last_error_message) _error_message",
            "arrayStringConcat(arrayMap(addr -> concat(addressToLine(addr), '::', demangle(addressToSymbol(addr))), last_error_trace), '\n') _error_trace",
        ];

        // TODO: on submit show logs from system.query_log/system.text_log, but we need to
        // implement wrapping before
        self.show_query_result_view(
            context,
            "errors",
            None,
            "value",
            &mut columns,
            1,
            Some(|siv: &mut Cursive, row: view::QueryResultRow| {
                let trace = row.0.iter().last().unwrap();
                siv.add_layer(Dialog::info(trace.to_string()).title("Error trace"));
            }),
            &HashMap::from([("allow_introspection_functions", "1")]),
        );
    }

    fn show_clickhouse_backups(&mut self, context: ContextArc) {
        let mut columns = vec![
            "name",
            "status::String status",
            "error",
            "start_time",
            "end_time",
            "total_size",
        ];

        // TODO:
        // - order by elapsed time
        // - on submit - show log entries from text_log
        self.show_query_result_view(
            context,
            "backups",
            None,
            "total_size",
            &mut columns,
            1,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_dictionaries(&mut self, context: ContextArc) {
        let mut columns = vec![
            "name",
            "status::String status",
            "origin",
            "bytes_allocated memory",
            "query_count queries",
            "found_rate",
            "load_factor",
            "last_successful_update_time last_update",
            "loading_duration",
            "last_exception",
        ];

        self.show_query_result_view(
            context,
            "dictionaries",
            None,
            "memory",
            &mut columns,
            1,
            QUERY_RESULT_VIEW_NOP_CALLBACK,
            &HashMap::new(),
        );
    }

    fn show_clickhouse_server_logs(&mut self, context: ContextArc) {
        if self.has_view("server_logs") {
            return;
        }

        let view_options = context.clone().lock().unwrap().options.view.clone();

        self.drop_main_view();
        self.set_main_view(
            LinearLayout::vertical()
                .child(TextView::new("Server logs:").center())
                .child(DummyView.fixed_height(1))
                .child(
                    TextLogView::new(
                        "server_logs",
                        context,
                        view_options.start,
                        Some(view_options.end),
                        None,
                    )
                    .with_name("server_logs")
                    .full_screen(),
                ),
        );
        self.focus_name("server_logs").unwrap();
    }

    fn show_query_result_view<F>(
        &mut self,
        context: ContextArc,
        table: &'static str,
        filter: Option<&'static str>,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
        columns_to_compare: usize,
        on_submit: Option<F>,
        settings: &HashMap<&str, &str>,
    ) where
        F: Fn(&mut Cursive, view::QueryResultRow) + Send + Sync + 'static,
    {
        if self.has_view(table) {
            return;
        }

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        if cluster {
            columns.insert(0, "hostName() host");
        }

        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", table);
        let settings = if settings.is_empty() {
            "".to_string()
        } else {
            format!(
                " SETTINGS {}",
                settings
                    .iter()
                    .map(|kv| format!("{}='{}'", kv.0, kv.1.replace('\'', "\\\'")))
                    .collect::<Vec<String>>()
                    .join(",")
            )
            .to_string()
        };
        let query = format!(
            "select {} from {}{}{}",
            columns.join(", "),
            dbtable,
            filter.map(|x| format!(" WHERE {}", x)).unwrap_or_default(),
            settings,
        );

        self.drop_main_view();

        let mut view = view::QueryResultView::new(
            context.clone(),
            table,
            sort_by,
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get {}", table));
        if let Some(on_submit) = on_submit {
            view.set_on_submit(on_submit);
        }
        let view = view.with_name(table).full_screen();

        self.set_main_view(Dialog::around(view).title(table));
        self.focus_name(table).unwrap();
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
