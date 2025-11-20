use crate::utils::fuzzy_actions;
use crate::{
    common::{RelativeDateTime, parse_datetime_or_date},
    interpreter::{
        ClickHouseAvailableQuirks, ContextArc, WorkerEvent, clickhouse::TraceType,
        options::ChDigViews,
    },
    view::{self, TextLogView},
};
use anyhow::Result;
use chrono::{DateTime, Duration, Local};
use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode},
};
use cursive::{
    Cursive, Rect, Vec2,
    event::{Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle, Effect, PaletteColor, Style, Theme},
    utils::{markup::StyledString, span::SpannedString},
    view::{IntoBoxedView, Nameable, Resizable, View},
    views::{
        Dialog, DummyView, EditView, FixedLayout, Layer, LinearLayout, NamedView, OnEventView,
        OnLayoutView, SelectView, TextContent, TextView,
    },
};
use cursive_flexi_logger_view::toggle_flexi_logger_debug_console;
use percent_encoding::percent_decode;
use std::collections::HashMap;
use std::io;
use strfmt::strfmt;

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

    fn show_clickhouse_processes(&mut self, context: ContextArc);
    fn show_clickhouse_slow_query_log(&mut self, context: ContextArc);
    fn show_clickhouse_last_query_log(&mut self, context: ContextArc);
    fn show_clickhouse_merges(&mut self, context: ContextArc);
    fn show_clickhouse_mutations(&mut self, context: ContextArc);
    fn show_clickhouse_replication_queue(&mut self, context: ContextArc);
    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc);
    fn show_clickhouse_replicas(&mut self, context: ContextArc);
    fn show_clickhouse_tables(&mut self, context: ContextArc);
    fn show_clickhouse_errors(&mut self, context: ContextArc);
    fn show_clickhouse_backups(&mut self, context: ContextArc);
    fn show_clickhouse_dictionaries(&mut self, context: ContextArc);
    fn show_clickhouse_s3queue(&mut self, context: ContextArc);
    fn show_clickhouse_server_logs(&mut self, context: ContextArc);
    fn show_clickhouse_logger_names(&mut self, context: ContextArc);
    fn show_clickhouse_client(&mut self, context: ContextArc);

    #[allow(clippy::too_many_arguments)]
    fn show_query_result_view<F>(
        &mut self,
        context: ContextArc,
        table: &'static str,
        join: Option<String>,
        filter: Option<&'static str>,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
        columns_to_compare: usize,
        on_submit: Option<F>,
        settings: &HashMap<&str, &str>,
    ) where
        F: Fn(&mut Cursive, Vec<&'static str>, view::QueryResultRow) + Send + Sync + 'static;

    // TODO: move into separate trait
    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: View,
        F: FnOnce(&mut V) -> Result<()>;
}

fn query_result_show_row(siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow) {
    let row = row.0;
    let width = columns.iter().map(|c| c.len()).max().unwrap_or_default();
    let info = columns
        .iter()
        .zip(row.iter())
        .map(|(c, r)| (*c, r.to_string()))
        .map(|(c, r)| format!("{:<width$}: {}", c, r, width = width))
        .collect::<Vec<_>>()
        .join("\n");
    siv.add_layer(Dialog::info(info).title("Details"));
}

fn query_result_show_logs_for_row(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
    logger_names_patterns: &[&'static str],
    view_name: &'static str,
) {
    let row = row.0;

    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row.iter()).for_each(|(c, r)| {
        map.insert(c.to_string(), r.to_string());
    });

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.clone().lock().unwrap().options.view.clone();
    let logger_names = logger_names_patterns
        .iter()
        .map(|p| strfmt(p, &map).unwrap())
        .collect::<Vec<_>>();

    siv.add_layer(Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Logs:").center())
            .child(DummyView.fixed_height(1))
            .child(NamedView::new(
                view_name,
                TextLogView::new(
                    view_name,
                    context,
                    DateTime::<Local>::from(view_options.start),
                    view_options.end,
                    None,
                    Some(logger_names),
                    None,
                    None,
                ),
            )),
    ));
    siv.focus_name(view_name).unwrap();
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
            .clone()
            .unwrap_or(ChDigViews::Queries);
        match start_view {
            ChDigViews::Queries => self.show_clickhouse_processes(context.clone()),
            ChDigViews::LastQueries => self.show_clickhouse_last_query_log(context.clone()),
            ChDigViews::SlowQueries => self.show_clickhouse_slow_query_log(context.clone()),
            ChDigViews::Merges => self.show_clickhouse_merges(context.clone()),
            ChDigViews::S3Queue => self.show_clickhouse_s3queue(context.clone()),
            ChDigViews::Mutations => self.show_clickhouse_mutations(context.clone()),
            ChDigViews::ReplicationQueue => self.show_clickhouse_replication_queue(context.clone()),
            ChDigViews::ReplicatedFetches => {
                self.show_clickhouse_replicated_fetches(context.clone())
            }
            ChDigViews::Replicas => self.show_clickhouse_replicas(context.clone()),
            ChDigViews::Tables => self.show_clickhouse_tables(context.clone()),
            ChDigViews::Errors => self.show_clickhouse_errors(context.clone()),
            ChDigViews::Backups => self.show_clickhouse_backups(context.clone()),
            ChDigViews::Dictionaries => self.show_clickhouse_dictionaries(context.clone()),
            ChDigViews::ServerLogs => self.show_clickhouse_server_logs(context.clone()),
            ChDigViews::Loggers => self.show_clickhouse_logger_names(context.clone()),
            ChDigViews::Client => self.show_clickhouse_client(context.clone()),
        }
    }

    fn initialize_global_shortcuts(&mut self, context: ContextArc) {
        let mut context = context.lock().unwrap();

        context.add_global_action(self, "Show help", Key::F1, |siv| siv.show_help_dialog());

        context.add_global_action(self, "Views", Key::F2, |siv| siv.show_views());
        context.add_global_action(self, "Show actions", Key::F8, |siv| siv.show_actions());
        context.add_global_action(self, "Fuzzy actions", Event::CtrlChar('p'), |siv| {
            siv.show_fuzzy_actions()
        });

        context.add_global_action(self, "CPU Server Flamegraph", 'F', |siv| {
            siv.show_server_flamegraph(true, Some(TraceType::CPU))
        });
        context.add_global_action_without_shortcut(self, "Real Server Flamegraph", |siv| {
            siv.show_server_flamegraph(true, Some(TraceType::Real))
        });
        context.add_global_action_without_shortcut(self, "Memory Server Flamegraph", |siv| {
            siv.show_server_flamegraph(true, Some(TraceType::Memory))
        });
        context.add_global_action_without_shortcut(
            self,
            "Memory Sample Server Flamegraph",
            |siv| siv.show_server_flamegraph(true, Some(TraceType::MemorySample)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Jemalloc Sample Server Flamegraph",
            |siv| siv.show_server_flamegraph(true, Some(TraceType::JemallocSample)),
        );
        context.add_global_action_without_shortcut(self, "Events Server Flamegraph", |siv| {
            siv.show_server_flamegraph(true, Some(TraceType::ProfileEvents))
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
            "Memory Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, Some(TraceType::Memory)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Memory Sample Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, Some(TraceType::MemorySample)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Jemalloc Sample Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, Some(TraceType::JemallocSample)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Events Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, Some(TraceType::ProfileEvents)),
        );
        context.add_global_action_without_shortcut(
            self,
            "Live Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false, None),
        );

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
        context.add_global_action(self, "Toggle pause", 'p', |siv| {
            siv.toggle_pause_updates(None)
        });
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
            c.add_view("S3Queue", move |siv| {
                siv.show_clickhouse_s3queue(ctx.clone())
            });
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
            c.add_view("Tables", move |siv| siv.show_clickhouse_tables(ctx.clone()));
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
            c.add_view("Loggers", move |siv| {
                siv.show_clickhouse_logger_names(ctx.clone())
            });
        }
        {
            let ctx = context.clone();
            c.add_view("Errors", move |siv| siv.show_clickhouse_errors(ctx.clone()));
        }
        {
            let ctx = context.clone();
            c.add_view("Client", move |siv| siv.show_clickhouse_client(ctx.clone()));
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
            "now()-elapsed _create_time",
            "tables.uuid::String _table_uuid",
        ];

        let merges_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                let mut map = HashMap::new();
                columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r);
                });

                let context = siv.user_data::<ContextArc>().unwrap().clone();
                siv.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new("Logs:").center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "merge_logs",
                            TextLogView::new(
                                "merge_logs",
                                context,
                                map["_create_time"].as_datetime().unwrap(),
                                RelativeDateTime::new(None),
                                Some(vec![format!(
                                    "{}::{}",
                                    map["_table_uuid"].to_string(),
                                    map["part"].to_string()
                                )]),
                                None,
                                None,
                                None,
                            ),
                        )),
                ));
                siv.focus_name("merge_logs").unwrap();
            };

        let tables_dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "tables");
        self.show_query_result_view(
            context,
            "merges",
            Some(format!(
                "left join (select distinct on (database, name) database, name, uuid from {}) tables on merges.database = tables.database and merges.table = tables.name",
                tables_dbtable
            )),
            None,
            "elapsed",
            &mut columns,
            3,
            Some(merges_logs_callback),
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
        // - on_submit show assigned merges (but first, need to expose enough info in system tables)
        // - sort by create_time OR latest_fail_time
        self.show_query_result_view(
            context,
            "mutations",
            None,
            Some("is_done = 0"),
            "latest_fail_time",
            &mut columns,
            3,
            Some(query_result_show_row),
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
            None,
            "tries",
            &mut columns,
            3,
            Some(query_result_show_row),
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
            None,
            "elapsed",
            &mut columns,
            3,
            Some(query_result_show_row),
            &HashMap::new(),
        );
    }

    fn show_clickhouse_replicas(&mut self, context: ContextArc) {
        if self.has_view("replicas") {
            return;
        }

        let has_uuid = context
            .clone()
            .lock()
            .unwrap()
            .clickhouse
            .quirks
            .has(ClickHouseAvailableQuirks::SystemReplicasUUID);
        let mut columns = vec![
            "database",
            "table",
            "is_readonly readonly",
            "parts_to_check",
            "queue_size queue",
            "absolute_delay delay",
            "last_queue_update last_update",
        ];

        if has_uuid {
            columns.push("uuid::String _uuid");
        }

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let mut columns_to_compare = 2;
        if cluster {
            columns.insert(0, "hostName() host");
            columns_to_compare += 1;
        }

        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "replicas");

        let query = format!(
            "SELECT DISTINCT ON (database, table, zookeeper_path) {} FROM {} ORDER BY queue_size DESC, database, table",
            columns.join(", "),
            dbtable,
        );

        self.drop_main_view();

        let mut view = view::QueryResultView::new(
            context.clone(),
            "replicas",
            "queue",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get replicas"));

        // TODO: proper escape of _/%
        let logger_names_patterns = if has_uuid {
            vec!["{database}.{table} ({_uuid})"]
        } else {
            vec!["{database}.{table} %"]
        };
        let replicas_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                query_result_show_logs_for_row(
                    siv,
                    columns,
                    row,
                    &logger_names_patterns,
                    "replica_logs",
                );
            };
        view.set_on_submit(replicas_logs_callback);

        let view = view.with_name("replicas").full_screen();
        self.set_main_view(view);
    }

    fn show_clickhouse_tables(&mut self, context: ContextArc) {
        if self.has_view("tables") {
            return;
        }

        let mut columns = vec![
            "database",
            "table",
            "uuid::String _uuid",
            "assumeNotNull(total_bytes) total_bytes",
            "assumeNotNull(total_rows) total_rows",
            // TODO: support number of background jobs counter in ClickHouse
        ];

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let mut columns_to_compare = 2;
        if cluster {
            columns.insert(0, "hostName() host");
            columns_to_compare += 1;
        }

        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "tables");

        let query = format!(
            "SELECT DISTINCT ON (database, table, uuid) {} FROM {} WHERE engine NOT LIKE 'System%' AND database NOT IN ('INFORMATION_SCHEMA', 'information_schema') ORDER BY database, table, total_bytes DESC",
            columns.join(", "),
            dbtable,
        );

        self.drop_main_view();

        let mut view = view::QueryResultView::new(
            context.clone(),
            "tables",
            "total_bytes",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get tables"));

        // TODO: proper escape of _/%
        let logger_names_patterns = vec!["%{database}.{table}%", "%{_uuid}%"];
        let tables_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                query_result_show_logs_for_row(
                    siv,
                    columns,
                    row,
                    &logger_names_patterns,
                    "table_logs",
                );
            };
        view.set_on_submit(tables_logs_callback);

        let view = view.with_name("tables").full_screen();
        self.set_main_view(view);
    }

    fn show_clickhouse_errors(&mut self, context: ContextArc) {
        let mut columns = vec![
            "name",
            "value",
            "last_error_time error_time",
            // "toValidUTF8(last_error_message) _error_message",
            "arrayStringConcat(arrayMap(addr -> concat(addressToLine(addr), '::', demangle(addressToSymbol(addr))), last_error_trace), '\n') _error_trace",
        ];

        let errors_logs_callback =
            |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                let row_data = row.0;

                let mut map = HashMap::<String, String>::new();
                columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r.to_string());
                });

                let error_time = map
                    .get("error_time")
                    .and_then(|t| t.parse::<DateTime<Local>>().ok())
                    .unwrap_or_else(Local::now);
                let error_name = map.get("name").map(|s| s.to_string()).unwrap_or_default();

                let context = siv.user_data::<ContextArc>().unwrap().clone();

                // Show logs for 1 minute before and after the error time
                // (Note, we need to add at least 1 second to error_time, otherwise it will be
                // filtered out by event_time_microseconds condition)
                let offset = Duration::try_minutes(1).unwrap_or_default();
                let end_time = error_time + offset;
                let start_time = error_time - offset;

                siv.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new(format!("Logs for error: {}", error_name)).center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "error_logs",
                            TextLogView::new(
                                "error_logs",
                                context,
                                start_time,
                                RelativeDateTime::from(end_time),
                                None,
                                None,
                                Some(error_name),
                                Some("Warning".to_string()),
                            ),
                        )),
                ));
                siv.focus_name("error_logs").unwrap();
            };

        self.show_query_result_view(
            context,
            "errors",
            None,
            None,
            "value",
            &mut columns,
            1,
            Some(errors_logs_callback),
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
            "query_id _query_id",
        ];

        let backups_logs_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                let mut map = HashMap::new();
                columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r);
                });

                let context = siv.user_data::<ContextArc>().unwrap().clone();
                siv.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new("Logs:").center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "backups_logs",
                            TextLogView::new(
                                "backups_logs",
                                context,
                                map["start_time"].as_datetime().unwrap(),
                                RelativeDateTime::from(map["end_time"].as_datetime()),
                                Some(vec![map["_query_id"].to_string()]),
                                None,
                                None,
                                None,
                            ),
                        )),
                ));
                siv.focus_name("backups_logs").unwrap();
            };

        // TODO:
        // - order by elapsed time
        self.show_query_result_view(
            context,
            "backups",
            None,
            None,
            "total_size",
            &mut columns,
            1,
            Some(backups_logs_callback),
            &HashMap::new(),
        );
    }

    fn show_clickhouse_dictionaries(&mut self, context: ContextArc) {
        let mut columns = vec![
            "name",
            "status::String status",
            "source",
            "bytes_allocated memory",
            "query_count queries",
            "found_rate",
            "load_factor",
            "last_successful_update_time last_update",
            "loading_duration",
            "last_exception",
            "origin",
        ];

        self.show_query_result_view(
            context,
            "dictionaries",
            None,
            None,
            "memory",
            &mut columns,
            1,
            Some(query_result_show_row),
            &HashMap::new(),
        );
    }

    fn show_clickhouse_s3queue(&mut self, context: ContextArc) {
        let mut columns = vec![
            "file_name",
            "rows_processed",
            "status",
            "assumeNotNull(processing_start_time) start_time",
            "exception",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(
            context,
            "s3queue",
            None,
            None,
            "start_time",
            &mut columns,
            1,
            Some(query_result_show_row),
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
                        DateTime::<Local>::from(view_options.start),
                        view_options.end,
                        None,
                        None,
                        None,
                        None,
                    )
                    .with_name("server_logs")
                    .full_screen(),
                ),
        );
        self.focus_name("server_logs").unwrap();
    }

    fn show_clickhouse_logger_names(&mut self, context: ContextArc) {
        if self.has_view("logger_names") {
            return;
        }

        let view_options = context.lock().unwrap().options.view.clone();
        let start = DateTime::<Local>::from(view_options.start);
        let end = view_options.end;

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let mut columns = vec![
            "logger_name::String logger_name",
            "count() count",
            "countIf(level = 'Fatal') fatal",
            "countIf(level = 'Critical') critical",
            "countIf(level = 'Error') error",
            "countIf(level = 'Warning') warning",
            "countIf(level = 'Notice') notice",
            "countIf(level = 'Information') information",
            "countIf(level = 'Debug') debug",
            "countIf(level = 'Trace') trace",
        ];
        let mut columns_to_compare = 1;

        if cluster {
            columns.insert(0, "hostName() host");
            columns_to_compare = 2;
        }

        let logger_names_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                let row = row.0;
                let mut map = HashMap::<String, String>::new();
                columns.iter().zip(row.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r.to_string());
                });

                let logger_name = map.get("logger_name").unwrap().clone();
                let context = siv.user_data::<ContextArc>().unwrap().clone();
                let view_options = context.lock().unwrap().options.view.clone();

                siv.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new(format!("Logs for logger: {}", logger_name)).center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "logger_logs",
                            TextLogView::new(
                                "logger_logs",
                                context,
                                DateTime::<Local>::from(view_options.start),
                                view_options.end,
                                None,
                                Some(vec![logger_name]),
                                None,
                                None,
                            ),
                        )),
                ));
                siv.focus_name("logger_logs").unwrap();
            };

        // Build the query with time filtering
        let dbtable = context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", "text_log");

        let start_nanos = start
            .timestamp_nanos_opt()
            .ok_or(anyhow::anyhow!("Invalid start time"))
            .unwrap();
        let end_datetime = end.to_sql_datetime_64().unwrap_or_default();

        let query = format!(
            r#"
            WITH
                fromUnixTimestamp64Nano({}) AS start_time_,
                {} AS end_time_
            SELECT {}
            FROM {}
            WHERE
                event_date >= toDate(start_time_) AND event_time >= toDateTime(start_time_) AND event_time_microseconds > start_time_
                AND event_date <= toDate(end_time_) AND event_time <= toDateTime(end_time_) AND event_time_microseconds <= end_time_
            GROUP BY {}
            ORDER BY count DESC
            LIMIT {}
            "#,
            start_nanos,
            end_datetime,
            columns.join(", "),
            dbtable,
            if cluster {
                "host, logger_name"
            } else {
                "logger_name"
            },
            context.lock().unwrap().options.clickhouse.limit,
        );

        self.drop_main_view();

        let mut view = view::QueryResultView::new(
            context.clone(),
            "logger_names",
            "count",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get logger_names"));
        view.set_on_submit(logger_names_callback);
        let view = view.with_name("logger_names").full_screen();

        self.set_main_view(Dialog::around(view).title("Loggers"));
        self.focus_name("logger_names").unwrap();
    }

    fn show_query_result_view<F>(
        &mut self,
        context: ContextArc,
        table: &'static str,
        join: Option<String>,
        filter: Option<&'static str>,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
        mut columns_to_compare: usize,
        on_submit: Option<F>,
        settings: &HashMap<&str, &str>,
    ) where
        F: Fn(&mut Cursive, Vec<&'static str>, view::QueryResultRow) + Send + Sync + 'static,
    {
        if self.has_view(table) {
            return;
        }

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        if cluster {
            columns.insert(0, "hostName() host");
            columns_to_compare += 1;
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
            "select {} from {} as {} {}{}{}",
            columns.join(", "),
            dbtable,
            table,
            join.unwrap_or_default(),
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

    fn show_clickhouse_client(&mut self, context: ContextArc) {
        use std::process::Command;
        use std::str::FromStr;

        let options = context.lock().unwrap().options.clickhouse.clone();

        let mut cmd = Command::new("clickhouse");
        cmd.arg("client");

        if let Some(config) = &options.config {
            cmd.arg("--config").arg(config);
        }

        if let Some(url) = &options.url
            && let Ok(url) = url::Url::parse(url)
        {
            if let Some(host) = &url.host() {
                cmd.arg("--host").arg(host.to_string());
            }
            if let Some(port) = &url.port() {
                cmd.arg("--port").arg(port.to_string());
            }
            if !url.username().is_empty() {
                cmd.arg("--user").arg(url.username());
            }
            if let Some(password) = &url.password() {
                cmd.arg("--password").arg(
                    percent_decode(password.as_bytes())
                        .decode_utf8_lossy()
                        .to_string(),
                );
            }

            let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
            if let Some(skip_verify) = pairs
                .get("skip_verify")
                .and_then(|v| bool::from_str(v).ok())
                && skip_verify
            {
                cmd.arg("--accept-invalid-certificate");
            }
            if pairs
                .get("secure")
                .and_then(|v| bool::from_str(v).ok())
                .unwrap_or_default()
            {
                cmd.arg("--secure");
            }
        }

        disable_raw_mode().unwrap();
        execute!(
            io::stdout(),
            crossterm::event::DisableMouseCapture,
            crossterm::style::ResetColor,
            crossterm::style::SetAttribute(crossterm::style::Attribute::Reset),
            crossterm::terminal::Clear(crossterm::terminal::ClearType::All),
            crossterm::cursor::MoveTo(0, 0)
        )
        .unwrap();

        let cb_sink = self.cb_sink().clone();
        let cmd_line = format!("{:?}", cmd);
        log::info!("Spawning client: {}", cmd_line);

        match cmd.status() {
            Ok(status) => {
                cb_sink
                    .send(Box::new(move |siv| {
                        siv.clear();
                        if !status.success() {
                            siv.add_layer(Dialog::info(format!(
                                "clickhouse client exited with status: {}\n\nCommand: {}",
                                status, cmd_line
                            )));
                        }
                    }))
                    .ok();
            }
            Err(err) => {
                cb_sink.send(Box::new(move |siv| {
                    siv.clear();
                    siv.add_layer(Dialog::info(format!(
                        "Failed to spawn clickhouse client: {}\n\nCommand: {}\n\nMake sure clickhouse is installed and in PATH",
                        err, cmd_line
                    )));
                })).ok();
            }
        }

        enable_raw_mode().unwrap();
        execute!(
            io::stdout(),
            crossterm::event::EnableMouseCapture,
            crossterm::terminal::Clear(crossterm::terminal::ClearType::All)
        )
        .unwrap();

        // Force a full redraw of the screen
        self.clear();

        log::info!("Client terminated. Raw mode and mouse capture enabled.");
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
