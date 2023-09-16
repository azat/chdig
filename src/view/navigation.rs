use crate::{
    interpreter::{clickhouse::TraceType, ContextArc, WorkerEvent},
    view,
};
use anyhow::Result;
use chdig::fuzzy_actions;
use cursive::{
    event::{Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle, Effect, PaletteColor, Style, Theme},
    utils::{markup::StyledString, span::SpannedString},
    view::View,
    view::{IntoBoxedView, Nameable, Resizable},
    views::{
        Dialog, DummyView, FixedLayout, Layer, LinearLayout, OnEventView, OnLayoutView, SelectView,
        TextContent, TextView,
    },
    Cursive, {Rect, Vec2},
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
    fn pop_ui(&mut self);

    fn initialize_global_shortcuts(&mut self, context: ContextArc);
    fn initialize_views_menu(&mut self, context: ContextArc);
    fn chdig(&mut self, context: ContextArc);

    fn show_help_dialog(&mut self);
    fn show_views(&mut self);
    fn show_actions(&mut self);
    fn show_fuzzy_actions(&mut self);
    fn show_server_flamegraph(&mut self, tui: bool);

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

    fn show_query_result_view(
        &mut self,
        context: ContextArc,
        table: &'static str,
        filter: Option<&'static str>,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
    );

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

    fn pop_ui(&mut self) {
        // - main view
        // - statusbar
        if self.screen_mut().len() == 2 {
            self.quit();
        } else {
            self.pop_layer();
        }
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
                        .child(TextView::new(make_menu_text()))
                        .child(view::SummaryView::new(context.clone()).with_name("summary"))
                        .with_name("main"),
                ),
        );

        self.show_clickhouse_processes(context.clone());
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
            siv.show_server_flamegraph(true)
        });
        context.add_global_action_without_shortcut(
            self,
            "CPU Server Flamegraph in speedscope",
            |siv| siv.show_server_flamegraph(false),
        );

        context.add_global_action(
            self,
            "chdig debug console",
            '~',
            toggle_flexi_logger_debug_console,
        );
        context.add_global_action(self, "Back/Quit", 'q', |siv| siv.pop_ui());
        context.add_global_action(self, "Back/Quit", Key::Backspace, |siv| siv.pop_ui());
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
            if left_menu_view.len() > 0 {
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
            if left_menu_view.len() > 0 {
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
                    if context.view_actions.len() == 0 {
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

    fn show_server_flamegraph(&mut self, tui: bool) {
        self.user_data::<ContextArc>()
            .unwrap()
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::ShowServerFlameGraph(tui, TraceType::CPU));
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
        let table = "system.merges";
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
        self.show_query_result_view(context, table, None, "elapsed", &mut columns);
    }

    fn show_clickhouse_mutations(&mut self, context: ContextArc) {
        let table = "system.mutations";
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
            table,
            Some(&"is_done = 0"),
            "latest_fail_time",
            &mut columns,
        );
    }

    fn show_clickhouse_replication_queue(&mut self, context: ContextArc) {
        let table = "system.replication_queue";
        let mut columns = vec![
            "database",
            "table",
            "create_time",
            "new_part_name part",
            "is_currently_executing executing",
            "num_tries tries",
            "last_exception exception",
            "num_postponed postponed",
            "postpone_reason reason",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(context, table, None, "tries", &mut columns);
    }

    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc) {
        let table = "system.replicated_fetches";
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
        self.show_query_result_view(context, table, None, "elapsed", &mut columns);
    }

    fn show_clickhouse_replicas(&mut self, context: ContextArc) {
        let table = "system.replicas";
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
        self.show_query_result_view(context, table, None, "queue", &mut columns);
    }

    fn show_clickhouse_errors(&mut self, context: ContextArc) {
        let table = "system.errors";
        let mut columns = vec![
            "name",
            "value",
            "last_error_time error_time",
            // TODO: on_submit show:
            // - last_error_message
            // - last_error_trace
        ];

        self.show_query_result_view(context, table, None, "value", &mut columns);
    }

    fn show_clickhouse_backups(&mut self, context: ContextArc) {
        let table = "system.backups";
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
        self.show_query_result_view(context, table, None, "total_size", &mut columns);
    }

    fn show_query_result_view(
        &mut self,
        context: ContextArc,
        table: &'static str,
        filter: Option<&'static str>,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
    ) {
        if self.has_view(table) {
            return;
        }

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        if cluster {
            columns.insert(0, "hostName() host");
        }

        let dbtable = context.lock().unwrap().clickhouse.get_table_name(table);
        let query = format!(
            "select {} from {}{}",
            columns.join(", "),
            dbtable,
            filter
                .and_then(|x| Some(format!(" WHERE {}", x)))
                .unwrap_or_default()
        );

        self.drop_main_view();
        self.set_main_view(
            Dialog::around(
                view::QueryResultView::new(context.clone(), table, sort_by, columns.clone(), query)
                    .expect(&format!("Cannot get {}", table))
                    .with_name(table)
                    .full_screen(),
            )
            .title(table),
        );
        self.focus_name(table).unwrap();
    }

    fn call_on_name_or_render_error<V, F>(&mut self, name: &str, callback: F)
    where
        V: View,
        F: FnOnce(&mut V) -> Result<()>,
    {
        let ret = self.call_on_name(name, callback);
        if let Some(val) = ret {
            if let Err(err) = val {
                self.add_layer(Dialog::info(err.to_string()));
            }
        }
    }
}
