use crate::{
    interpreter::{clickhouse::TraceType, ContextArc, WorkerEvent},
    view,
};
use chdig::{fuzzy_actions, shortcuts};
use cursive::{
    event::{Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle, Effect, PaletteColor, Style, Theme},
    utils::{markup::StyledString, span::SpannedString},
    view::View as _,
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
    text.append_styled("Actions", ColorStyle::highlight());

    return text;
}

pub trait Navigation {
    fn has_view(&mut self, name: &str) -> bool;

    fn make_theme_from_therminal(&mut self) -> Theme;
    fn pop_ui(&mut self);

    fn chdig(&mut self, context: ContextArc);

    fn show_help_dialog(&mut self);
    fn show_actions(&mut self);
    fn show_fuzzy_actions(&mut self);
    fn show_server_flamegraph(&mut self);

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
        let theme = self.make_theme_from_therminal();
        self.set_theme(theme);

        self.add_global_callback(Event::CtrlChar('p'), |siv| siv.show_fuzzy_actions());

        // TODO: add other variants of flamegraphs
        self.add_global_callback('F', |siv| siv.show_server_flamegraph());

        // NOTE: Do not bind pop_ui() to Esc, since this breaks other bindings (Home/End/...)
        self.add_global_callback(Key::Backspace, |siv| siv.pop_ui());
        self.add_global_callback('q', |siv| siv.pop_ui());

        self.add_global_callback(Key::F1, |siv| siv.show_help_dialog());
        self.add_global_callback(Key::F2, |siv| siv.show_actions());
        self.add_global_callback('~', toggle_flexi_logger_debug_console);
        self.set_user_data(context.clone());

        self.statusbar(format!(
            "Connected to {}.",
            context.lock().unwrap().server_version
        ));

        self.add_layer(
            LinearLayout::horizontal()
                .child(LinearLayout::vertical().with_name("actions"))
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

        text.append_styled("\nGeneral shortcuts:\n\n", Effect::Bold);
        for shortcut in shortcuts::GENERAL_SHORTCUTS.iter() {
            text.append(shortcut.preview_styled());
        }

        text.append_styled("\nQuery actions:\n\n", Effect::Bold);
        for shortcut in shortcuts::QUERY_SHORTCUTS.iter() {
            text.append(shortcut.preview_styled());
        }

        text.append_styled("\nGlobal server actions:\n\n", Effect::Bold);
        for shortcut in shortcuts::SERVER_SHORTCUTS.iter() {
            text.append(shortcut.preview_styled());
        }

        text.append_plain(format!(
            "\nIssues and suggestions: {homepage}/issues",
            homepage = env!("CARGO_PKG_HOMEPAGE")
        ));

        self.add_layer(Dialog::info(text).with_name("help"));
    }

    fn show_actions(&mut self) {
        let mut has_actions = false;
        self.call_on_name("actions", |actions_view: &mut LinearLayout| {
            if actions_view.len() > 0 {
                actions_view
                    .remove_child(actions_view.len() - 1)
                    .expect("No child view to remove");
            } else {
                let mut select = SelectView::new().autojump();
                select.set_on_submit(move |siv, selected_action: &str| {
                    let item = shortcuts::QUERY_SHORTCUTS
                        .iter()
                        .find(|x| x.text == selected_action)
                        .expect(&format!("No action {}", selected_action));
                    log::trace!("Triggering {:?} (from actions)", item.event);
                    siv.call_on_name("main", |main_view: &mut LinearLayout| {
                        main_view.on_event(item.event.clone());
                    });
                    siv.call_on_name("actions", |actions_view: &mut LinearLayout| {
                        actions_view
                            .remove_child(actions_view.len() - 1)
                            .expect("No child view to remove");
                    });
                });

                for action in shortcuts::QUERY_SHORTCUTS.iter() {
                    select.add_item_str(action.text);
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

                actions_view.add_child(select);

                has_actions = true;
            }
        });

        if has_actions {
            self.focus_name("actions").unwrap();
        } else {
            self.focus_name("main").unwrap();
        }
    }

    fn show_fuzzy_actions(&mut self) {
        let actions = shortcuts::GENERAL_SHORTCUTS
            .iter()
            .chain(shortcuts::QUERY_SHORTCUTS.iter())
            .chain(shortcuts::SERVER_SHORTCUTS.iter())
            .cloned()
            .collect();
        let event = fuzzy_actions(actions);
        log::trace!("Triggering {:?} (from fuzzy serach)", event);
        self.call_on_name("main", |main_view: &mut LinearLayout| {
            main_view.on_event(event);
        });
    }

    fn show_server_flamegraph(&mut self) {
        self.user_data::<ContextArc>()
            .unwrap()
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::ShowServerFlameGraph(TraceType::CPU));
    }

    fn set_main_view<V: IntoBoxedView + 'static>(&mut self, view: V) {
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

        self.set_main_view(
            Dialog::around(
                view::ProcessesView::new(context.clone(), WorkerEvent::UpdateProcessList)
                    .expect("Cannot get processlist")
                    .with_name("processes")
                    .full_screen(),
            )
            .title("Queries"),
        );
    }

    fn show_clickhouse_slow_query_log(&mut self, context: ContextArc) {
        if self.has_view("slow_query_log") {
            return;
        }

        self.set_main_view(
            Dialog::around(
                view::ProcessesView::new(context.clone(), WorkerEvent::UpdateSlowQueryLog)
                    .expect("Cannot get slow query log")
                    .with_name("slow_query_log")
                    .full_screen(),
            )
            .title("Slow queries"),
        );
    }

    fn show_clickhouse_last_query_log(&mut self, context: ContextArc) {
        if self.has_view("last_query_log") {
            return;
        }

        self.set_main_view(
            Dialog::around(
                view::ProcessesView::new(context.clone(), WorkerEvent::UpdateLastQueryLog)
                    .expect("Cannot get last query log")
                    .with_name("last_query_log")
                    .full_screen(),
            )
            .title("Last queries"),
        );
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

        self.set_main_view(
            Dialog::around(
                view::QueryResultView::new(context.clone(), table, sort_by, columns.clone(), query)
                    .expect(&format!("Cannot get {}", table))
                    .with_name(table)
                    .full_screen(),
            )
            .title(table),
        );
    }
}
