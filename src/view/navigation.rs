use crate::utils::fuzzy_actions;
use crate::{
    common::parse_datetime_or_date,
    interpreter::{ContextArc, WorkerEvent, clickhouse::TraceType, options::ChDigViews},
    view,
};
use anyhow::Result;
use chrono::{DateTime, Local};
use cursive::{
    Cursive,
    event::{Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle, Effect, PaletteColor, Style, Theme},
    utils::{markup::StyledString, span::SpannedString},
    view::{IntoBoxedView, Nameable, Resizable, View},
    views::{
        Checkbox, Dialog, DummyView, EditView, LinearLayout, OnEventView, ScrollView, SelectView,
        TextView,
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
    // F3
    text.append_plain("F3");
    text.append_styled("Settings", ColorStyle::highlight());
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
    fn show_settings_dialog(&mut self);
    fn show_views(&mut self);
    fn show_actions(&mut self);
    fn show_fuzzy_actions(&mut self);
    fn show_server_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>);
    fn show_jemalloc_flamegraph(&mut self, tui: bool);
    fn show_server_perfetto(&mut self);
    fn show_connection_dialog(&mut self);

    fn drop_main_view(&mut self);
    fn set_main_view<V: IntoBoxedView + 'static>(&mut self, view: V);

    fn set_statusbar_version(&mut self, main_content: impl Into<SpannedString<Style>>);
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>);
    fn set_statusbar_connection(&mut self, content: impl Into<SpannedString<Style>>);

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

        if self.screen_mut().len() == 1 {
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
                        .child(TextView::new(
                            "format: YYYY-MM-DDTHH:MM:SS[.ssssss][±hh:mm|Z]",
                        ))
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

        self.add_fullscreen_layer(
            LinearLayout::horizontal()
                .child(LinearLayout::vertical().with_name("left_menu"))
                .child(
                    LinearLayout::vertical()
                        .child(
                            LinearLayout::horizontal()
                                .child(TextView::new(make_menu_text()))
                                .child(TextView::new("").with_name("is_paused"))
                                // Align status to the right
                                .child(DummyView.full_width())
                                .child(TextView::new("").with_name("status"))
                                .child(DummyView.fixed_width(1))
                                .child(TextView::new("").with_name("connection"))
                                .child(DummyView.fixed_width(1))
                                .child(TextView::new("").with_name("version")),
                        )
                        .child(view::SummaryView::new(context.clone()).with_name("summary"))
                        .with_name("main"),
                ),
        );

        {
            let ctx = context.lock().unwrap();
            self.set_statusbar_version(ctx.server_version.clone());
            self.set_statusbar_connection(ctx.options.clickhouse.connection_info());
        }

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
        context.add_global_action(self, "Settings", Key::F3, |siv| siv.show_settings_dialog());

        context.add_global_action(self, "Views", Key::F2, |siv| siv.show_views());
        context.add_global_action(self, "Show actions", Key::F8, |siv| siv.show_actions());
        context.add_global_action(self, "Fuzzy actions", Event::CtrlChar('p'), |siv| siv.show_fuzzy_actions());

        if context.options.clickhouse.cluster.is_some() {
            context.add_global_action(self, "Filter by host", Event::CtrlChar('h'), |siv| siv.show_connection_dialog());
        }

        context.add_global_action(self, "Server CPU Flamegraph", 'F', |siv| siv.show_server_flamegraph(true, Some(TraceType::CPU)));
        context.add_global_action_without_shortcut(self, "Server Real Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::Real)));
        context.add_global_action_without_shortcut(self, "Server Memory Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::Memory)));
        context.add_global_action_without_shortcut(self, "Server Memory Sample Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::MemorySample)));
        context.add_global_action_without_shortcut(self, "Server Jemalloc Sample Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::JemallocSample)));
        context.add_global_action_without_shortcut(self, "Server MemoryAllocatedWithoutCheck Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::MemoryAllocatedWithoutCheck)));
        context.add_global_action_without_shortcut(self, "Server Events Flamegraph", |siv| siv.show_server_flamegraph(true, Some(TraceType::ProfileEvent)));
        context.add_global_action_without_shortcut(self, "Server Live Flamegraph", |siv| siv.show_server_flamegraph(true, None));
        context.add_global_action_without_shortcut(self, "Share Server CPU Flamegraph", |siv| siv.show_server_flamegraph(false, Some(TraceType::CPU)));
        context.add_global_action_without_shortcut(self, "Share Server Real Flamegraph", |siv| siv.show_server_flamegraph(false, Some(TraceType::Real)));
        context.add_global_action_without_shortcut(self, "Share Server Memory Flamegraph", |siv| siv.show_server_flamegraph(false, Some(TraceType::Memory)));
        context.add_global_action_without_shortcut(self, "Share Server Memory Sample Flamegraph", |siv| siv.show_server_flamegraph(false, Some(TraceType::MemorySample)));
        context.add_global_action_without_shortcut(self, "Share Server MemoryAllocatedWithoutCheck Flamegraph", |siv| siv.show_server_flamegraph(false, Some(TraceType::MemoryAllocatedWithoutCheck)));
        context.add_global_action_without_shortcut(self, "Share Server Events Flamegraph", |siv| siv.show_server_flamegraph(false, Some(TraceType::ProfileEvent)));
        context.add_global_action_without_shortcut(self, "Share Server Live Flamegraph", |siv| siv.show_server_flamegraph(false, None));
        context.add_global_action_without_shortcut(self, "Jemalloc", |siv| siv.show_jemalloc_flamegraph(true));
        context.add_global_action_without_shortcut(self, "Share Jemalloc", |siv| siv.show_jemalloc_flamegraph(false));
        context.add_global_action_without_shortcut(self, "Server Perfetto Export", |siv| siv.show_server_perfetto());

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
        c.register_provider(Arc::new(AzureQueueViewProvider));
        c.register_provider(Arc::new(MutationsViewProvider));
        c.register_provider(Arc::new(ReplicatedFetchesViewProvider));
        c.register_provider(Arc::new(ReplicationQueueViewProvider));
        c.register_provider(Arc::new(ReplicasViewProvider));
        c.register_provider(Arc::new(TablesViewProvider));
        c.register_provider(Arc::new(BackgroundSchedulePoolViewProvider));
        c.register_provider(Arc::new(BackgroundSchedulePoolLogViewProvider));
        c.register_provider(Arc::new(TablePartsViewProvider));
        c.register_provider(Arc::new(AsynchronousInsertsViewProvider));
        c.register_provider(Arc::new(PartLogViewProvider));
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

    fn show_settings_dialog(&mut self) {
        if self.has_view("settings") {
            self.pop_layer();
            return;
        }

        let context = self.user_data::<ContextArc>().unwrap().clone();
        let (opts, server_version, selected_host, current_view) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.clone(),
                ctx.server_version.clone(),
                ctx.selected_host.clone(),
                ctx.current_view,
            )
        };

        let bold = |s: &str| TextView::new(StyledString::styled(s, Effect::Bold));
        let checkbox_row = |label: &str, name: &str, checked: bool| {
            LinearLayout::horizontal()
                .child(DummyView.fixed_width(2))
                .child(Checkbox::new().with_checked(checked).with_name(name))
                .child(TextView::new(format!(" {}", label)))
        };
        let edit_row = |label: &str, name: &str, value: &str, width: usize| {
            LinearLayout::horizontal()
                .child(TextView::new(format!("  {}: ", label)))
                .child(
                    EditView::new()
                        .content(value)
                        .with_name(name)
                        .fixed_width(width),
                )
        };

        let mut layout = LinearLayout::vertical();

        // ClickHouse
        layout.add_child(bold("ClickHouse:"));
        layout.add_child(TextView::new(format!(
            "  url: {}",
            opts.clickhouse.url_safe
        )));
        if let Some(ref cluster) = opts.clickhouse.cluster {
            layout.add_child(TextView::new(format!("  cluster: {}", cluster)));
        }
        layout.add_child(checkbox_row(
            "history",
            "set_history",
            opts.clickhouse.history,
        ));
        layout.add_child(checkbox_row(
            "internal_queries",
            "set_internal_queries",
            opts.clickhouse.internal_queries,
        ));
        layout.add_child(edit_row(
            "limit",
            "set_limit",
            &opts.clickhouse.limit.to_string(),
            12,
        ));
        layout.add_child(checkbox_row(
            "skip_unavailable_shards",
            "set_skip_unavailable_shards",
            opts.clickhouse.skip_unavailable_shards,
        ));
        layout.add_child(TextView::new(format!(
            "  server_version: {}",
            server_version
        )));
        layout.add_child(DummyView);

        // View
        layout.add_child(bold("View:"));
        layout.add_child(edit_row(
            "delay_interval (ms)",
            "set_delay_interval",
            &opts.view.delay_interval.as_millis().to_string(),
            12,
        ));
        layout.add_child(checkbox_row("group_by", "set_group_by", opts.view.group_by));
        layout.add_child(checkbox_row(
            "no_subqueries",
            "set_no_subqueries",
            opts.view.no_subqueries,
        ));
        layout.add_child(checkbox_row("wrap", "set_wrap", opts.view.wrap));
        layout.add_child(checkbox_row(
            "no_strip_hostname_suffix",
            "set_no_strip_hostname_suffix",
            opts.view.no_strip_hostname_suffix,
        ));
        layout.add_child(edit_row(
            "start",
            "set_start",
            &opts.view.start.to_editable_string(),
            22,
        ));
        layout.add_child(edit_row(
            "end",
            "set_end",
            &opts.view.end.to_editable_string(),
            22,
        ));
        layout.add_child(DummyView);

        // Service (read-only)
        layout.add_child(bold("Service:"));
        layout.add_child(TextView::new(format!(
            "  log: {}",
            opts.service.log.as_deref().unwrap_or("(none)")
        )));
        layout.add_child(TextView::new(format!(
            "  chdig_config: {}",
            opts.service.chdig_config.as_deref().unwrap_or("(none)")
        )));
        layout.add_child(DummyView);

        // Perfetto (query)
        layout.add_child(bold("Perfetto (query):"));
        layout.add_child(checkbox_row(
            "opentelemetry_span_log",
            "set_otel",
            opts.perfetto.opentelemetry_span_log,
        ));
        layout.add_child(checkbox_row(
            "trace_log",
            "set_trace_log",
            opts.perfetto.trace_log,
        ));
        layout.add_child(checkbox_row(
            "query_metric_log",
            "set_query_metric_log",
            opts.perfetto.query_metric_log,
        ));
        layout.add_child(checkbox_row(
            "part_log",
            "set_part_log",
            opts.perfetto.part_log,
        ));
        layout.add_child(checkbox_row(
            "query_thread_log",
            "set_query_thread_log",
            opts.perfetto.query_thread_log,
        ));
        layout.add_child(checkbox_row(
            "text_log",
            "set_text_log",
            opts.perfetto.text_log,
        ));
        layout.add_child(checkbox_row(
            "text_log_android",
            "set_text_log_android",
            opts.perfetto.text_log_android,
        ));
        layout.add_child(checkbox_row(
            "per_server",
            "set_per_server",
            opts.perfetto.per_server,
        ));
        layout.add_child(DummyView);

        // Perfetto (server)
        layout.add_child(bold("Perfetto (server):"));
        layout.add_child(checkbox_row(
            "metric_log",
            "set_metric_log",
            opts.perfetto.metric_log,
        ));
        layout.add_child(checkbox_row(
            "asynchronous_metric_log",
            "set_async_metric_log",
            opts.perfetto.asynchronous_metric_log,
        ));
        layout.add_child(checkbox_row(
            "asynchronous_insert_log",
            "set_async_insert_log",
            opts.perfetto.asynchronous_insert_log,
        ));
        layout.add_child(checkbox_row(
            "error_log",
            "set_error_log",
            opts.perfetto.error_log,
        ));
        layout.add_child(checkbox_row(
            "s3_queue_log",
            "set_s3_queue_log",
            opts.perfetto.s3_queue_log,
        ));
        layout.add_child(checkbox_row(
            "azure_queue_log",
            "set_azure_queue_log",
            opts.perfetto.azure_queue_log,
        ));
        layout.add_child(checkbox_row(
            "blob_storage_log",
            "set_blob_storage_log",
            opts.perfetto.blob_storage_log,
        ));
        layout.add_child(checkbox_row(
            "background_schedule_pool_log",
            "set_bg_pool_log",
            opts.perfetto.background_schedule_pool_log,
        ));
        layout.add_child(checkbox_row(
            "session_log",
            "set_session_log",
            opts.perfetto.session_log,
        ));
        layout.add_child(checkbox_row(
            "aggregated_zookeeper_log",
            "set_zk_log",
            opts.perfetto.aggregated_zookeeper_log,
        ));
        layout.add_child(DummyView);

        // Runtime (read-only)
        layout.add_child(bold("Runtime:"));
        layout.add_child(TextView::new(format!(
            "  selected_host: {}",
            selected_host.as_deref().unwrap_or("(all)")
        )));
        layout.add_child(TextView::new(format!(
            "  current_view: {:?}",
            current_view.unwrap_or(ChDigViews::Queries)
        )));

        let context_for_apply = context;
        let dialog = Dialog::new()
            .title("Settings")
            .content(ScrollView::new(layout))
            .button("Apply", move |siv| {
                let history = siv
                    .call_on_name("set_history", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let internal_queries = siv
                    .call_on_name("set_internal_queries", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let limit_str = siv
                    .call_on_name("set_limit", |v: &mut EditView| v.get_content())
                    .unwrap();
                let skip_unavailable_shards = siv
                    .call_on_name("set_skip_unavailable_shards", |v: &mut Checkbox| {
                        v.is_checked()
                    })
                    .unwrap();

                let delay_str = siv
                    .call_on_name("set_delay_interval", |v: &mut EditView| v.get_content())
                    .unwrap();
                let group_by = siv
                    .call_on_name("set_group_by", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let no_subqueries = siv
                    .call_on_name("set_no_subqueries", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let wrap = siv
                    .call_on_name("set_wrap", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let no_strip = siv
                    .call_on_name("set_no_strip_hostname_suffix", |v: &mut Checkbox| {
                        v.is_checked()
                    })
                    .unwrap();
                let start_str = siv
                    .call_on_name("set_start", |v: &mut EditView| v.get_content())
                    .unwrap();
                let end_str = siv
                    .call_on_name("set_end", |v: &mut EditView| v.get_content())
                    .unwrap();

                let otel = siv
                    .call_on_name("set_otel", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let trace_log = siv
                    .call_on_name("set_trace_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let query_metric = siv
                    .call_on_name("set_query_metric_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let part_log = siv
                    .call_on_name("set_part_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let query_thread = siv
                    .call_on_name("set_query_thread_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let text_log = siv
                    .call_on_name("set_text_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let text_log_android = siv
                    .call_on_name("set_text_log_android", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let per_server = siv
                    .call_on_name("set_per_server", |v: &mut Checkbox| v.is_checked())
                    .unwrap();

                let metric_log = siv
                    .call_on_name("set_metric_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let async_metric_log = siv
                    .call_on_name("set_async_metric_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let async_insert_log = siv
                    .call_on_name("set_async_insert_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let error_log = siv
                    .call_on_name("set_error_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let s3_queue_log = siv
                    .call_on_name("set_s3_queue_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let azure_queue_log = siv
                    .call_on_name("set_azure_queue_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let blob_storage_log = siv
                    .call_on_name("set_blob_storage_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let bg_pool_log = siv
                    .call_on_name("set_bg_pool_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let session_log = siv
                    .call_on_name("set_session_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();
                let zk_log = siv
                    .call_on_name("set_zk_log", |v: &mut Checkbox| v.is_checked())
                    .unwrap();

                let limit: u64 = match limit_str.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        siv.add_layer(Dialog::info("Invalid limit value"));
                        return;
                    }
                };
                let delay_ms: u64 = match delay_str.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        siv.add_layer(Dialog::info("Invalid delay_interval value"));
                        return;
                    }
                };
                let new_start = match start_str.parse::<crate::common::RelativeDateTime>() {
                    Ok(v) => v,
                    Err(err) => {
                        siv.add_layer(Dialog::info(format!("Invalid start: {}", err)));
                        return;
                    }
                };
                let new_end = match end_str.parse::<crate::common::RelativeDateTime>() {
                    Ok(v) => v,
                    Err(err) => {
                        siv.add_layer(Dialog::info(format!("Invalid end: {}", err)));
                        return;
                    }
                };

                {
                    let mut ctx = context_for_apply.lock().unwrap();
                    ctx.options.clickhouse.history = history;
                    ctx.options.clickhouse.internal_queries = internal_queries;
                    ctx.options.clickhouse.limit = limit;
                    ctx.options.clickhouse.skip_unavailable_shards = skip_unavailable_shards;

                    ctx.options.view.delay_interval = std::time::Duration::from_millis(delay_ms);
                    ctx.options.view.group_by = group_by;
                    ctx.options.view.no_subqueries = no_subqueries;
                    ctx.options.view.wrap = wrap;
                    ctx.options.view.no_strip_hostname_suffix = no_strip;
                    ctx.options.view.start = new_start;
                    ctx.options.view.end = new_end;

                    ctx.options.perfetto.opentelemetry_span_log = otel;
                    ctx.options.perfetto.trace_log = trace_log;
                    ctx.options.perfetto.query_metric_log = query_metric;
                    ctx.options.perfetto.part_log = part_log;
                    ctx.options.perfetto.query_thread_log = query_thread;
                    ctx.options.perfetto.text_log = text_log;
                    ctx.options.perfetto.text_log_android = text_log_android;
                    ctx.options.perfetto.per_server = per_server;
                    ctx.options.perfetto.metric_log = metric_log;
                    ctx.options.perfetto.asynchronous_metric_log = async_metric_log;
                    ctx.options.perfetto.asynchronous_insert_log = async_insert_log;
                    ctx.options.perfetto.error_log = error_log;
                    ctx.options.perfetto.s3_queue_log = s3_queue_log;
                    ctx.options.perfetto.azure_queue_log = azure_queue_log;
                    ctx.options.perfetto.blob_storage_log = blob_storage_log;
                    ctx.options.perfetto.background_schedule_pool_log = bg_pool_log;
                    ctx.options.perfetto.session_log = session_log;
                    ctx.options.perfetto.aggregated_zookeeper_log = zk_log;

                    ctx.trigger_view_refresh();
                }
                siv.pop_layer();
            })
            .button("Cancel", |siv| {
                siv.pop_layer();
            });
        self.add_layer(dialog.with_name("settings"));
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
                WorkerEvent::ServerFlameGraph(tui, trace_type, start, end),
            );
        } else {
            context
                .worker
                .send(true, WorkerEvent::LiveQueryFlameGraph(tui, None));
        }
    }

    fn show_jemalloc_flamegraph(&mut self, tui: bool) {
        let mut context = self.user_data::<ContextArc>().unwrap().lock().unwrap();
        context
            .worker
            .send(true, WorkerEvent::JemallocFlameGraph(tui));
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

        let on_submit = move |siv: &mut Cursive| {
            let start_str = siv
                .call_on_name("perfetto_start", |view: &mut EditView| view.get_content())
                .unwrap();
            let end_str = siv
                .call_on_name("perfetto_end", |view: &mut EditView| view.get_content())
                .unwrap();

            let start = match start_str.parse::<crate::common::RelativeDateTime>() {
                Ok(v) => v,
                Err(err) => {
                    siv.add_layer(Dialog::info(format!("Invalid start: {}", err)));
                    return;
                }
            };
            let end = match end_str.parse::<crate::common::RelativeDateTime>() {
                Ok(v) => v,
                Err(err) => {
                    siv.add_layer(Dialog::info(format!("Invalid end: {}", err)));
                    return;
                }
            };

            siv.pop_layer();

            let start_dt: DateTime<Local> = start.into();
            let end_dt: DateTime<Local> = end.into();
            let mut ctx = siv.user_data::<ContextArc>().unwrap().lock().unwrap();
            ctx.worker
                .send(true, WorkerEvent::ServerPerfettoExport(start_dt, end_dt));
        };

        let dialog = Dialog::new()
            .title("Server Perfetto Export")
            .content(
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
            )
            .button("Export", on_submit)
            .button("Cancel", |siv| {
                siv.pop_layer();
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
        let cb_sink = context.cb_sink.clone();
        drop(context);

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let hosts = runtime.block_on(async { clickhouse.get_cluster_hosts().await });

            cb_sink
                .send(Box::new(move |siv: &mut Cursive| match hosts {
                    Ok(hosts) if !hosts.is_empty() => {
                        let mut select = SelectView::new().autojump();

                        select.add_item("<All hosts (reset filter)>", String::new());
                        for host in hosts {
                            let host_clone = host.clone();
                            select.add_item(host, host_clone);
                        }

                        let context_arc = siv.user_data::<ContextArc>().unwrap().clone();
                        select.set_on_submit(move |siv, selected_host: &String| {
                            let current_view = {
                                let mut context = context_arc.lock().unwrap();

                                let url_safe = context.options.clickhouse.url_safe.clone();
                                if selected_host.is_empty() {
                                    context.selected_host = None;
                                    log::info!("Reset host filter");
                                    siv.set_statusbar_connection(url_safe);
                                } else {
                                    context.selected_host = Some(selected_host.clone());
                                    log::info!("Set host filter to: {}", selected_host);
                                    siv.set_statusbar_connection(format!(
                                        "{url_safe} (host: {selected_host})"
                                    ));
                                }

                                // Get current view name to re-open it
                                context
                                    .current_view
                                    .or(context.options.start_view)
                                    .unwrap_or(ChDigViews::Queries)
                            };

                            siv.pop_layer();

                            // Re-open the current view to rebuild with correct columns
                            log::info!("Reopen {:?} view", current_view);

                            let provider = context_arc
                                .lock()
                                .unwrap()
                                .view_registry
                                .get_by_view_type(current_view);

                            siv.drop_main_view();
                            provider.show(siv, context_arc.clone());

                            context_arc.lock().unwrap().trigger_view_refresh();
                        });

                        let dialog = Dialog::around(select).title("Filter by host").button(
                            "Cancel",
                            |siv| {
                                siv.pop_layer();
                            },
                        );

                        siv.add_layer(dialog);
                    }
                    Ok(_) => {
                        siv.add_layer(Dialog::info("No hosts found in cluster"));
                    }
                    Err(err) => {
                        siv.add_layer(Dialog::info(format!(
                            "Failed to fetch cluster hosts: {}",
                            err
                        )));
                    }
                }))
                .unwrap();
        });
    }

    fn drop_main_view(&mut self) {
        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        self.call_on_name("main", |main_view: &mut LinearLayout| {
            // Views that should not be touched:
            // - top bar (menu text + is_paused + status)
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

    fn set_statusbar_version(&mut self, main_content: impl Into<SpannedString<Style>>) {
        self.call_on_name("version", |text_view: &mut TextView| {
            let content: SpannedString<Style> = main_content.into();
            let mut styled = StyledString::new();
            // NOTE: may not work in some terminals
            styled.append_styled(content.source(), Effect::Dim);
            text_view.set_content(styled);
        })
        .expect("version");
    }

    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>) {
        self.call_on_name("status", |text_view: &mut TextView| {
            text_view.set_content(content);
        })
        .expect("set_status")
    }

    fn set_statusbar_connection(&mut self, content: impl Into<SpannedString<Style>>) {
        self.call_on_name("connection", |text_view: &mut TextView| {
            text_view.set_content(content);
        })
        .expect("connection");
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
