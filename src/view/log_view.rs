use anyhow::{Error, Result};
use chrono::{DateTime, Datelike, Duration, Local, Timelike};
use cursive::{
    Cursive, Printer, Vec2,
    event::{Callback, Event, EventResult, Key},
    theme::{Color, ColorStyle, Style},
    utils::{lines::spans::LinesIterator, markup::StyledString},
    view::{Nameable, Resizable, ScrollStrategy, View, ViewWrapper, scroll},
    views::{Dialog, EditView, NamedView, OnEventView},
    wrap_impl,
};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use unicode_width::UnicodeWidthStr;

use crate::common::RelativeDateTime;
use crate::interpreter::{ContextArc, TextLogArguments};
use crate::utils::find_common_hostname_prefix_and_suffix;
use crate::view::{TextLogView, show_bottom_prompt};

// Hash-based color function matching ClickHouse's setColor from terminalColors.cpp
// Uses YCbCr color space with constant brightness (y=128) for better readability
fn hash_to_color(hash: u64) -> Color {
    let y = 128u8;
    let cb = ((hash >> 8) & 0xFF) as u8;
    let cr = (hash & 0xFF) as u8;

    // YCbCr to RGB conversion (ITU-R BT.601)
    // R = Y + 1.402 * (Cr - 128)
    // G = Y - 0.344136 * (Cb - 128) - 0.714136 * (Cr - 128)
    // B = Y + 1.772 * (Cb - 128)

    let cb_offset = cb as i32 - 128;
    let cr_offset = cr as i32 - 128;

    let r = (y as i32 + (1402 * cr_offset) / 1000).clamp(0, 255) as u8;
    let g = (y as i32 - (344 * cb_offset) / 1000 - (714 * cr_offset) / 1000).clamp(0, 255) as u8;
    let b = (y as i32 + (1772 * cb_offset) / 1000).clamp(0, 255) as u8;

    Color::Rgb(r, g, b)
}

// Color for log priority level matching ClickHouse's setColorForLogPriority from terminalColors.cpp
fn get_level_color(level: &str) -> Color {
    match level {
        // Fatal: \033[1;41m (bold + red background) - using bright red
        "Fatal" => Color::Rgb(255, 85, 85),
        // Critical: \033[7;31m (reverse video + red) - using bright red
        "Critical" => Color::Rgb(255, 85, 85),
        // Error: \033[1;31m (bold red) - bright red
        "Error" => Color::Rgb(255, 85, 85),
        // Warning: \033[0;31m (red) - normal red
        "Warning" => Color::Rgb(255, 0, 0),
        // Notice: \033[0;33m (yellow) - normal yellow
        "Notice" => Color::Rgb(255, 255, 0),
        // Information: \033[1m (bold) - using default terminal color (light gray)
        "Information" => Color::Rgb(192, 192, 192),
        // Debug: no color - default terminal color
        "Debug" => Color::TerminalDefault,
        // Trace: \033[2m (dim) - dark gray
        "Trace" => Color::Rgb(128, 128, 128),
        // Test: no specific color in ClickHouse
        "Test" => Color::TerminalDefault,
        _ => Color::TerminalDefault,
    }
}

// Hash function similar to ClickHouse's intHash64
fn int_hash_64(value: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn string_hash(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone)]
pub struct LogEntry {
    pub host_name: String,
    pub display_host_name: Option<String>,
    pub event_time_microseconds: DateTime<Local>,
    pub thread_id: u64,
    pub level: String,
    pub message: String,
    pub query_id: Option<String>,
    pub logger_name: Option<String>,
}

struct IdentifierMaps {
    query_id_map: HashMap<String, String>,
    logger_name_map: HashMap<String, String>,
    level_map: HashMap<String, String>,
    host_name_map: HashMap<String, String>,
}

impl LogEntry {
    fn to_styled_string(&self, cluster: bool) -> StyledString {
        self.to_styled_string_with_identifiers(cluster, None)
    }

    fn to_styled_string_with_identifiers(
        &self,
        cluster: bool,
        identifier_maps: Option<&IdentifierMaps>,
    ) -> StyledString {
        let mut line = StyledString::new();

        if cluster {
            line.append_plain("[");
            let host_hash = string_hash(&self.host_name);
            let host_color = hash_to_color(host_hash);
            let display_name = self.display_host_name.as_ref().unwrap_or(&self.host_name);
            line.append_styled(display_name, host_color);

            if let Some(maps) = identifier_maps
                && let Some(id) = maps.host_name_map.get(&self.host_name)
            {
                line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
            }
            line.append_plain("] ");
        }

        // Format timestamp with microseconds matching ClickHouse format: YYYY.MM.DD HH:MM:SS.microseconds
        let dt = self.event_time_microseconds;
        let microseconds = dt.timestamp_subsec_micros();
        let timestamp = format!(
            "{:04}.{:02}.{:02} {:02}:{:02}:{:02}.{:06}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            microseconds
        );
        line.append_plain(format!("{} ", timestamp));

        // Thread ID with hash-based coloring: [ thread_id ]
        line.append_plain("[ ");
        let thread_hash = int_hash_64(self.thread_id);
        let thread_color = hash_to_color(thread_hash);
        line.append_styled(format!("{}", self.thread_id), thread_color);
        line.append_plain(" ] ");

        // Query ID with hash-based coloring: {query_id}
        // ClickHouse writes query_id even if empty for log parser convenience
        line.append_plain("{");
        let query_id_str = self.query_id.as_deref().unwrap_or("");
        if !query_id_str.is_empty() {
            let query_hash = string_hash(query_id_str);
            let query_color = hash_to_color(query_hash);
            line.append_styled(query_id_str, query_color);

            if let Some(maps) = identifier_maps
                && let Some(id) = maps.query_id_map.get(query_id_str)
            {
                line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
            }
        }
        line.append_plain("} ");

        // Priority level with color: <level>
        line.append_plain("<");
        let level_color = get_level_color(self.level.as_str());
        line.append_styled(self.level.as_str(), level_color);
        if let Some(maps) = identifier_maps
            && let Some(id) = maps.level_map.get(&self.level)
        {
            line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
        }
        line.append_plain("> ");

        // Logger name (source) with hash-based coloring: source:
        if let Some(logger_name) = &self.logger_name {
            let logger_hash = string_hash(logger_name);
            let logger_color = hash_to_color(logger_hash);
            line.append_styled(logger_name, logger_color);

            if let Some(maps) = identifier_maps
                && let Some(id) = maps.logger_name_map.get(logger_name)
            {
                line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
            }
            line.append_plain(": ");
        }

        // Message
        line.append_plain(self.message.as_str());
        return line;
    }
}

#[derive(Clone)]
enum FilterType {
    QueryId(String),
    LoggerName(String),
    Level(String),
    HostName(String),
}

pub struct LogViewBase {
    max_width: usize,

    content_size_with_wrap: Vec2,
    // Size without respecting wrap, since with wrap width is equal to the longest line
    screen_size_without_wrap: Vec2,

    needs_relayout: bool,
    update_content: bool,
    scroll_core: scroll::Core,

    search_direction_forward: bool,
    search_term: String,
    matched_row: Option<usize>,
    matched_col: Option<usize>,
    skip_scroll: bool,

    cluster: bool,
    wrap: bool,
    no_strip_hostname_suffix: bool,

    // Filter mode state
    filter_mode: bool,
    filter_identifiers: HashMap<String, FilterType>,
    active_filter: Option<FilterType>,

    logs: Vec<LogEntry>,

    // When filtering is active, stores indices into self.logs for visible entries
    // Empty when no filter is active (all logs visible)
    filtered_log_indices: Vec<usize>,

    // Cumulative row counts: log_cumulative_rows[i] = total rows in logs 0..i
    // This allows O(log n) binary search to map display_row -> log_index
    log_cumulative_rows: Vec<usize>,
    last_computed_width: usize,
}

impl Default for LogViewBase {
    fn default() -> Self {
        Self {
            max_width: 0,
            content_size_with_wrap: Vec2::zero(),
            screen_size_without_wrap: Vec2::zero(),
            needs_relayout: false,
            update_content: false,
            scroll_core: scroll::Core::default(),
            search_direction_forward: false,
            search_term: String::new(),
            matched_row: None,
            matched_col: None,
            skip_scroll: false,
            cluster: false,
            wrap: false,
            no_strip_hostname_suffix: false,
            filter_mode: false,
            filter_identifiers: HashMap::new(),
            active_filter: None,
            logs: Vec::new(),
            filtered_log_indices: Vec::new(),
            log_cumulative_rows: Vec::new(),
            last_computed_width: usize::MAX,
        }
    }
}

cursive::impl_scroller!(LogViewBase::scroll_core);

impl LogViewBase {
    // Get the log at the given visible index
    // If filtering is active, maps through filtered_log_indices
    fn get_visible_log(&self, visible_idx: usize) -> Option<&LogEntry> {
        if self.filtered_log_indices.is_empty() {
            self.logs.get(visible_idx)
        } else {
            self.filtered_log_indices
                .get(visible_idx)
                .and_then(|&idx| self.logs.get(idx))
        }
    }

    // Get count of visible logs
    fn visible_log_count(&self) -> usize {
        if self.filtered_log_indices.is_empty() {
            self.logs.len()
        } else {
            self.filtered_log_indices.len()
        }
    }

    // Get identifier maps for rendering with highlights
    fn get_identifier_maps(&self) -> Option<IdentifierMaps> {
        if !self.filter_mode {
            return None;
        }

        let mut identifier_maps = IdentifierMaps {
            query_id_map: HashMap::new(),
            logger_name_map: HashMap::new(),
            level_map: HashMap::new(),
            host_name_map: HashMap::new(),
        };

        for (id, filter_type) in &self.filter_identifiers {
            match filter_type {
                FilterType::QueryId(val) => {
                    identifier_maps.query_id_map.insert(val.clone(), id.clone());
                }
                FilterType::LoggerName(val) => {
                    identifier_maps
                        .logger_name_map
                        .insert(val.clone(), id.clone());
                }
                FilterType::Level(val) => {
                    identifier_maps.level_map.insert(val.clone(), id.clone());
                }
                FilterType::HostName(val) => {
                    identifier_maps
                        .host_name_map
                        .insert(val.clone(), id.clone());
                }
            }
        }

        Some(identifier_maps)
    }

    // Binary search to find which log a display row belongs to
    // Returns (log_index, row_within_log)
    fn display_row_to_log(&self, display_row: usize) -> Option<(usize, usize)> {
        if self.log_cumulative_rows.is_empty() {
            return None;
        }

        // Use proper binary search: find first cumulative > display_row
        // cumulative_rows[i] = total rows in logs 0..=i
        let log_idx = match self.log_cumulative_rows.binary_search(&(display_row + 1)) {
            Ok(idx) => idx,  // Found exact match for display_row + 1
            Err(idx) => idx, // Would insert at idx, so first element > display_row is at idx
        };

        if log_idx >= self.log_cumulative_rows.len() {
            return None;
        }

        let row_start = if log_idx == 0 {
            0
        } else {
            self.log_cumulative_rows[log_idx - 1]
        };
        let row_within_log = display_row - row_start;

        Some((log_idx, row_within_log))
    }

    // Map log_index to its starting display row
    fn log_to_display_row(&self, log_idx: usize) -> usize {
        if log_idx == 0 {
            0
        } else {
            self.log_cumulative_rows
                .get(log_idx - 1)
                .copied()
                .unwrap_or(0)
        }
    }

    fn extract_identifiers(&mut self) {
        let mut query_ids: HashMap<String, usize> = HashMap::new();
        let mut logger_names: HashMap<String, usize> = HashMap::new();
        let mut levels: HashMap<String, usize> = HashMap::new();
        let mut host_names: HashMap<String, usize> = HashMap::new();

        for log in &self.logs {
            if let Some(ref query_id) = log.query_id
                && !query_id.is_empty()
            {
                query_ids.entry(query_id.clone()).or_insert(0);
            }
            if let Some(ref logger_name) = log.logger_name {
                logger_names.entry(logger_name.clone()).or_insert(0);
            }
            levels.entry(log.level.clone()).or_insert(0);
            host_names.entry(log.host_name.clone()).or_insert(0);
        }

        self.filter_identifiers.clear();
        let mut counter = 1;

        for query_id in query_ids.keys() {
            let id = format!("q{}", counter);
            self.filter_identifiers
                .insert(id, FilterType::QueryId(query_id.clone()));
            counter += 1;
        }

        counter = 1;
        for logger_name in logger_names.keys() {
            let id = format!("l{}", counter);
            self.filter_identifiers
                .insert(id, FilterType::LoggerName(logger_name.clone()));
            counter += 1;
        }

        counter = 1;
        for level in levels.keys() {
            let id = format!("v{}", counter);
            self.filter_identifiers
                .insert(id, FilterType::Level(level.clone()));
            counter += 1;
        }

        counter = 1;
        for host_name in host_names.keys() {
            let id = format!("h{}", counter);
            self.filter_identifiers
                .insert(id, FilterType::HostName(host_name.clone()));
            counter += 1;
        }
    }

    fn rebuild_content_with_highlights(&mut self) {
        self.filtered_log_indices.clear();
        self.needs_relayout = true;
        self.compute_rows();
    }

    fn rebuild_content_normal(&mut self) {
        self.filtered_log_indices.clear();
        self.needs_relayout = true;
        self.compute_rows();
    }

    fn apply_filter(&mut self) {
        self.filtered_log_indices.clear();

        if let Some(ref filter) = self.active_filter {
            for (idx, log) in self.logs.iter().enumerate() {
                let matches = match filter {
                    FilterType::QueryId(val) => log.query_id.as_ref() == Some(val),
                    FilterType::LoggerName(val) => log.logger_name.as_ref() == Some(val),
                    FilterType::Level(val) => &log.level == val,
                    FilterType::HostName(val) => &log.host_name == val,
                };
                if matches {
                    self.filtered_log_indices.push(idx);
                }
            }
        }

        self.needs_relayout = true;
        self.compute_rows();
    }

    fn search_in_direction(&mut self, forward: bool) -> bool {
        if self.search_term.is_empty() {
            return false;
        }

        let start_log_idx = if let Some(matched_row) = self.matched_row {
            self.display_row_to_log(matched_row)
                .map(|(idx, _)| idx)
                .unwrap_or(0)
        } else {
            0
        };

        let total_logs = self.visible_log_count();
        let identifier_maps = self.get_identifier_maps();

        if forward {
            for log_idx in (start_log_idx..total_logs).chain(0..start_log_idx) {
                if self.search_log(log_idx, start_log_idx, &identifier_maps, forward) {
                    return true;
                }
            }
        } else {
            for log_idx in (0..=start_log_idx)
                .rev()
                .chain((start_log_idx + 1..total_logs).rev())
            {
                if self.search_log(log_idx, start_log_idx, &identifier_maps, forward) {
                    return true;
                }
            }
        }

        false
    }

    fn search_log(
        &mut self,
        log_idx: usize,
        start_log_idx: usize,
        identifier_maps: &Option<IdentifierMaps>,
        forward: bool,
    ) -> bool {
        if let Some(log) = self.get_visible_log(log_idx) {
            let mut styled = if let Some(maps) = identifier_maps {
                log.to_styled_string_with_identifiers(self.cluster, Some(maps))
            } else {
                log.to_styled_string(self.cluster)
            };
            styled.append("\n");

            let display_row_start = self.log_to_display_row(log_idx);

            if forward {
                let mut current_row = display_row_start;
                for row in LinesIterator::new(&styled, self.last_computed_width) {
                    if log_idx == start_log_idx && Some(current_row) <= self.matched_row {
                        current_row += 1;
                        continue;
                    }

                    if self.search_row(&styled, &row, current_row, forward) {
                        return true;
                    }
                    current_row += 1;
                }
            } else {
                let rows: Vec<_> = LinesIterator::new(&styled, self.last_computed_width).collect();
                for (row_within_log, row) in rows.iter().enumerate().rev() {
                    let current_row = display_row_start + row_within_log;

                    if log_idx == start_log_idx && Some(current_row) >= self.matched_row {
                        continue;
                    }

                    if self.search_row(&styled, row, current_row, forward) {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn search_row(
        &mut self,
        styled: &StyledString,
        row: &cursive::utils::lines::spans::Row,
        current_row: usize,
        forward: bool,
    ) -> bool {
        let mut x = 0;
        for span in row.resolve_stream(styled) {
            if let Some(pos) = span.content.find(&self.search_term) {
                self.matched_row = Some(current_row);
                self.matched_col = Some(x + pos);
                log::trace!(
                    "search_term: {}, matched_row: {:?} ({}-search)",
                    &self.search_term,
                    self.matched_row,
                    if forward { "forward" } else { "reverse" }
                );
                return true;
            }
            x += span.content.width();
        }
        false
    }

    fn update_search_forward(&mut self) -> bool {
        self.search_in_direction(true)
    }

    fn update_search_reverse(&mut self) -> bool {
        self.search_in_direction(false)
    }

    fn update_search(&mut self) -> bool {
        // In case of resize we can have less rows then before,
        // so reset the matched_row for this scenario to avoid out-of-bound access.
        let total_rows = self.log_cumulative_rows.last().copied().unwrap_or(0);
        if total_rows < self.matched_row.unwrap_or_default() {
            self.matched_row = None;
        }
        if self.search_direction_forward {
            return self.update_search_forward();
        } else {
            return self.update_search_reverse();
        }
    }

    fn set_options(&mut self, options: &str) -> Result<()> {
        if options.is_empty() {
        } else if options == "S" {
            self.wrap = !self.wrap;
            log::trace!("Toggle wrap mode, switched to {}", self.wrap);
        } else {
            return Err(Error::msg(format!("Invalid options: {}", options)));
        }
        return Ok(());
    }

    fn push_logs(&mut self, mut logs: Vec<LogEntry>) {
        log::trace!("Add {} log entries", logs.len());

        // Strip common hostname prefix and suffix from first 1000 newly added items
        if !self.no_strip_hostname_suffix && logs.len() > 1 {
            let sample_size = logs.len().min(1000);
            let (common_prefix, common_suffix) = find_common_hostname_prefix_and_suffix(
                logs.iter().take(sample_size).map(|l| l.host_name.as_str()),
            );

            if !common_prefix.is_empty() || !common_suffix.is_empty() {
                for log in logs.iter_mut() {
                    let mut hostname = log.host_name.as_str();

                    if !common_prefix.is_empty()
                        && let Some(stripped) = hostname.strip_prefix(&common_prefix)
                    {
                        hostname = stripped;
                    }

                    if !common_suffix.is_empty()
                        && let Some(stripped) = hostname.strip_suffix(&common_suffix)
                    {
                        hostname = stripped;
                    }

                    log.display_host_name = Some(hostname.to_string());
                }
            }
        }

        self.logs.extend(logs);

        if self.filter_mode {
            self.extract_identifiers();
            self.rebuild_content_with_highlights();
        } else if self.active_filter.is_some() {
            self.apply_filter();
        } else {
            self.needs_relayout = true;
            self.compute_rows();
        }
    }

    fn compute_rows(&mut self) {
        let width = if self.wrap {
            // For scrolling we need to subtract some padding
            self.screen_size_without_wrap.x.saturating_sub(2)
        } else {
            usize::MAX
        };

        let visible_count = self.visible_log_count();

        // Check if we can do incremental computation:
        // - Width hasn't changed (no wrap mode change or resize affecting width)
        // - No filtering is active (filtered_log_indices is empty, NOTE: we can optimize this case as well)
        // - We have previous computed data
        // - We're only adding logs (visible_count >= previous count)
        let can_do_incremental = self.last_computed_width == width
            && self.filtered_log_indices.is_empty()
            && !self.log_cumulative_rows.is_empty()
            && visible_count >= self.log_cumulative_rows.len();

        let start_idx = if can_do_incremental {
            self.log_cumulative_rows.len()
        } else {
            self.log_cumulative_rows.clear();
            0
        };

        let mut max_width = if can_do_incremental {
            self.max_width
        } else {
            0
        };
        let mut cumulative = if can_do_incremental {
            *self.log_cumulative_rows.last().unwrap()
        } else {
            0
        };

        let identifier_maps = self.get_identifier_maps();

        // Build cumulative row counts by computing styled strings on-demand
        // We compute them here just to count rows, then discard them (saves memory)
        for i in start_idx..visible_count {
            if let Some(log) = self.get_visible_log(i) {
                let mut styled = if let Some(ref maps) = identifier_maps {
                    log.to_styled_string_with_identifiers(self.cluster, Some(maps))
                } else {
                    log.to_styled_string(self.cluster)
                };
                styled.append("\n");

                let mut row_count = 0;
                for row in LinesIterator::new(&styled, width) {
                    max_width = usize::max(max_width, row.width);
                    row_count += 1;
                }
                cumulative += row_count;
                self.log_cumulative_rows.push(cumulative);
            }
        }

        self.max_width = max_width;
        self.last_computed_width = width;

        log::trace!(
            "Updating rows cache (width: {:?}, wrap: {}, max width: {}, rows: {}, visible_logs: {}/{}, incremental: {}/{}, inner size: {:?}, last size: {:?})",
            width,
            self.wrap,
            max_width,
            cumulative,
            visible_count,
            self.logs.len(),
            can_do_incremental,
            start_idx,
            self.scroll_core.inner_size(),
            self.scroll_core.last_available_size()
        );

        self.update_search();
        // Show the horizontal scrolling
        self.needs_relayout = true;
    }

    fn rows_are_valid(&mut self, size: Vec2) -> bool {
        if self.update_content || self.needs_relayout {
            return false;
        }
        if self.wrap && self.content_size_with_wrap != size {
            return false;
        }
        return true;
    }

    fn layout_content(&mut self, size: Vec2) {
        if !self.rows_are_valid(size) {
            log::trace!(
                "Size changed: content_size={:?}, screen_size={:?}, size={:?}",
                self.content_size_with_wrap,
                self.screen_size_without_wrap,
                size
            );
            self.content_size_with_wrap = size;
            self.compute_rows();

            self.scroll_core.set_scroll_x(!self.wrap);
        }
        self.needs_relayout = false;
        self.update_content = false;
    }

    fn inner_required_size(&mut self, mut req: Vec2) -> Vec2 {
        self.screen_size_without_wrap = req;

        let total_rows = self.log_cumulative_rows.last().copied().unwrap_or(0);
        req.y = total_rows;
        req.x = usize::max(req.x, self.max_width);
        return req;
    }

    fn draw_content(&self, printer: &Printer<'_, '_>) {
        let start_row = printer.content_offset.y;
        let end_row = start_row + printer.output_size.y;
        let total_rows = self.log_cumulative_rows.last().copied().unwrap_or(0);

        let identifier_maps = self.get_identifier_maps();

        for display_row in start_row..end_row.min(total_rows) {
            // Binary search to find which log this display row belongs to
            if let Some((log_idx, row_within_log)) = self.display_row_to_log(display_row)
                && let Some(log) = self.get_visible_log(log_idx)
            {
                let mut styled = if let Some(ref maps) = identifier_maps {
                    log.to_styled_string_with_identifiers(self.cluster, Some(maps))
                } else {
                    log.to_styled_string(self.cluster)
                };
                styled.append("\n");
                if let Some(row) =
                    LinesIterator::new(&styled, self.last_computed_width).nth(row_within_log)
                {
                    let y = display_row;
                    let mut x = 0;

                    for span in row.resolve_stream(&styled) {
                        // Check if the span contains the search term
                        if !self.search_term.is_empty() && span.content.contains(&self.search_term)
                        {
                            let content = span.content;
                            let search_term = &self.search_term;
                            let mut last_pos = 0;

                            for (match_start, _) in content.match_indices(search_term) {
                                // Print text before match with normal style
                                if match_start > last_pos {
                                    let before = &content[last_pos..match_start];
                                    printer.with_style(*span.attr, |printer| {
                                        printer.print((x, y), before);
                                    });
                                    x += before.width();
                                }

                                // Use the same highlight theme as less(1):
                                // - Always use black as text color
                                // - Use original text color as background
                                // - For no-style use white as background
                                let matched =
                                    &content[match_start..match_start + search_term.len()];
                                let bg_color = if *span.attr == Style::default() {
                                    Color::Rgb(255, 255, 255).into()
                                } else {
                                    span.attr.color.front
                                };
                                let inverted_style = ColorStyle::new(Color::Rgb(0, 0, 0), bg_color);
                                printer.with_style(inverted_style, |printer| {
                                    printer.print((x, y), matched);
                                });
                                x += matched.width();

                                last_pos = match_start + search_term.len();
                            }

                            // Print remaining text after last match
                            if last_pos < content.len() {
                                let after = &content[last_pos..];
                                printer.with_style(*span.attr, |printer| {
                                    printer.print((x, y), after);
                                });
                                x += after.width();
                            }
                        } else {
                            // No match in this span or row, print normally
                            printer.with_style(*span.attr, |printer| {
                                printer.print((x, y), span.content);
                                x += span.content.width();
                            });
                        }
                    }
                }
            }
        }
    }

    // Write plain text content from the styled string directly to a writer
    fn write_plain_text<W: Write>(&self, writer: &mut W) -> Result<()> {
        let visible_count = self.visible_log_count();

        for i in 0..visible_count {
            if let Some(log) = self.get_visible_log(i) {
                let mut styled = log.to_styled_string(self.cluster);
                styled.append("\n");

                for row in LinesIterator::new(&styled, self.last_computed_width) {
                    for span in row.resolve_stream(&styled) {
                        writer.write_all(span.content.as_bytes())?;
                    }
                    writer.write_all(b"\n")?;
                }
            }
        }
        Ok(())
    }
}

fn show_filtered_logs_popup(siv: &mut Cursive) {
    let context = siv.user_data::<ContextArc>().unwrap().clone();

    // Ensure filter mode is active and identifiers are extracted
    siv.call_on_name("logs", |base: &mut LogViewBase| {
        if !base.filter_mode {
            base.filter_mode = true;
            base.extract_identifiers();
            base.rebuild_content_with_highlights();
        }
    });

    // Get current log entry's timestamp for time range calculation
    let log_time = siv.call_on_name("logs", |base: &mut LogViewBase| {
        let viewport = base.scroll_core.content_viewport();
        let top_row = viewport.top();

        if let Some((log_idx, _)) = base.display_row_to_log(top_row)
            && let Some(log) = base.get_visible_log(log_idx)
        {
            return Some(log.event_time_microseconds);
        }
        None
    });

    let Some(Some(event_time)) = log_time else {
        siv.add_layer(Dialog::info("No log entry at current position"));
        return;
    };

    // Calculate time range: ±1 minute from the log entry
    let start = event_time - Duration::try_minutes(1).unwrap();
    let end = event_time + Duration::try_minutes(1).unwrap();

    let apply_adjacent_filter = move |siv: &mut Cursive, text: &str| {
        let identifier = text.trim().to_string();

        if identifier.is_empty() {
            return;
        }

        // Get the filter type for this identifier
        let filter_info = siv.call_on_name("logs", |base: &mut LogViewBase| {
            base.filter_mode = false;
            base.filter_identifiers.get(&identifier).cloned()
        });

        let Some(Some(filter_type)) = filter_info else {
            siv.add_layer(Dialog::info(format!("Unknown identifier: {}", identifier)));
            return;
        };

        // Build TextLogArguments based on filter type
        let (title, args) = match filter_type {
            FilterType::HostName(hostname) => (
                format!("Logs for host: {}", hostname),
                TextLogArguments {
                    query_ids: None,
                    logger_names: None,
                    hostname: Some(hostname),
                    message_filter: None,
                    max_level: None,
                    start,
                    end: RelativeDateTime::from(end),
                },
            ),
            FilterType::QueryId(query_id) => (
                format!("Logs for query: {}", query_id),
                TextLogArguments {
                    query_ids: Some(vec![query_id]),
                    logger_names: None,
                    hostname: None,
                    message_filter: None,
                    max_level: None,
                    start,
                    end: RelativeDateTime::from(end),
                },
            ),
            FilterType::LoggerName(logger_name) => (
                format!("Logs for logger: {}", logger_name),
                TextLogArguments {
                    query_ids: None,
                    logger_names: Some(vec![logger_name]),
                    hostname: None,
                    message_filter: None,
                    max_level: None,
                    start,
                    end: RelativeDateTime::from(end),
                },
            ),
            FilterType::Level(level) => (
                format!("Logs with level <= {}", level),
                TextLogArguments {
                    query_ids: None,
                    logger_names: None,
                    hostname: None,
                    message_filter: None,
                    max_level: Some(level),
                    start,
                    end: RelativeDateTime::from(end),
                },
            ),
        };

        siv.pop_layer();

        siv.add_layer(
            Dialog::around(
                TextLogView::new("filtered_logs", context.clone(), args)
                    .with_name("filtered_logs")
                    .full_screen(),
            )
            .title(title),
        );
    };

    show_bottom_prompt(siv, "(popup) identifier:", apply_adjacent_filter);
}

pub struct LogView {
    inner_view: OnEventView<NamedView<LogViewBase>>,
}

impl LogView {
    pub fn new(cluster: bool, wrap: bool, no_strip_hostname_suffix: bool) -> Self {
        let mut v = LogViewBase {
            needs_relayout: true,
            cluster,
            wrap,
            no_strip_hostname_suffix,
            ..Default::default()
        };
        v.scroll_core
            .set_scroll_strategy(ScrollStrategy::StickToBottom);
        v.scroll_core.set_scroll_x(!wrap);
        v.scroll_core.set_scroll_y(true);
        // NOTE: we cannot pass mutable ref to view in search_prompt callback, sigh.
        let v = v.with_name("logs");

        let scroll = move |v: &mut NamedView<LogViewBase>, e: &Event| -> Option<EventResult> {
            v.get_mut().skip_scroll = true;
            return Some(scroll::on_event(
                &mut *v.get_mut(),
                e.clone(),
                |s: &mut LogViewBase, e| s.on_event(e),
                |s, si| s.important_area(si),
            ));
        };

        let show_options = |siv: &mut Cursive| {
            let options = move |siv: &mut Cursive, text: &str| {
                let status = siv.call_on_name("logs", |base: &mut LogViewBase| {
                    let status = base.set_options(text);
                    base.compute_rows();
                    return status;
                });
                siv.pop_layer();
                if let Some(Err(err)) = status {
                    siv.add_layer(Dialog::info(err.to_string()));
                }
            };
            show_bottom_prompt(siv, "-", options);
        };

        let search_prompt_impl = |siv: &mut Cursive, forward: bool| {
            let find = move |siv: &mut Cursive, text: &str| {
                let found = siv.call_on_name("logs", |base: &mut LogViewBase| {
                    base.search_term = text.to_string();
                    base.matched_row = None;
                    base.matched_col = None;
                    base.skip_scroll = false;

                    base.search_direction_forward = forward;
                    base.update_search()
                });
                siv.pop_layer();
                if let Some(false) = found {
                    siv.add_layer(Dialog::info("Pattern not found"));
                }
            };
            show_bottom_prompt(siv, "/", find);
        };
        let search_prompt_forward = move |siv: &mut Cursive| {
            search_prompt_impl(siv, /* forward= */ true);
        };
        let search_prompt_reverse = move |siv: &mut Cursive| {
            search_prompt_impl(siv, /* forward= */ false);
        };

        let show_save_prompt = |siv: &mut Cursive| {
            let save_file_impl = |siv: &mut Cursive| {
                let file_path = siv
                    .call_on_name("save_file_path", |view: &mut EditView| {
                        view.get_content().to_string()
                    })
                    .unwrap();
                siv.pop_layer();

                if file_path.trim().is_empty() {
                    siv.add_layer(Dialog::info("File path cannot be empty"));
                    return;
                }

                let result = siv.call_on_name("logs", |base: &mut LogViewBase| -> Result<()> {
                    let mut file = fs::File::create(&file_path)?;
                    base.write_plain_text(&mut file)?;
                    Ok(())
                });

                match result {
                    Some(Ok(_)) => {
                        siv.add_layer(Dialog::info(format!("Logs saved to: {}", file_path)));
                    }
                    Some(Err(err)) => {
                        siv.add_layer(Dialog::info(format!("Error saving file: {}", err)));
                    }
                    None => {
                        siv.add_layer(Dialog::info("Error: Could not access log content"));
                    }
                }
            };

            let save_file_for_submit = {
                move |siv: &mut Cursive, _: &str| {
                    save_file_impl(siv);
                }
            };
            let view = EditView::new()
                .on_submit(save_file_for_submit)
                .with_name("save_file_path")
                .min_width(40);
            siv.add_layer(
                Dialog::around(view)
                    .title("Save logs to file")
                    .button("Save", save_file_impl)
                    .button("Cancel", |siv: &mut Cursive| {
                        siv.pop_layer();
                    }),
            );
        };

        let show_share_prompt = |siv: &mut Cursive| {
            let context = siv.user_data::<ContextArc>().unwrap().clone();

            let dialog = Dialog::text(format!(
                "Share logs to {} with end-to-end encryption?",
                context.clone().lock().unwrap().options.service.pastila_url
            ))
            .title("Share Logs")
            .button("Share (encrypted)", move |siv: &mut Cursive| {
                let context = context.clone();
                siv.pop_layer();

                let content =
                    siv.call_on_name("logs", |base: &mut LogViewBase| -> Result<String> {
                        let mut buffer = Vec::new();
                        base.write_plain_text(&mut buffer)?;
                        Ok(String::from_utf8(buffer)?)
                    });

                let content = match content {
                    Some(Ok(c)) => c,
                    Some(Err(e)) => {
                        siv.add_layer(Dialog::info(format!("Error reading logs: {}", e)));
                        return;
                    }
                    None => {
                        siv.add_layer(Dialog::info("Error: Could not access log content"));
                        return;
                    }
                };

                if content.trim().is_empty() {
                    siv.add_layer(Dialog::info("No logs to share"));
                    return;
                }

                siv.add_layer(Dialog::text("Uploading logs...").title("Please wait"));

                context
                    .lock()
                    .unwrap()
                    .worker
                    .send(false, crate::interpreter::WorkerEvent::ShareLogs(content));
            })
            .button("Cancel", |siv: &mut Cursive| {
                siv.pop_layer();
            });

            siv.add_layer(dialog);
        };

        let toggle_filter_mode_and_prompt = |siv: &mut Cursive| {
            siv.call_on_name("logs", |base: &mut LogViewBase| {
                if base.filter_mode {
                    base.filter_mode = false;
                    base.active_filter = None;
                    base.rebuild_content_normal();
                } else {
                    base.filter_mode = true;
                    base.extract_identifiers();
                    base.rebuild_content_with_highlights();
                }
            });

            let should_show_prompt = siv
                .call_on_name("logs", |base: &mut LogViewBase| base.filter_mode)
                .unwrap_or(false);

            if should_show_prompt {
                let apply_filter = move |siv: &mut Cursive, text: &str| {
                    let identifier = text.trim().to_string();
                    siv.pop_layer();

                    if identifier.is_empty() {
                        siv.call_on_name("logs", |base: &mut LogViewBase| {
                            base.filter_mode = false;
                            base.active_filter = None;
                            base.rebuild_content_normal();
                        });
                        return;
                    }

                    let filter_result = siv.call_on_name("logs", |base: &mut LogViewBase| {
                        if let Some(filter_type) = base.filter_identifiers.get(&identifier) {
                            base.filter_mode = false;
                            base.active_filter = Some(filter_type.clone());
                            base.apply_filter();
                            Ok(())
                        } else {
                            Err(format!("Unknown identifier: {}", identifier))
                        }
                    });

                    if let Some(Err(msg)) = filter_result {
                        siv.add_layer(Dialog::info(msg));
                    }
                };
                show_bottom_prompt(siv, "identifier:", apply_filter);
            }
        };

        let v = OnEventView::new(v)
            .on_pre_event_inner(Key::PageUp, scroll)
            .on_pre_event_inner(Key::PageDown, scroll)
            .on_pre_event_inner(Key::Left, scroll)
            .on_pre_event_inner(Key::Right, scroll)
            .on_pre_event_inner(Key::Up, scroll)
            .on_pre_event_inner(Key::Down, scroll)
            .on_pre_event_inner('j', move |v, _| scroll(v, &Event::Key(Key::Down)))
            .on_pre_event_inner('k', move |v, _| scroll(v, &Event::Key(Key::Up)))
            .on_pre_event_inner('g', move |v, _| scroll(v, &Event::Key(Key::Home)))
            .on_pre_event_inner(Key::End, move |v, _| {
                let mut base = v.get_mut();
                base.skip_scroll = true;
                base.scroll_core.scroll_to_bottom();
                Some(EventResult::consumed())
            })
            .on_pre_event_inner('G', move |v, _| {
                let mut base = v.get_mut();
                base.skip_scroll = true;
                base.scroll_core.scroll_to_bottom();
                Some(EventResult::consumed())
            })
            .on_event_inner('-', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(show_options))));
            })
            .on_event_inner('/', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    search_prompt_forward,
                ))));
            })
            .on_event_inner('?', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    search_prompt_reverse,
                ))));
            })
            .on_event_inner('n', move |v, _| {
                let mut base = v.get_mut();
                base.search_direction_forward = true;
                if base.update_search_forward() {
                    return Some(EventResult::consumed());
                } else {
                    return Some(EventResult::Consumed(Some(Callback::from_fn(|siv| {
                        siv.add_layer(Dialog::info("Pattern not found"));
                    }))));
                }
            })
            .on_event_inner('N', move |v, _| {
                let mut base = v.get_mut();
                base.search_direction_forward = false;
                if base.update_search_reverse() {
                    return Some(EventResult::consumed());
                } else {
                    return Some(EventResult::Consumed(Some(Callback::from_fn(|siv| {
                        siv.add_layer(Dialog::info("Pattern not found"));
                    }))));
                }
            })
            .on_event_inner('s', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    show_save_prompt,
                ))));
            })
            .on_event_inner('S', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    show_share_prompt,
                ))));
            })
            .on_event_inner(Event::CtrlChar('f'), move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    toggle_filter_mode_and_prompt,
                ))));
            })
            .on_event_inner(Event::CtrlChar('s'), move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    show_filtered_logs_popup,
                ))));
            });

        let log_view = LogView { inner_view: v };
        return log_view;
    }

    pub fn push_logs(&mut self, logs: Vec<LogEntry>) {
        self.inner_view.get_inner_mut().get_mut().push_logs(logs);
    }
}

impl View for LogViewBase {
    fn draw(&self, printer: &Printer<'_, '_>) {
        scroll::draw(self, printer, Self::draw_content);
    }

    fn layout(&mut self, size: Vec2) {
        scroll::layout(
            self,
            size.saturating_sub((0, 0)),
            self.needs_relayout,
            Self::layout_content,
            Self::inner_required_size,
        );

        if self.skip_scroll {
            self.skip_scroll = false;
        } else if let Some(matched_row) = self.matched_row {
            let match_start = self.matched_col.unwrap_or(0);
            let match_end = match_start + self.search_term.len();
            let viewport_width = self.scroll_core.last_available_size().x;
            let current_offset = self.scroll_core.content_viewport().left();

            // Only adjust horizontal scroll if the match is not fully visible
            let x_offset = if match_end > current_offset + viewport_width {
                // Match extends beyond right edge - scroll to show the end with max context on left
                match_end.saturating_sub(viewport_width)
            } else if match_start < current_offset {
                // Match starts before left edge - scroll to show start with some context
                match_start
            } else {
                // Match is already visible - keep current position
                current_offset
            };

            self.scroll_core.set_offset((x_offset, matched_row));
        }
    }
}

impl ViewWrapper for LogView {
    wrap_impl!(self.inner_view: OnEventView<NamedView<LogViewBase>>);

    fn wrap_required_size(&mut self, mut req: Vec2) -> Vec2 {
        req = self
            .inner_view
            .get_inner_mut()
            .get_mut()
            .inner_required_size(req);
        // For scrollbars
        req.x += 1;
        req.y += 1;
        return req;
    }
}
