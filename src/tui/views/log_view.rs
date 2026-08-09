use anyhow::{Error, Result};
use chrono::{Datelike, Duration, Timelike};
use ratatui::layout::{Position, Rect};
use ratatui::text::Span;
use regex::Regex;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::common::RelativeDateTime;
use crate::interpreter::{ContextArc, TextLogArguments, WorkerEvent};
use crate::tui::app::App;
use crate::tui::component::{Canvas, Component, Nameable, NamedView, OnEventView};
use crate::tui::dialog::Dialog;
use crate::tui::edit::EditView;
use crate::tui::event::{Event, EventResult, Key, MouseEvent};
use crate::tui::prompt::show_bottom_prompt;
use crate::tui::resize::Resizable;
use crate::tui::style::{Color, Modifier, Style, StyledString, print_str, str_width};
use crate::tui::views::log_store::{LogEntry, LogStore};
use crate::tui::views::text_log_view::TextLogView;
use crate::utils::find_common_hostname_prefix_and_suffix;

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
        "Debug" => Color::Reset,
        // Trace: \033[2m (dim) - dark gray
        "Trace" => Color::Rgb(128, 128, 128),
        // Test: no specific color in ClickHouse
        "Test" => Color::Reset,
        _ => Color::Reset,
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

// Serialize a style into ANSI SGR parameters ("" for unstyled), so that the
// shared/saved text reproduces the TUI colors (e.g. in pastila's .terminal
// renderer).
fn ansi_sgr_params(style: &Style) -> String {
    fn push_color(params: &mut Vec<String>, color: Option<Color>, base: u8) {
        let Some(color) = color else {
            return;
        };
        let named = |c: u8| (base + c).to_string();
        let bright = |c: u8| (base + 60 + c).to_string();
        match color {
            Color::Reset => {}
            Color::Black => params.push(named(0)),
            Color::Red => params.push(named(1)),
            Color::Green => params.push(named(2)),
            Color::Yellow => params.push(named(3)),
            Color::Blue => params.push(named(4)),
            Color::Magenta => params.push(named(5)),
            Color::Cyan => params.push(named(6)),
            Color::Gray => params.push(named(7)),
            Color::DarkGray => params.push(bright(0)),
            Color::LightRed => params.push(bright(1)),
            Color::LightGreen => params.push(bright(2)),
            Color::LightYellow => params.push(bright(3)),
            Color::LightBlue => params.push(bright(4)),
            Color::LightMagenta => params.push(bright(5)),
            Color::LightCyan => params.push(bright(6)),
            Color::White => params.push(bright(7)),
            Color::Rgb(r, g, b) => params.push(format!("{};2;{};{};{}", base + 8, r, g, b)),
            Color::Indexed(i) => params.push(format!("{};5;{}", base + 8, i)),
        }
    }

    let mut params = Vec::new();
    for (modifier, code) in [
        (Modifier::BOLD, 1),
        (Modifier::DIM, 2),
        (Modifier::ITALIC, 3),
        (Modifier::UNDERLINED, 4),
        (Modifier::SLOW_BLINK, 5),
        (Modifier::REVERSED, 7),
        (Modifier::CROSSED_OUT, 9),
    ] {
        if style.add_modifier.contains(modifier) {
            params.push(code.to_string());
        }
    }
    push_color(&mut params, style.fg, 30);
    push_color(&mut params, style.bg, 40);
    params.join(";")
}

struct IdentifierMaps {
    query_id_map: HashMap<String, String>,
    logger_name_map: HashMap<String, String>,
    level_map: HashMap<String, String>,
    host_name_map: HashMap<String, String>,
}

// Maximum display width of each log column, so that every line can be padded
// to a common grid (host is only rendered in cluster mode)
#[derive(Default, Clone, Copy, PartialEq)]
struct ColumnWidths {
    host: usize,
    thread: usize,
    query_id: usize,
    level: usize,
    logger: usize,
}

// Pad the field that started at display offset `start` to `width` columns
fn pad_column(line: &mut StyledString, start: usize, width: usize) {
    let written = line.width() - start;
    if written < width {
        line.append_plain(" ".repeat(width - written));
    }
}

// Renders the line and also returns the display offsets where each seekable
// column starts, always 7 entries: date, time, thread_id, query_id, level,
// logger_name, message (used for horizontal seeking by columns, which relies
// on the positions being stable across lines).
// With column_widths set, each field is padded to the column width so that
// all lines share the same grid.
fn render_entry(
    entry: &LogEntry,
    cluster: bool,
    identifier_maps: Option<&IdentifierMaps>,
    column_widths: Option<&ColumnWidths>,
) -> (StyledString, Vec<usize>) {
    let mut line = StyledString::new();
    let mut column_offsets = Vec::with_capacity(7);

    if cluster {
        line.append_plain("[");
        let start = line.width();
        let host_hash = string_hash(&entry.host_name);
        let host_color = hash_to_color(host_hash);
        let display_name = entry.display_host_name.as_ref().unwrap_or(&entry.host_name);
        line.append_styled(display_name.clone(), host_color);

        if let Some(maps) = identifier_maps
            && let Some(id) = maps.host_name_map.get(&entry.host_name)
        {
            line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
        }
        if let Some(widths) = column_widths {
            pad_column(&mut line, start, widths.host);
        }
        line.append_plain("] ");
    }

    // Format timestamp with microseconds matching ClickHouse format: YYYY.MM.DD HH:MM:SS.microseconds
    let dt = entry.event_time_microseconds;
    let microseconds = dt.timestamp_subsec_micros();
    column_offsets.push(line.width());
    line.append_plain(format!(
        "{:04}.{:02}.{:02} ",
        dt.year(),
        dt.month(),
        dt.day()
    ));
    column_offsets.push(line.width());
    line.append_plain(format!(
        "{:02}:{:02}:{:02}.{:06} ",
        dt.hour(),
        dt.minute(),
        dt.second(),
        microseconds
    ));

    // Thread ID with hash-based coloring: [ thread_id ]
    column_offsets.push(line.width());
    line.append_plain("[ ");
    let thread_hash = int_hash_64(entry.thread_id);
    let thread_color = hash_to_color(thread_hash);
    let thread_str = format!("{}", entry.thread_id);
    // Numbers are right-aligned
    if let Some(widths) = column_widths
        && widths.thread > thread_str.len()
    {
        line.append_plain(" ".repeat(widths.thread - thread_str.len()));
    }
    line.append_styled(thread_str, thread_color);
    line.append_plain(" ] ");

    // Query ID with hash-based coloring: {query_id}
    // ClickHouse writes query_id even if empty for log parser convenience
    column_offsets.push(line.width());
    line.append_plain("{");
    let start = line.width();
    let query_id_str = entry.query_id.as_deref().unwrap_or("");
    if !query_id_str.is_empty() {
        let query_hash = string_hash(query_id_str);
        let query_color = hash_to_color(query_hash);
        line.append_styled(query_id_str.to_string(), query_color);

        if let Some(maps) = identifier_maps
            && let Some(id) = maps.query_id_map.get(query_id_str)
        {
            line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
        }
    }
    if let Some(widths) = column_widths {
        pad_column(&mut line, start, widths.query_id);
    }
    line.append_plain("} ");

    // Priority level with color: <level>
    column_offsets.push(line.width());
    line.append_plain("<");
    let start = line.width();
    let level_color = get_level_color(entry.level.as_str());
    line.append_styled(entry.level.clone(), level_color);
    if let Some(maps) = identifier_maps
        && let Some(id) = maps.level_map.get(&entry.level)
    {
        line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
    }
    if let Some(widths) = column_widths {
        pad_column(&mut line, start, widths.level);
    }
    line.append_plain("> ");

    // Logger name (source) with hash-based coloring: source:
    if let Some(logger_name) = &entry.logger_name {
        column_offsets.push(line.width());
        let start = line.width();
        let logger_hash = string_hash(logger_name);
        let logger_color = hash_to_color(logger_hash);
        line.append_styled(logger_name.clone(), logger_color);

        if let Some(maps) = identifier_maps
            && let Some(id) = maps.logger_name_map.get(logger_name)
        {
            line.append_styled(format!("[{}]", id), Color::Rgb(255, 255, 0));
        }
        line.append_plain(": ");
        if let Some(widths) = column_widths {
            pad_column(&mut line, start, widths.logger + 2);
        }
    } else {
        column_offsets.push(line.width());
        if let Some(widths) = column_widths
            && widths.logger > 0
        {
            // Keep the message aligned with the lines that do have a logger
            line.append_plain(" ".repeat(widths.logger + 2));
        }
    }

    // Message
    column_offsets.push(line.width());
    line.append_plain(entry.message.clone());
    return (line, column_offsets);
}

// One display row of a (possibly wrapped) rendered log entry.
type Row = Vec<Span<'static>>;

// Character-based wrapping (unlike cursive's word wrapping): every function
// counting or addressing rows must use this, so the mapping display_row ->
// (entry, row-within-entry) stays consistent.
fn wrap_styled(styled: &StyledString, width: usize) -> Vec<Row> {
    let width = width.max(1);
    let mut rows = Vec::new();
    for line in &styled.as_text().lines {
        let mut current: Row = Vec::new();
        let mut current_width = 0usize;
        for span in &line.spans {
            let mut chunk = String::new();
            for ch in span.content.chars() {
                let char_width = ch.width().unwrap_or(0);
                if current_width + char_width > width && current_width > 0 {
                    if !chunk.is_empty() {
                        current.push(Span::styled(std::mem::take(&mut chunk), span.style));
                    }
                    rows.push(std::mem::take(&mut current));
                    current_width = 0;
                }
                chunk.push(ch);
                current_width += char_width;
            }
            if !chunk.is_empty() {
                current.push(Span::styled(chunk, span.style));
            }
        }
        rows.push(current);
    }
    if rows.is_empty() {
        rows.push(Row::new());
    }
    rows
}

fn row_width(row: &Row) -> usize {
    row.iter().map(|span| span.content.width()).sum()
}

// Match highlight, same theme as less(1): black text over the original text
// color (white for unstyled text)
fn invert_style(style: Style) -> Style {
    let bg = if style == Style::default() {
        Color::Rgb(255, 255, 255)
    } else {
        style.fg.unwrap_or(Color::Reset)
    };
    Style::default().fg(Color::Rgb(0, 0, 0)).bg(bg)
}

#[derive(Clone)]
enum FilterType {
    QueryId(String),
    LoggerName(String),
    Level(String),
    HostName(String),
}

#[derive(Clone, Copy, PartialEq)]
enum ScrollStrategy {
    // Descending mode: the newest log goes on top, pin the viewport there
    StickToTop,
    // Ascending mode: follow the tail
    StickToBottom,
    // User scrolled away: keep the offset as-is
    KeepRow,
}

// Row-based scroll state (usize offsets: log row counts exceed u16)
struct ScrollCore {
    offset_x: usize,
    offset_y: usize,
    strategy: ScrollStrategy,
    viewport_w: usize,
    viewport_h: usize,
    content_w: usize,
    content_h: usize,
    scroll_x: bool,
}

impl Default for ScrollCore {
    fn default() -> Self {
        Self {
            offset_x: 0,
            offset_y: 0,
            strategy: ScrollStrategy::KeepRow,
            viewport_w: 0,
            viewport_h: 0,
            content_w: 0,
            content_h: 0,
            scroll_x: false,
        }
    }
}

impl ScrollCore {
    fn max_offset_y(&self) -> usize {
        self.content_h.saturating_sub(self.viewport_h)
    }

    fn max_offset_x(&self) -> usize {
        if self.scroll_x {
            self.content_w.saturating_sub(self.viewport_w)
        } else {
            0
        }
    }

    fn set_offset(&mut self, x: usize, y: usize) {
        self.offset_x = x.min(self.max_offset_x());
        self.offset_y = y.min(self.max_offset_y());
    }

    fn can_scroll_up(&self) -> bool {
        self.offset_y > 0
    }

    fn can_scroll_down(&self) -> bool {
        self.offset_y < self.max_offset_y()
    }

    fn scroll_up(&mut self, n: usize) {
        self.offset_y = self.offset_y.saturating_sub(n);
    }

    fn scroll_down(&mut self, n: usize) {
        self.offset_y = usize::min(self.max_offset_y(), self.offset_y + n);
    }

    // Apply the scroll strategy and keep the offset within the content
    fn adjust(&mut self) {
        match self.strategy {
            ScrollStrategy::StickToTop => self.offset_y = 0,
            ScrollStrategy::StickToBottom => self.offset_y = self.max_offset_y(),
            ScrollStrategy::KeepRow => {}
        }
        self.offset_x = self.offset_x.min(self.max_offset_x());
        self.offset_y = self.offset_y.min(self.max_offset_y());
    }
}

pub struct LogViewBase {
    max_width: usize,
    // Largest message-column start: the horizontal scroll range is extended
    // to max_message_offset + viewport width, so that seeking can bring any
    // column to the left edge even when the lines (almost) fit the screen
    // (the area right of a line's end is just blank)
    max_message_offset: usize,

    scroll: ScrollCore,
    // Last drawn area in absolute screen coordinates (mouse hit-testing)
    last_area: Rect,

    needs_relayout: bool,

    search_direction_forward: bool,
    search_regex: Option<Regex>,
    matched_row: Option<usize>,
    matched_col: Option<usize>,
    matched_len: usize,
    cluster: bool,
    wrap: bool,
    no_strip_hostname_suffix: bool,
    descending: bool,

    // Pad columns to a common width (of the widest value seen so far)
    align_columns: bool,
    // Widths of the raw values, and the effective widths used for rendering
    // (in filter mode the latter also accounts for the "[q1]" identifier tags)
    raw_column_widths: ColumnWidths,
    column_widths: ColumnWidths,

    // True until the first fetch finishes, to distinguish "Loading..." from "No logs"
    loading: bool,

    // Filter mode state
    filter_mode: bool,
    filter_identifiers: HashMap<String, FilterType>,
    active_filter: Option<FilterType>,

    logs: LogStore,

    // Descending mode: entry index where the next streamed block of the
    // current fetch is inserted (blocks of one fetch arrive newest-first, so
    // each subsequent block goes right after the previous one, in front of
    // the older pre-existing logs)
    stream_insert_pos: usize,

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
            max_message_offset: 0,
            scroll: ScrollCore::default(),
            last_area: Rect::default(),
            needs_relayout: false,
            search_direction_forward: false,
            search_regex: None,
            matched_row: None,
            matched_col: None,
            matched_len: 0,
            cluster: false,
            wrap: false,
            no_strip_hostname_suffix: false,
            descending: false,
            align_columns: false,
            raw_column_widths: ColumnWidths::default(),
            column_widths: ColumnWidths::default(),
            loading: true,
            filter_mode: false,
            filter_identifiers: HashMap::new(),
            active_filter: None,
            logs: LogStore::new(),
            stream_insert_pos: 0,
            filtered_log_indices: Vec::new(),
            log_cumulative_rows: Vec::new(),
            last_computed_width: usize::MAX,
        }
    }
}

impl LogViewBase {
    // Call f with the log at the given visible index (logs live on disk, so
    // only a borrow scoped to the store's cache can be handed out).
    // If filtering is active, maps through filtered_log_indices
    fn with_visible_log<R>(&self, visible_idx: usize, f: impl FnOnce(&LogEntry) -> R) -> Option<R> {
        if self.filtered_log_indices.is_empty() {
            self.logs.with_entry(visible_idx, f)
        } else {
            self.filtered_log_indices
                .get(visible_idx)
                .and_then(|&idx| self.logs.with_entry(idx, f))
        }
    }

    fn render_log(
        &self,
        log: &LogEntry,
        identifier_maps: Option<&IdentifierMaps>,
    ) -> (StyledString, Vec<usize>) {
        render_entry(
            log,
            self.cluster,
            identifier_maps,
            self.align_columns.then_some(&self.column_widths),
        )
    }

    // Recompute the effective column widths; on change the row cache is
    // invalidated (the cached widths/rows were rendered with the old padding)
    fn refresh_column_widths(&mut self) {
        if !self.align_columns {
            return;
        }

        let mut widths = self.raw_column_widths;
        if self.filter_mode {
            // In filter mode every distinct value gets a "[q1]"-style tag
            // appended, so reserve the widest tag per category on top of the
            // raw width (a slight over-estimation for the widest value)
            let mut tags = ColumnWidths::default();
            for (id, filter_type) in &self.filter_identifiers {
                let tag = id.width() + 2;
                let max_tag = match filter_type {
                    FilterType::QueryId(_) => &mut tags.query_id,
                    FilterType::LoggerName(_) => &mut tags.logger,
                    FilterType::Level(_) => &mut tags.level,
                    FilterType::HostName(_) => &mut tags.host,
                };
                *max_tag = usize::max(*max_tag, tag);
            }
            widths.host += tags.host;
            widths.query_id += tags.query_id;
            widths.level += tags.level;
            widths.logger += tags.logger;
        }

        if widths != self.column_widths {
            self.column_widths = widths;
            self.log_cumulative_rows.clear();
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

    // Horizontal seeking by columns: jump to the start of the next/previous
    // column (date, time, thread_id, query_id, level, logger_name); past the
    // last column seek by half of the viewport width.
    //
    // Field widths vary per line (empty query_id vs UUID, thread ids, ...),
    // so the seek grid is the per-column maximum of the start offsets across
    // the visible lines: each stop is past the given field of every line.
    fn seek_columns(&mut self, forward: bool) {
        let x = self.scroll.offset_x;
        let top = self.scroll.offset_y;
        let bottom = top + self.scroll.viewport_h.saturating_sub(1);
        let half = self.scroll.viewport_w / 2;

        let identifier_maps = self.get_identifier_maps();
        let mut offsets: Vec<usize> = Vec::new();
        let mut prev_log_idx = usize::MAX;
        for row in top..=bottom {
            let Some((log_idx, _)) = self.display_row_to_log(row) else {
                break;
            };
            if log_idx == prev_log_idx {
                continue;
            }
            prev_log_idx = log_idx;

            let line_offsets = self
                .with_visible_log(log_idx, |log| {
                    self.render_log(log, identifier_maps.as_ref()).1
                })
                .unwrap_or_default();
            if offsets.len() < line_offsets.len() {
                offsets.resize(line_offsets.len(), 0);
            }
            for (offset, line_offset) in offsets.iter_mut().zip(line_offsets) {
                *offset = usize::max(*offset, line_offset);
            }
        }

        let target = if forward {
            offsets
                .iter()
                .copied()
                .find(|&offset| offset > x)
                .unwrap_or(x + half)
        } else {
            let last = offsets.last().copied().unwrap_or(0);
            if x > last {
                usize::max(last, x.saturating_sub(half))
            } else {
                offsets
                    .iter()
                    .rev()
                    .copied()
                    .find(|&offset| offset < x)
                    .unwrap_or(0)
            }
        };
        self.scroll.set_offset(target, top);
    }

    fn extract_identifiers(&mut self) {
        let mut query_ids: HashMap<String, usize> = HashMap::new();
        let mut logger_names: HashMap<String, usize> = HashMap::new();
        let mut levels: HashMap<String, usize> = HashMap::new();
        let mut host_names: HashMap<String, usize> = HashMap::new();

        for i in 0..self.logs.len() {
            self.logs.with_entry(i, |log| {
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
            });
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
        self.refresh_column_widths();
        self.filtered_log_indices.clear();
        self.needs_relayout = true;
        self.compute_rows();
    }

    fn rebuild_content_normal(&mut self) {
        self.refresh_column_widths();
        self.filtered_log_indices.clear();
        self.needs_relayout = true;
        self.compute_rows();
    }

    fn apply_filter(&mut self) {
        self.refresh_column_widths();
        self.filtered_log_indices.clear();

        if let Some(ref filter) = self.active_filter {
            let mut indices = Vec::new();
            for idx in 0..self.logs.len() {
                let matches = self.logs.with_entry(idx, |log| match filter {
                    FilterType::QueryId(val) => log.query_id.as_ref() == Some(val),
                    FilterType::LoggerName(val) => log.logger_name.as_ref() == Some(val),
                    FilterType::Level(val) => &log.level == val,
                    FilterType::HostName(val) => &log.host_name == val,
                });
                if matches == Some(true) {
                    indices.push(idx);
                }
            }
            self.filtered_log_indices = indices;
        }

        self.needs_relayout = true;
        self.compute_rows();
    }

    fn search_in_direction(&mut self, forward: bool) -> bool {
        if self.search_regex.is_none() {
            return false;
        }

        let start_row = self.matched_row.unwrap_or(self.scroll.offset_y);
        let start_log_idx = self
            .display_row_to_log(start_row)
            .map(|(idx, _)| idx)
            .unwrap_or(0);

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
        let styled = self.with_visible_log(log_idx, |log| {
            self.render_log(log, identifier_maps.as_ref()).0
        });
        if let Some(styled) = styled {
            let display_row_start = self.log_to_display_row(log_idx);
            let rows = wrap_styled(&styled, self.last_computed_width);

            if forward {
                for (row_within_log, row) in rows.iter().enumerate() {
                    let current_row = display_row_start + row_within_log;
                    if log_idx == start_log_idx && Some(current_row) <= self.matched_row {
                        continue;
                    }

                    if self.search_row(row, current_row, forward) {
                        return true;
                    }
                }
            } else {
                for (row_within_log, row) in rows.iter().enumerate().rev() {
                    let current_row = display_row_start + row_within_log;

                    if log_idx == start_log_idx && Some(current_row) >= self.matched_row {
                        continue;
                    }

                    if self.search_row(row, current_row, forward) {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn search_row(&mut self, row: &Row, current_row: usize, forward: bool) -> bool {
        let re = match &self.search_regex {
            Some(re) => re,
            None => return false,
        };
        let mut x = 0;
        for span in row {
            let content: &str = span.content.as_ref();
            if let Some(m) = re.find(content) {
                self.matched_row = Some(current_row);
                self.matched_col = Some(x + content[..m.start()].width());
                self.matched_len = m.as_str().width();
                log::trace!(
                    "search regex matched_row: {:?} ({}-search)",
                    self.matched_row,
                    if forward { "forward" } else { "reverse" }
                );
                return true;
            }
            x += content.width();
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

    fn push_logs(&mut self, mut logs: Vec<LogEntry>, new_batch: bool) {
        log::trace!("Add {} log entries", logs.len());

        if logs.is_empty() {
            return;
        }

        // In descending mode the "head" is the top of the viewport, otherwise it's the bottom.
        let old_total_rows = self.log_cumulative_rows.last().copied().unwrap_or(0);
        let viewport_top = self.scroll.offset_y;
        let viewport_bottom = viewport_top + self.scroll.viewport_h.saturating_sub(1);
        let at_head = if self.descending {
            viewport_top == 0
        } else {
            old_total_rows == 0 || viewport_bottom + 1 >= old_total_rows
        };
        // If the user scrolled away from the head, pin the viewport so incoming rows do
        // not yank them around (for DESC we still need to shift below, since prepending
        // rotates every row index).
        if !at_head {
            self.scroll.strategy = ScrollStrategy::KeepRow;
        }

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

        {
            let widths = &mut self.raw_column_widths;
            for log in &logs {
                let host = log.display_host_name.as_ref().unwrap_or(&log.host_name);
                widths.host = usize::max(widths.host, host.width());
                widths.thread = usize::max(
                    widths.thread,
                    (log.thread_id.checked_ilog10().unwrap_or(0) + 1) as usize,
                );
                if let Some(ref query_id) = log.query_id {
                    widths.query_id = usize::max(widths.query_id, query_id.width());
                }
                widths.level = usize::max(widths.level, log.level.width());
                if let Some(ref logger_name) = log.logger_name {
                    widths.logger = usize::max(widths.logger, logger_name.width());
                }
            }
        }
        self.refresh_column_widths();

        if new_batch {
            self.stream_insert_pos = 0;
        }
        // Row where the new entries land, in pre-push coordinates (best-effort
        // under an active filter, where log_cumulative_rows indexes visible
        // entries only): inserts below the viewport must not shift it.
        let insert_row = if self.stream_insert_pos == 0 {
            0
        } else {
            self.log_cumulative_rows
                .get(self.stream_insert_pos - 1)
                .copied()
                .unwrap_or(old_total_rows)
        };
        if self.descending {
            // The fetch arrives newest-first (ORDER BY ... DESC) block by
            // block, so each block goes right after the previous blocks of the
            // same fetch, in front of the older pre-existing logs.
            let count = logs.len();
            self.logs.insert_at(self.stream_insert_pos, logs);
            self.stream_insert_pos += count;
            // Indices of existing logs shifted, so incremental compute_rows() is unsafe.
            self.log_cumulative_rows.clear();
        } else {
            self.logs.append(logs);
        }

        if self.filter_mode {
            self.extract_identifiers();
            self.rebuild_content_with_highlights();
        } else if self.active_filter.is_some() {
            self.apply_filter();
        } else {
            self.needs_relayout = true;
            self.compute_rows();
        }

        // After inserting above the viewport, shift it down by the number of rows
        // added so the user keeps looking at the same logical entry they were
        // reading before.
        if self.descending && !at_head && insert_row <= viewport_top {
            let new_total_rows = self.log_cumulative_rows.last().copied().unwrap_or(0);
            let delta = new_total_rows.saturating_sub(old_total_rows);
            if delta > 0 {
                let x = self.scroll.offset_x;
                let y = self.scroll.offset_y + delta;
                self.scroll.set_offset(x, y);
            }
        }
    }

    fn wrap_width(&self) -> usize {
        if self.wrap {
            self.scroll.viewport_w
        } else {
            usize::MAX
        }
    }

    fn compute_rows(&mut self) {
        let width = self.wrap_width();

        // On resize/wrap change row indices shift, so the old matched_row is invalid
        if self.matched_row.is_some() && self.last_computed_width != width {
            self.matched_row = None;
        }

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
        let mut max_message_offset = if can_do_incremental {
            self.max_message_offset
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
        // NOTE: a non-incremental pass (resize, wrap or filter toggle) re-reads
        // every entry from the store, i.e. the whole backing file.
        for i in start_idx..visible_count {
            let counts = self.with_visible_log(i, |log| {
                let (styled, offsets) = self.render_log(log, identifier_maps.as_ref());

                let mut row_count = 0;
                let mut row_max_width = 0;
                for row in wrap_styled(&styled, width) {
                    row_max_width = usize::max(row_max_width, row_width(&row));
                    row_count += 1;
                }
                (
                    row_count,
                    row_max_width,
                    offsets.last().copied().unwrap_or(0),
                )
            });
            if let Some((row_count, row_max_width, message_offset)) = counts {
                max_width = usize::max(max_width, row_max_width);
                max_message_offset = usize::max(max_message_offset, message_offset);
                cumulative += row_count;
                self.log_cumulative_rows.push(cumulative);
            }
        }

        self.max_width = max_width;
        self.max_message_offset = max_message_offset;
        self.last_computed_width = width;

        // Update the scrollable extent right away, so that offset clamping
        // (e.g. the KeepRow shift in push_logs) sees the new content size even
        // before the next draw.
        self.update_content_size();

        log::trace!(
            "Updating rows cache (width: {:?}, wrap: {}, max width: {}, rows: {}, visible_logs: {}/{}, incremental: {}/{}, content size: {:?}, viewport: {:?})",
            width,
            self.wrap,
            max_width,
            cumulative,
            visible_count,
            self.logs.len(),
            can_do_incremental,
            start_idx,
            (self.scroll.content_w, self.scroll.content_h),
            (self.scroll.viewport_w, self.scroll.viewport_h),
        );

        // Show the horizontal scrolling
        self.needs_relayout = true;
    }

    fn update_content_size(&mut self) {
        self.scroll.content_h = self.log_cumulative_rows.last().copied().unwrap_or(0);
        let mut content_w = usize::max(self.max_width, self.scroll.viewport_w);
        // Extend the scrollable width so that the message column can reach
        // the left edge (see max_message_offset)
        if !self.wrap && self.max_message_offset > 0 {
            content_w = usize::max(content_w, self.max_message_offset + self.scroll.viewport_w);
        }
        self.scroll.content_w = content_w;
    }

    // Scroll handling shared by the key bindings (cursive scroll::on_event
    // semantics: any manual scroll resets the strategy to KeepRow)
    fn scroll_on_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(Key::Home) => {
                self.scroll.offset_y = 0;
                if self.scroll.scroll_x {
                    self.scroll.offset_x = 0;
                }
            }
            Event::Key(Key::Up) if self.scroll.can_scroll_up() => self.scroll.scroll_up(1),
            Event::Key(Key::Down) if self.scroll.can_scroll_down() => self.scroll.scroll_down(1),
            Event::Key(Key::PageUp) if self.scroll.can_scroll_up() => {
                let page = self.scroll.viewport_h;
                self.scroll.scroll_up(page);
            }
            Event::Key(Key::PageDown) if self.scroll.can_scroll_down() => {
                let page = self.scroll.viewport_h;
                self.scroll.scroll_down(page);
            }
            _ => return EventResult::Ignored,
        }
        self.scroll.strategy = ScrollStrategy::KeepRow;
        EventResult::consumed()
    }

    // Write text content from the rendered rows directly to a writer, either
    // plain or with ANSI SGR escapes reproducing the styles
    fn write_text<W: Write>(&self, writer: &mut W, ansi: bool) -> Result<()> {
        let visible_count = self.visible_log_count();

        for i in 0..visible_count {
            let result = self.with_visible_log(i, |log| -> Result<()> {
                let styled = self.render_log(log, None).0;

                for row in wrap_styled(&styled, self.last_computed_width) {
                    let mut current = String::new();
                    for span in &row {
                        if ansi {
                            let params = ansi_sgr_params(&span.style);
                            if params != current {
                                if !current.is_empty() {
                                    writer.write_all(b"\x1b[0m")?;
                                }
                                if !params.is_empty() {
                                    write!(writer, "\x1b[{}m", params)?;
                                }
                                current = params;
                            }
                        }
                        writer.write_all(span.content.as_bytes())?;
                    }
                    if !current.is_empty() {
                        writer.write_all(b"\x1b[0m")?;
                    }
                    writer.write_all(b"\n")?;
                }
                Ok(())
            });
            result.transpose()?;
        }
        Ok(())
    }

    fn draw_content(&self, canvas: &mut Canvas<'_>, area: Rect) {
        let start_row = self.scroll.offset_y;
        let end_row = start_row + area.height as usize;
        let total_rows = self.log_cumulative_rows.last().copied().unwrap_or(0);

        let identifier_maps = self.get_identifier_maps();

        for display_row in start_row..end_row.min(total_rows) {
            // Binary search to find which log this display row belongs to
            if let Some((log_idx, row_within_log)) = self.display_row_to_log(display_row)
                && let Some(styled) = self.with_visible_log(log_idx, |log| {
                    self.render_log(log, identifier_maps.as_ref()).0
                })
                && let Some(row) =
                    wrap_styled(&styled, self.last_computed_width).get(row_within_log)
            {
                let y = area.y + (display_row - start_row) as u16;
                self.print_row(canvas, area, y, row);
            }
        }
    }

    fn print_row(&self, canvas: &mut Canvas<'_>, area: Rect, y: u16, row: &Row) {
        // Virtual column within the (unscrolled) content
        let mut col = 0usize;
        for span in row {
            let content: &str = span.content.as_ref();
            if let Some(ref re) = self.search_regex {
                let mut last_pos = 0;
                let mut has_match = false;

                for m in re.find_iter(content) {
                    has_match = true;
                    if m.start() > last_pos {
                        self.print_clipped(
                            canvas,
                            area,
                            y,
                            &mut col,
                            &content[last_pos..m.start()],
                            span.style,
                        );
                    }
                    self.print_clipped(
                        canvas,
                        area,
                        y,
                        &mut col,
                        m.as_str(),
                        invert_style(span.style),
                    );
                    last_pos = m.end();
                }

                if has_match {
                    if last_pos < content.len() {
                        self.print_clipped(
                            canvas,
                            area,
                            y,
                            &mut col,
                            &content[last_pos..],
                            span.style,
                        );
                    }
                } else {
                    // No match in this span, print normally
                    self.print_clipped(canvas, area, y, &mut col, content, span.style);
                }
            } else {
                self.print_clipped(canvas, area, y, &mut col, content, span.style);
            }
        }
    }

    // Print `text` at the virtual column `*col`, skipping what is scrolled out
    // to the left and clipping at the right edge of `area`.
    fn print_clipped(
        &self,
        canvas: &mut Canvas<'_>,
        area: Rect,
        y: u16,
        col: &mut usize,
        text: &str,
        style: Style,
    ) {
        let offset_x = self.scroll.offset_x;
        for ch in text.chars() {
            let char_width = ch.width().unwrap_or(0);
            let start = *col;
            *col += char_width;
            // A wide char crossing the left edge is dropped entirely
            if start < offset_x {
                continue;
            }
            let x = area.x as usize + (start - offset_x);
            if x >= area.right() as usize {
                continue;
            }
            let mut buf = [0u8; 4];
            print_str(
                canvas.buf,
                x as u16,
                y,
                area,
                ch.encode_utf8(&mut buf),
                style,
            );
        }
    }

    fn draw_scrollbars(&self, canvas: &mut Canvas<'_>, area: Rect) {
        let viewport_h = self.scroll.viewport_h;
        let viewport_w = self.scroll.viewport_w;

        if area.width > 0 && self.scroll.content_h > viewport_h && viewport_h > 0 {
            crate::tui::scroll::draw_scrollbar_v(
                canvas.buf,
                area.right() - 1,
                area.y,
                self.scroll.content_h,
                viewport_h,
                self.scroll.offset_y,
            );
        }

        if area.height > 0
            && self.scroll.scroll_x
            && self.scroll.content_w > viewport_w
            && viewport_w > 0
        {
            crate::tui::scroll::draw_scrollbar_h(
                canvas.buf,
                area.bottom() - 1,
                area.x,
                self.scroll.content_w,
                viewport_w,
                self.scroll.offset_x,
            );
        }
    }
}

impl Component for LogViewBase {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, _focused: bool) {
        self.last_area = area;
        // The rightmost column is reserved for the vertical scrollbar, the
        // bottom row for the horizontal one (only without wrapping).
        self.scroll.viewport_w = area.width.saturating_sub(1) as usize;
        self.scroll.viewport_h = if self.wrap {
            area.height as usize
        } else {
            area.height.saturating_sub(1) as usize
        };
        self.scroll.scroll_x = !self.wrap;

        if self.needs_relayout || self.wrap_width() != self.last_computed_width {
            self.compute_rows();
            self.needs_relayout = false;
        }
        self.update_content_size();
        self.scroll.adjust();

        // Keep the current match visible: pin the viewport to the matched row
        // and only adjust the horizontal scroll if the match is out of view.
        if let Some(matched_row) = self.matched_row {
            let match_start = self.matched_col.unwrap_or(0);
            let match_end = match_start + self.matched_len;
            let viewport_width = self.scroll.viewport_w;
            let current_offset = self.scroll.offset_x;

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

            self.scroll.set_offset(x_offset, matched_row);
        }

        if self.visible_log_count() == 0 {
            let text = if self.loading {
                "Loading..."
            } else {
                "No logs"
            };
            let x = area.x + (area.width.saturating_sub(str_width(text) as u16)) / 2;
            let y = area.y + area.height.saturating_sub(1) / 2;
            print_str(
                canvas.buf,
                x,
                y,
                area,
                text,
                Style::default().fg(Color::Rgb(128, 128, 128)),
            );
            return;
        }

        let content_area = Rect::new(
            area.x,
            area.y,
            self.scroll.viewport_w as u16,
            self.scroll.viewport_h as u16,
        );
        self.draw_content(canvas, content_area);
        self.draw_scrollbars(canvas, area);
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        let Event::Mouse { position, event } = event else {
            return EventResult::Ignored;
        };
        if !self
            .last_area
            .contains(Position::new(position.x, position.y))
        {
            return EventResult::Ignored;
        }
        match event {
            MouseEvent::WheelUp if self.scroll.can_scroll_up() => {
                self.matched_row = None;
                self.scroll.scroll_up(3);
            }
            MouseEvent::WheelDown if self.scroll.can_scroll_down() => {
                self.matched_row = None;
                self.scroll.scroll_down(3);
            }
            _ => return EventResult::Ignored,
        }
        self.scroll.strategy = ScrollStrategy::KeepRow;
        EventResult::consumed()
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}

fn show_options(app: &mut App) {
    show_bottom_prompt(app, "-", |app: &mut App, text: &str| {
        let status = app.call_on_name("logs", |base: &mut LogViewBase| {
            let status = base.set_options(text);
            base.compute_rows();
            return status;
        });
        app.pop_layer();
        if let Some(Err(err)) = status {
            app.add_layer(Dialog::info(err.to_string()));
        }
    });
}

fn search_prompt_impl(app: &mut App, forward: bool) {
    show_bottom_prompt(app, "/", move |app: &mut App, text: &str| {
        let re = match Regex::new(text) {
            Ok(re) => re,
            Err(err) => {
                app.pop_layer();
                app.add_layer(Dialog::info(format!("Invalid regex: {err}")));
                return;
            }
        };
        let found = app.call_on_name("logs", |base: &mut LogViewBase| {
            base.search_regex = Some(re);
            base.matched_row = None;
            base.matched_col = None;
            base.matched_len = 0;
            base.search_direction_forward = forward;
            base.update_search()
        });
        app.pop_layer();
        if let Some(false) = found {
            app.add_layer(Dialog::info("Pattern not found"));
        }
    });
}

fn search_prompt_forward(app: &mut App) {
    search_prompt_impl(app, /* forward= */ true);
}

fn search_prompt_reverse(app: &mut App) {
    search_prompt_impl(app, /* forward= */ false);
}

fn save_file_impl(app: &mut App) {
    let file_path = app
        .call_on_name("save_file_path", |view: &mut EditView| view.get_content())
        .unwrap_or_default();
    app.pop_layer();

    if file_path.trim().is_empty() {
        app.add_layer(Dialog::info("File path cannot be empty"));
        return;
    }

    let result = app.call_on_name("logs", |base: &mut LogViewBase| -> Result<()> {
        let mut file = fs::File::create(&file_path)?;
        base.write_text(&mut file, /* ansi= */ false)?;
        Ok(())
    });

    match result {
        Some(Ok(_)) => {
            app.add_layer(Dialog::info(format!("Logs saved to: {}", file_path)));
        }
        Some(Err(err)) => {
            app.add_layer(Dialog::info(format!("Error saving file: {}", err)));
        }
        None => {
            app.add_layer(Dialog::info("Error: Could not access log content"));
        }
    }
}

fn show_save_prompt(app: &mut App) {
    let view = EditView::new()
        .on_submit(|app: &mut App, _| save_file_impl(app))
        .with_name("save_file_path")
        .min_width(40);
    app.add_layer(
        Dialog::around(view)
            .title("Save logs to file")
            .button("Save", save_file_impl)
            .button("Cancel", |app: &mut App| {
                app.pop_layer();
            }),
    );
}

fn show_share_prompt(app: &mut App) {
    let context = app.user_data::<ContextArc>().unwrap().clone();

    let dialog = Dialog::text(format!(
        "Share logs to {} with end-to-end encryption?",
        context.clone().lock().unwrap().options.service.pastila_url
    ))
    .title("Share Logs")
    .button("Share (encrypted)", move |app: &mut App| {
        let context = context.clone();
        app.pop_layer();

        let content = app.call_on_name("logs", |base: &mut LogViewBase| -> Result<String> {
            let mut buffer = Vec::new();
            base.write_text(&mut buffer, /* ansi= */ true)?;
            Ok(String::from_utf8(buffer)?)
        });

        let content = match content {
            Some(Ok(c)) => c,
            Some(Err(e)) => {
                app.add_layer(Dialog::info(format!("Error reading logs: {}", e)));
                return;
            }
            None => {
                app.add_layer(Dialog::info("Error: Could not access log content"));
                return;
            }
        };

        if content.trim().is_empty() {
            app.add_layer(Dialog::info("No logs to share"));
            return;
        }

        let owner = context.lock().unwrap().worker.event_owner();
        context.lock().unwrap().worker.send_owned(
            &owner,
            true,
            WorkerEvent::ShareLogs(content.into()),
        );

        // The dialog holds the upload's EventOwner (captured by the
        // Cancel callback): dropping the layer on any dismissal path
        // (Cancel/Esc/q) aborts the queued or in-flight upload.
        app.add_layer(
            Dialog::text("Uploading logs...")
                .title("Please wait")
                .button("Cancel", move |app: &mut App| {
                    let _ = &owner;
                    app.pop_layer();
                })
                .with_name("uploading_logs"),
        );
    })
    .button("Cancel", |app: &mut App| {
        app.pop_layer();
    });

    app.add_layer(dialog);
}

fn toggle_filter_mode_and_prompt(app: &mut App) {
    app.call_on_name("logs", |base: &mut LogViewBase| {
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

    let should_show_prompt = app
        .call_on_name("logs", |base: &mut LogViewBase| base.filter_mode)
        .unwrap_or(false);

    if should_show_prompt {
        let apply_filter = move |app: &mut App, text: &str| {
            let identifier = text.trim().to_string();
            app.pop_layer();

            if identifier.is_empty() {
                app.call_on_name("logs", |base: &mut LogViewBase| {
                    base.filter_mode = false;
                    base.active_filter = None;
                    base.rebuild_content_normal();
                });
                return;
            }

            let filter_result = app.call_on_name("logs", |base: &mut LogViewBase| {
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
                app.add_layer(Dialog::info(msg));
            }
        };
        show_bottom_prompt(app, "identifier:", apply_filter);
    }
}

fn show_filtered_logs_popup(app: &mut App) {
    let context = app.user_data::<ContextArc>().unwrap().clone();

    // Ensure filter mode is active and identifiers are extracted
    app.call_on_name("logs", |base: &mut LogViewBase| {
        if !base.filter_mode {
            base.filter_mode = true;
            base.extract_identifiers();
            base.rebuild_content_with_highlights();
        }
    });

    // Get current log entry's timestamp for time range calculation
    let log_time = app.call_on_name("logs", |base: &mut LogViewBase| {
        let top_row = base.scroll.offset_y;

        if let Some((log_idx, _)) = base.display_row_to_log(top_row) {
            return base.with_visible_log(log_idx, |log| log.event_time_microseconds);
        }
        None
    });

    let Some(Some(event_time)) = log_time else {
        app.add_layer(Dialog::info("No log entry at current position"));
        return;
    };

    // Calculate time range: ±1 minute from the log entry
    let start = event_time - Duration::try_minutes(1).unwrap();
    let end = event_time + Duration::try_minutes(1).unwrap();

    let apply_adjacent_filter = move |app: &mut App, text: &str| {
        let identifier = text.trim().to_string();

        if identifier.is_empty() {
            return;
        }

        // Get the filter type for this identifier
        let filter_info = app.call_on_name("logs", |base: &mut LogViewBase| {
            base.filter_mode = false;
            base.filter_identifiers.get(&identifier).cloned()
        });

        let Some(Some(filter_type)) = filter_info else {
            app.add_layer(Dialog::info(format!("Unknown identifier: {}", identifier)));
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

        app.pop_layer();

        app.add_layer(
            Dialog::around(
                TextLogView::new("filtered_logs", context.clone(), args)
                    .with_name("filtered_logs")
                    .full_screen(),
            )
            .title(title),
        );
    };

    show_bottom_prompt(app, "(popup) identifier:", apply_adjacent_filter);
}

pub struct LogView {
    inner_view: OnEventView<NamedView<LogViewBase>>,
}

impl LogView {
    pub fn new(
        cluster: bool,
        wrap: bool,
        no_strip_hostname_suffix: bool,
        descending: bool,
        align_columns: bool,
    ) -> Self {
        let mut v = LogViewBase {
            needs_relayout: true,
            cluster,
            wrap,
            no_strip_hostname_suffix,
            descending,
            align_columns,
            ..Default::default()
        };
        // In descending mode the newest log goes on top, so pin the viewport there and
        // let incremental updates keep pushing old content down.
        v.scroll.strategy = if descending {
            ScrollStrategy::StickToTop
        } else {
            ScrollStrategy::StickToBottom
        };
        v.scroll.scroll_x = !wrap;
        // NOTE: we cannot pass mutable ref to view in search_prompt callback, sigh.
        let v = v.with_name("logs");

        fn scroll(v: &mut NamedView<LogViewBase>, e: &Event) -> Option<EventResult> {
            let base = v.get_mut();
            base.matched_row = None;
            return Some(base.scroll_on_event(e));
        }

        fn pin_to_tail(v: &mut NamedView<LogViewBase>) -> Option<EventResult> {
            let base = v.get_mut();
            base.matched_row = None;
            base.scroll.strategy = ScrollStrategy::StickToBottom;
            base.scroll.adjust();
            Some(EventResult::consumed())
        }

        let v = OnEventView::new(v)
            .on_pre_event_inner(Key::PageUp, scroll)
            .on_pre_event_inner(Key::PageDown, scroll)
            .on_pre_event_inner(Key::Left, |v, _| {
                let base = v.get_mut();
                base.matched_row = None;
                base.seek_columns(false);
                Some(EventResult::consumed())
            })
            .on_pre_event_inner(Key::Right, |v, _| {
                let base = v.get_mut();
                base.matched_row = None;
                base.seek_columns(true);
                Some(EventResult::consumed())
            })
            .on_pre_event_inner(Key::Up, scroll)
            .on_pre_event_inner(Key::Down, scroll)
            .on_pre_event_inner('j', |v, _| scroll(v, &Event::Key(Key::Down)))
            .on_pre_event_inner('k', |v, _| scroll(v, &Event::Key(Key::Up)))
            .on_pre_event_inner('g', |v, _| scroll(v, &Event::Key(Key::Home)))
            .on_pre_event_inner(Key::Home, scroll)
            .on_pre_event_inner(Key::End, |v, _| pin_to_tail(v))
            .on_pre_event_inner('G', |v, _| pin_to_tail(v))
            .on_event_inner('-', |_, _| Some(EventResult::with_cb(show_options)))
            .on_event_inner('/', |_, _| {
                Some(EventResult::with_cb(search_prompt_forward))
            })
            .on_event_inner('?', |_, _| {
                Some(EventResult::with_cb(search_prompt_reverse))
            })
            .on_event_inner('n', |v, _| {
                let base = v.get_mut();
                base.search_direction_forward = true;
                if base.update_search_forward() {
                    return Some(EventResult::consumed());
                } else {
                    return Some(EventResult::with_cb(|app| {
                        app.add_layer(Dialog::info("Pattern not found"));
                    }));
                }
            })
            .on_event_inner('N', |v, _| {
                let base = v.get_mut();
                base.search_direction_forward = false;
                if base.update_search_reverse() {
                    return Some(EventResult::consumed());
                } else {
                    return Some(EventResult::with_cb(|app| {
                        app.add_layer(Dialog::info("Pattern not found"));
                    }));
                }
            })
            .on_event_inner('s', |_, _| Some(EventResult::with_cb(show_save_prompt)))
            .on_event_inner('S', |_, _| Some(EventResult::with_cb(show_share_prompt)))
            .on_event_inner(Event::CtrlChar('f'), |_, _| {
                Some(EventResult::with_cb(toggle_filter_mode_and_prompt))
            })
            .on_event_inner(Event::CtrlChar('s'), |_, _| {
                Some(EventResult::with_cb(show_filtered_logs_popup))
            });

        let log_view = LogView { inner_view: v };
        return log_view;
    }

    pub fn push_logs(&mut self, logs: Vec<LogEntry>, new_batch: bool) {
        self.inner_view
            .get_inner_mut()
            .get_mut()
            .push_logs(logs, new_batch);
    }

    pub fn finish_loading(&mut self) {
        self.inner_view.get_inner_mut().get_mut().loading = false;
    }
}

impl Component for LogView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.inner_view.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: ratatui::layout::Size) -> ratatui::layout::Size {
        self.inner_view.required_size(max)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        // The inherent builder OnEventView::on_event shadows the trait method
        Component::on_event(&mut self.inner_view, event)
    }

    fn take_focus(&mut self) -> bool {
        self.inner_view.take_focus()
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.inner_view);
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.inner_view.focus_name(name)
    }
}
