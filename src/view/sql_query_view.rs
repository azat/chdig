use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{
    BackgroundRunner, ContextArc, WorkerEvent,
    clickhouse::{Columns, column_as_string},
};
use crate::view::TableViewItem;
use crate::view::table_view::TableView;
use crate::wrap_impl_no_move;
use chrono::{DateTime, Local};
use chrono_tz::Tz;
use clickhouse_rs::types::SqlType;
use cursive::Cursive;
use cursive::theme::Color;
use cursive::utils::markup::StyledString;
use cursive::view::ViewWrapper;
use cursive::views::OnEventView;

/// Physical meaning of a numeric column, used to format its value for display
/// without losing the underlying number for sorting (see Field::Quantity).
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub enum Unit {
    Count,
    Bytes,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl Unit {
    fn format(self, v: f64) -> String {
        match self {
            Unit::Count => format_count(v),
            Unit::Bytes => SizeFormatter::new()
                .with_base(Base::Base2)
                .with_style(Style::Abbreviated)
                .format(v as i64),
            Unit::Microseconds => format_duration_ms(v / 1000.0),
            Unit::Milliseconds => format_duration_ms(v),
            Unit::Seconds => format_duration_ms(v * 1000.0),
        }
    }
}

fn format_count(v: f64) -> String {
    if v.abs() < 1000.0 {
        return format!("{}", v.round() as i64);
    }
    const UNITS: [&str; 5] = ["", "K", "M", "B", "T"];
    let mut x = v;
    let mut i = 0;
    while x.abs() >= 1000.0 && i < UNITS.len() - 1 {
        x /= 1000.0;
        i += 1;
    }
    format!("{:.2}{}", x, UNITS[i])
}

fn format_duration_ms(ms: f64) -> String {
    if ms < 1000.0 {
        return format!("{:.0}ms", ms);
    }
    let s = ms / 1000.0;
    if s < 60.0 {
        return format!("{:.2}s", s);
    }
    let m = s / 60.0;
    if m < 60.0 {
        return format!("{:.1}m", m);
    }
    format!("{:.1}h", m / 60.0)
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Field {
    String(String),
    Float64(f64),
    Float32(f32),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    DateTime(DateTime<Local>),
    // Numeric value rendered through Unit::format(); ordering stays numeric.
    Quantity(f64, Unit),
    // TODO: support more types
}

impl Field {
    // TODO: write this in a better way
    pub fn as_datetime(&self) -> Option<DateTime<Local>> {
        if let Field::DateTime(dt) = self {
            Some(*dt)
        } else {
            None
        }
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: add human time formatter
        let fmt_bytes = SizeFormatter::new()
            // TODO: use Base10 for rows and Base2 for bytes
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        match *self {
            Self::String(ref value) => write!(f, "{}", value),
            Self::Float64(ref value) => write!(f, "{:.2}", value),
            Self::Float32(ref value) => write!(f, "{:.2}", value),
            Self::UInt64(ref value) => {
                if *value < 1_000 {
                    write!(f, "{}", value)
                } else {
                    write!(f, "{}", fmt_bytes.format(*value as i64))
                }
            }
            Self::UInt32(ref value) => write!(f, "{}", value),
            Self::UInt16(ref value) => write!(f, "{}", value),
            Self::UInt8(ref value) => write!(f, "{}", value),
            Self::Int64(ref value) => {
                if *value < 1_000 {
                    write!(f, "{}", value)
                } else {
                    write!(f, "{}", fmt_bytes.format(*value))
                }
            }
            Self::Int32(ref value) => write!(f, "{}", value),
            Self::Int16(ref value) => write!(f, "{}", value),
            Self::Int8(ref value) => write!(f, "{}", value),
            Self::DateTime(ref value) => write!(f, "{}", value),
            Self::Quantity(value, unit) => write!(f, "{}", unit.format(value)),
        }
    }
}

#[derive(Clone, Default, Debug)]
// Fields:
// - list of fields
// - indices of fields to compare (columns_to_compare)
// - row color (see set_color_log_scale())
// - per-cell styled content for one column (see set_heatmap_column())
pub struct Row(
    pub Vec<Field>,
    Vec<usize>,
    Option<Color>,
    Option<(usize, StyledString)>,
);

impl PartialEq<Row> for Row {
    fn eq(&self, other: &Self) -> bool {
        for &idx in &self.1 {
            if self.0[idx] != other.0[idx] {
                return false;
            }
        }
        return true;
    }
}

impl TableViewItem<u8> for Row {
    fn to_column(&self, column: u8) -> String {
        return self.0[column as usize].to_string();
    }

    fn cmp(&self, other: &Self, column: u8) -> Ordering
    where
        Self: Sized,
    {
        let index = column as usize;
        let field_lhs = &self.0[index];
        let field_rhs = &other.0[index];
        return field_lhs.partial_cmp(field_rhs).unwrap();
    }

    fn to_column_styled(&self, column: u8) -> StyledString {
        if let Some((idx, ref styled)) = self.3
            && idx == column as usize
        {
            return styled.clone();
        }
        let text = self.to_column(column);
        match self.2 {
            Some(color) => StyledString::styled(text, color),
            None => StyledString::plain(text),
        }
    }
}

type RowCallback = Arc<dyn Fn(&mut Cursive, Vec<&'static str>, Row) + Send + Sync>;

/// (bar_column_name, source_column_name)
type BarColumnConfig = (&'static str, &'static str);

/// (source_column_name, palette) - rows are bucketed into palette.len() bands
/// on a logarithmic scale between the min and max of the column across the
/// current result set, so the coloring is relative to the shown rows.
type ColorScaleConfig = (&'static str, Vec<Color>);

/// (heatmap_column_name, values_column_name) - the values column holds
/// comma-separated per-time-bucket sums (clickhouse-rs cannot read arrays),
/// rendered as one colored cell per bucket, normalized by the global max
/// across the current result set.
type HeatmapColumnConfig = (&'static str, &'static str);

const BAR_WIDTH: usize = 10;
const BAR_FILLED: char = '█';
const BAR_EMPTY: char = '░';

const HEATMAP_SHADES: [char; 4] = ['░', '▒', '▓', '█'];

// Black→red→yellow→white ramp (channels saturate one after another)
fn heat_color(f: f64) -> Color {
    // Keep tiny non-zero buckets visible
    let f = f.max(0.15);
    let c = |v: f64| (v.clamp(0.0, 1.0) * 255.0) as u8;
    Color::Rgb(c(3.0 * f), c(3.0 * f - 1.0), c(3.0 * f - 2.0))
}

fn render_bar(value: f64, max: f64) -> String {
    if max <= 0.0 {
        return std::iter::repeat_n(BAR_EMPTY, BAR_WIDTH).collect();
    }
    let filled = ((value / max) * BAR_WIDTH as f64).round() as usize;
    let filled = filled.min(BAR_WIDTH);
    std::iter::repeat_n(BAR_FILLED, filled)
        .chain(std::iter::repeat_n(BAR_EMPTY, BAR_WIDTH - filled))
        .collect()
}

fn field_to_f64(field: &Field) -> f64 {
    match *field {
        Field::UInt64(v) => v as f64,
        Field::UInt32(v) => v as f64,
        Field::UInt16(v) => v as f64,
        Field::UInt8(v) => v as f64,
        Field::Int64(v) => v as f64,
        Field::Int32(v) => v as f64,
        Field::Int16(v) => v as f64,
        Field::Int8(v) => v as f64,
        Field::Float64(v) => v,
        Field::Float32(v) => v as f64,
        Field::Quantity(v, _) => v,
        _ => 0.0,
    }
}

pub struct SQLQueryView {
    context: ContextArc,
    table: TableView<Row, u8>,

    // Indices of columns to compare for PartialEq
    columns_to_compare: Vec<usize>,
    columns: Vec<&'static str>,
    on_submit: Option<RowCallback>,

    // Store all items and filter
    all_items: Vec<Row>,
    filter: Arc<Mutex<String>>,

    bar_columns: Vec<BarColumnConfig>,
    color_scale: Option<ColorScaleConfig>,
    heatmap_column: Option<HeatmapColumnConfig>,
    value_units: Vec<(&'static str, Unit)>,

    query: Arc<Mutex<String>>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl SQLQueryView {
    pub fn set_title<S: Into<String>>(&mut self, title: S) {
        self.table.set_title(title);
    }

    pub fn set_query(&mut self, query: String) {
        *self.query.lock().unwrap() = query;
    }

    pub fn update(&mut self, block: Columns) -> Result<()> {
        let mut items = Vec::new();

        for i in 0..block.row_count() {
            let mut row = Row::default();
            for &column in &self.columns {
                let sql_column = block
                    .columns()
                    .iter()
                    .find(|c| c.name() == column)
                    .ok_or(anyhow!("Cannot get {} column", column))?;
                let field = match sql_column.sql_type() {
                    SqlType::Float64 => Field::Float64(block.get::<_, _>(i, column)?),
                    SqlType::Float32 => Field::Float32(block.get::<_, _>(i, column)?),
                    SqlType::UInt64 => Field::UInt64(block.get::<_, _>(i, column)?),
                    SqlType::UInt32 => Field::UInt32(block.get::<_, _>(i, column)?),
                    SqlType::UInt16 => Field::UInt16(block.get::<_, _>(i, column)?),
                    SqlType::UInt8 => Field::UInt8(block.get::<_, _>(i, column)?),
                    SqlType::Int64 => Field::Int64(block.get::<_, _>(i, column)?),
                    SqlType::Int32 => Field::Int32(block.get::<_, _>(i, column)?),
                    SqlType::Int16 => Field::Int16(block.get::<_, _>(i, column)?),
                    SqlType::Int8 => Field::Int8(block.get::<_, _>(i, column)?),
                    SqlType::DateTime(_) => Field::DateTime(
                        block
                            .get::<DateTime<Tz>, _>(i, column)?
                            .with_timezone(&Local),
                    ),
                    // String, LowCardinality(String), Enum8/16 and UUID all render as text
                    _ => Field::String(column_as_string(&block, i, column)?),
                };
                row.0.push(field);
            }
            row.1 = self.columns_to_compare.clone();
            items.push(row);
        }

        // Store all items, compute bars/colors/heatmaps, and apply filtering
        self.all_items = items;
        self.apply_units();
        self.compute_bars();
        self.compute_colors();
        self.compute_heatmaps();
        self.apply_filter();

        return Ok(());
    }

    fn apply_filter(&mut self) {
        let filter_text = self.filter.lock().unwrap().clone();
        let filter_lower = filter_text.to_lowercase();

        let filtered_items: Vec<Row> = if filter_text.is_empty() {
            self.all_items.clone()
        } else {
            self.all_items
                .iter()
                .filter(|row| {
                    // Check if any column contains the filter text (case-insensitive)
                    row.0
                        .iter()
                        .any(|field| field.to_string().to_lowercase().contains(&filter_lower))
                })
                .cloned()
                .collect()
        };

        self.table.set_items_stable(filtered_items);
    }

    pub fn set_bar_columns(&mut self, configs: Vec<BarColumnConfig>) {
        self.bar_columns = configs;
    }

    pub fn set_color_log_scale(&mut self, column: &'static str, palette: Vec<Color>) {
        self.color_scale = Some((column, palette));
    }

    pub fn set_heatmap_column(&mut self, heatmap: &'static str, values: &'static str) {
        self.heatmap_column = Some((heatmap, values));
    }

    /// Tags a numeric column with a physical unit so its value is rendered via
    /// Unit::format() (sorting stays numeric). Re-tagging the same column
    /// overrides the previous unit (used when the column's meaning changes, e.g.
    /// the metric switch in the Query patterns view).
    pub fn set_value_unit(&mut self, column: &'static str, unit: Unit) {
        self.value_units.retain(|(c, _)| *c != column);
        self.value_units.push((column, unit));
    }

    fn apply_units(&mut self) {
        for &(column, unit) in &self.value_units {
            let Some(idx) = self.columns.iter().position(|c| *c == column) else {
                continue;
            };
            for row in &mut self.all_items {
                let v = field_to_f64(&row.0[idx]);
                row.0[idx] = Field::Quantity(v, unit);
            }
        }
    }

    /// Overrides the displayed header of a column (column names come from the
    /// SQL aliases, which cannot contain spaces).
    pub fn set_column_title(&mut self, column: &'static str, title: &str) {
        if let Some(idx) = self.columns.iter().position(|c| *c == column) {
            self.table.set_column_title(idx as u8, title.to_string());
        }
    }

    /// Pins a column to a fixed width so it stops auto-resizing when content
    /// changes (useful for columns whose values can vary across modes).
    pub fn set_column_width(&mut self, column: &'static str, width: usize) {
        if let Some(idx) = self.columns.iter().position(|c| *c == column) {
            self.table.set_column_width(idx as u8, width);
        }
    }

    fn compute_colors(&mut self) {
        // Clear stale colors before potentially short-circuiting, otherwise toggling
        // no_color back on at runtime would leave the previously-coloured rows.
        for row in &mut self.all_items {
            row.2 = None;
        }
        if self.context.lock().unwrap().options.view.no_color {
            return;
        }
        let Some((column, ref palette)) = self.color_scale else {
            return;
        };
        if palette.is_empty() {
            return;
        }
        let Some(src_idx) = self.columns.iter().position(|c| *c == column) else {
            return;
        };

        // Clamp to avoid ln(0) (e.g. query_duration_ms has only 1ms resolution anyway)
        const EPS: f64 = 1e-3;
        let values = self
            .all_items
            .iter()
            .map(|row| field_to_f64(&row.0[src_idx]).max(EPS))
            .collect::<Vec<_>>();
        let Some(&min) = values.iter().min_by(|a, b| a.total_cmp(b)) else {
            return;
        };
        let max = *values.iter().max_by(|a, b| a.total_cmp(b)).unwrap();

        let range = (max / min).ln();
        for (row, value) in self.all_items.iter_mut().zip(values) {
            let band = if range > 0. {
                ((value / min).ln() / range * palette.len() as f64) as usize
            } else {
                0
            };
            row.2 = Some(palette[band.min(palette.len() - 1)]);
        }
    }

    fn compute_bars(&mut self) {
        if self.bar_columns.is_empty() {
            return;
        }

        let resolved: Vec<(usize, usize)> = self
            .bar_columns
            .iter()
            .filter_map(|(bar_name, src_name)| {
                let bar_idx = self.columns.iter().position(|c| c == bar_name)?;
                let src_idx = self.columns.iter().position(|c| c == src_name)?;
                Some((bar_idx, src_idx))
            })
            .collect();

        for &(bar_idx, src_idx) in &resolved {
            let max = self
                .all_items
                .iter()
                .map(|row| field_to_f64(&row.0[src_idx]))
                .fold(0.0_f64, f64::max);

            for row in &mut self.all_items {
                let value = field_to_f64(&row.0[src_idx]);
                row.0[bar_idx] = Field::String(render_bar(value, max));
            }
        }
    }

    fn compute_heatmaps(&mut self) {
        // Clear stale styled cells before potentially short-circuiting, otherwise
        // toggling no_color back on at runtime would leave the previously-coloured
        // cells (see compute_colors()).
        for row in &mut self.all_items {
            row.3 = None;
        }
        let Some((hm_name, val_name)) = self.heatmap_column else {
            return;
        };
        let Some(hm_idx) = self.columns.iter().position(|c| *c == hm_name) else {
            return;
        };
        let Some(val_idx) = self.columns.iter().position(|c| *c == val_name) else {
            return;
        };
        let no_color = self.context.lock().unwrap().options.view.no_color;

        let values: Vec<Vec<f64>> = self
            .all_items
            .iter()
            .map(|row| {
                row.0[val_idx]
                    .to_string()
                    .split(',')
                    .map(|v| v.parse::<f64>().unwrap_or(0.0))
                    .collect()
            })
            .collect();
        let max = values.iter().flatten().fold(0.0_f64, |acc, &v| acc.max(v));

        for (row, values) in self.all_items.iter_mut().zip(values) {
            let mut text = String::new();
            let mut styled = StyledString::new();
            for v in values {
                if v <= 0.0 || max <= 0.0 {
                    text.push(' ');
                    styled.append_plain(" ");
                    continue;
                }
                let f = (v / max).sqrt();
                if no_color {
                    let band = ((f * HEATMAP_SHADES.len() as f64).ceil() as usize)
                        .clamp(1, HEATMAP_SHADES.len());
                    text.push(HEATMAP_SHADES[band - 1]);
                } else {
                    text.push(BAR_FILLED);
                    styled.append_styled(BAR_FILLED.to_string(), heat_color(f));
                }
            }
            row.0[hm_idx] = Field::String(text);
            if !no_color {
                row.3 = Some((hm_idx, styled));
            }
        }
    }

    pub fn set_on_submit<F>(&mut self, cb: F)
    where
        F: Fn(&mut Cursive, Vec<&'static str>, Row) + Send + Sync + 'static,
    {
        self.on_submit = Some(Arc::new(cb));
    }

    pub fn new(
        context: ContextArc,
        view_name: &'static str,
        sort_by: &'static str,
        columns: Vec<&'static str>,
        columns_to_compare: Vec<&'static str>,
        query: String,
    ) -> Result<OnEventView<Self>> {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let query = Arc::new(Mutex::new(query));

        let update_callback_context = context.clone();
        let update_callback_query = query.clone();
        let update_callback = move |force: bool| {
            let q = update_callback_query.lock().unwrap().clone();
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(force, WorkerEvent::SQLQuery(view_name, q));
        };

        let columns = parse_columns(&columns);

        // Convert column names to indices
        let columns_to_compare: Vec<usize> = columns_to_compare
            .iter()
            .map(|&col_name| {
                columns
                    .iter()
                    .position(|&c| c == col_name)
                    .unwrap_or_else(|| panic!("Column '{}' not found in columns list", col_name))
            })
            .collect();

        let mut table = TableView::<Row, u8>::new();
        for (i, column) in columns.iter().enumerate() {
            if column.starts_with('_') {
                continue;
            }
            let min_width = column.len();

            // Use width_min for columns in columns_to_compare (they should expand)
            if columns_to_compare.contains(&i) {
                table.add_column(i as u8, column.to_string(), |c| c.width_min(min_width));
            } else {
                let max_width = 20; // Reasonable max for most columns
                table.add_column(i as u8, column.to_string(), |c| {
                    c.width_min_max(min_width, max_width)
                });
            }
        }
        let sort_by_column = columns
            .iter()
            .enumerate()
            .find_map(|(i, c)| if *c == sort_by { Some(i) } else { None })
            .expect("sort_by column not found in columns");
        table.sort_by(sort_by_column as u8, Ordering::Greater);
        table.set_on_submit(|siv, _row, index| {
            if index.is_none() {
                return;
            }

            let (on_submit, columns, item) = siv
                .call_on_name(view_name, |table: &mut OnEventView<SQLQueryView>| {
                    let table = table.get_inner_mut();
                    let columns = table.columns.clone();
                    let item = table.table.borrow_item(index.unwrap()).unwrap();
                    return (table.on_submit.clone(), columns, item.clone());
                })
                .unwrap();
            if let Some(on_submit) = on_submit {
                on_submit(siv, columns, item);
            }
        });

        let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
        let bg_runner_force = context.lock().unwrap().background_runner_force.clone();
        let mut bg_runner = BackgroundRunner::new(delay, bg_runner_cv, bg_runner_force);
        bg_runner.start(update_callback);

        let filter = Arc::new(Mutex::new(String::new()));

        let view = SQLQueryView {
            context: context.clone(),
            table,
            columns,
            columns_to_compare,
            on_submit: None,
            all_items: Vec::new(),
            filter: filter.clone(),
            bar_columns: Vec::new(),
            color_scale: None,
            heatmap_column: None,
            value_units: Vec::new(),
            query,
            bg_runner,
        };

        // Wrap with OnEventView to add '/' key binding for filtering
        let event_view = OnEventView::new(view).on_event('/', move |siv: &mut Cursive| {
            let filter_cb = move |siv: &mut Cursive, text: &str| {
                siv.call_on_name(view_name, |v: &mut OnEventView<SQLQueryView>| {
                    let v = v.get_inner_mut();
                    log::info!("Set filter to '{}'", text);
                    *v.filter.lock().unwrap() = text.to_string();
                    v.apply_filter();
                });
                siv.pop_layer();
            };

            crate::view::show_bottom_prompt(siv, "/", filter_cb);
        });

        return Ok(event_view);
    }
}

impl ViewWrapper for SQLQueryView {
    wrap_impl_no_move!(self.table: TableView<Row, u8>);
}

fn parse_columns(columns: &[&'static str]) -> Vec<&'static str> {
    let mut result = Vec::new();
    for column in columns.iter() {
        // NOTE: this is broken for "x AS `foo bar`"
        let column_name = column.split(' ').next_back().unwrap();
        result.push(column_name);
    }
    return result;
}
