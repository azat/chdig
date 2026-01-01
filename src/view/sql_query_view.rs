use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{BackgroundRunner, ContextArc, WorkerEvent, clickhouse::Columns};
use crate::view::TableViewItem;
use crate::view::table_view::TableView;
use crate::wrap_impl_no_move;
use chrono::{DateTime, Local};
use chrono_tz::Tz;
use clickhouse_rs::types::SqlType;
use cursive::Cursive;
use cursive::view::ViewWrapper;
use cursive::views::OnEventView;

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
        }
    }
}

#[derive(Clone, Default, Debug)]
// Fields:
// - list of fields
// - indices of fields to compare (columns_to_compare)
pub struct Row(pub Vec<Field>, Vec<usize>);

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
}

type RowCallback = Arc<dyn Fn(&mut Cursive, Vec<&'static str>, Row) + Send + Sync>;

pub struct SQLQueryView {
    table: TableView<Row, u8>,

    // Indices of columns to compare for PartialEq
    columns_to_compare: Vec<usize>,
    columns: Vec<&'static str>,
    on_submit: Option<RowCallback>,

    // Store all items and filter
    all_items: Vec<Row>,
    filter: Arc<Mutex<String>>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl SQLQueryView {
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
                    SqlType::String => Field::String(block.get::<_, _>(i, column)?),
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
                    _ => unreachable!("Type for column {} not implemented", column),
                };
                row.0.push(field);
            }
            row.1 = self.columns_to_compare.clone();
            items.push(row);
        }

        // Store all items and apply filtering
        self.all_items = items;
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

        let update_callback_context = context.clone();
        let update_callback = move |force: bool| {
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(force, WorkerEvent::SQLQuery(view_name, query.clone()));
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
            table,
            columns,
            columns_to_compare,
            on_submit: None,
            all_items: Vec::new(),
            filter: filter.clone(),
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
