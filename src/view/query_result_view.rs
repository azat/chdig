use std::cmp::Ordering;

use anyhow::{anyhow, Result};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{ExtTableView, TableViewItem};
use crate::wrap_impl_no_move;
use cursive::view::ViewWrapper;

use chrono::DateTime;
use chrono_tz::Tz;

use clickhouse_rs::types::SqlType;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Field {
    String(String),
    Float64(f64),
    UInt64(u64),
    UInt32(u32),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int8(i8),
    DateTime(DateTime<Tz>),
    // TODO: support more types
}
impl ToString for Field {
    fn to_string(&self) -> String {
        // TODO: add human time formatter
        let fmt_bytes = SizeFormatter::new()
            // TODO: use Base10 for rows and Base2 for bytes
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        match self {
            &Self::String(ref value) => value.clone(),
            &Self::Float64(ref value) => format!("{:.2}", value),
            &Self::UInt64(ref value) => {
                if *value < 1_000 {
                    return value.to_string();
                }
                return fmt_bytes.format(*value as i64);
            }
            &Self::UInt32(ref value) => value.to_string(),
            &Self::UInt8(ref value) => value.to_string(),
            &Self::Int64(ref value) => {
                if *value < 1_000 {
                    return value.to_string();
                }
                return fmt_bytes.format(*value as i64);
            }
            &Self::Int32(ref value) => value.to_string(),
            &Self::Int8(ref value) => value.to_string(),
            &Self::DateTime(ref value) => value.to_string(),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct Row(Vec<Field>);

impl PartialEq<Row> for Row {
    fn eq(&self, other: &Self) -> bool {
        for it in self.0.iter().zip(other.0.iter()) {
            let (ai, bi) = it;
            if *ai != *bi {
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
        match (field_lhs, field_rhs) {
            (Field::String(ref lhs), Field::String(ref rhs)) => lhs.cmp(&rhs),
            (Field::Float64(ref lhs), Field::Float64(ref rhs)) => lhs.partial_cmp(&rhs).unwrap(),
            (Field::UInt64(ref lhs), Field::UInt64(ref rhs)) => lhs.cmp(&rhs),
            (Field::UInt32(ref lhs), Field::UInt32(ref rhs)) => lhs.cmp(&rhs),
            (Field::UInt8(ref lhs), Field::UInt8(ref rhs)) => lhs.cmp(&rhs),
            (Field::Int64(ref lhs), Field::Int64(ref rhs)) => lhs.cmp(&rhs),
            (Field::Int32(ref lhs), Field::Int32(ref rhs)) => lhs.cmp(&rhs),
            (Field::Int8(ref lhs), Field::Int8(ref rhs)) => lhs.cmp(&rhs),
            (Field::DateTime(ref lhs), Field::DateTime(ref rhs)) => lhs.cmp(&rhs),
            _ => unreachable!(
                "Type for field ({:?}, {:?}) not implemented",
                field_lhs, field_rhs
            ),
        }
    }
}

pub struct QueryResultView {
    table: ExtTableView<Row, u8>,

    columns: Vec<&'static str>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl QueryResultView {
    pub fn update(self: &mut Self, block: Columns) -> Result<()> {
        let mut items = Vec::new();

        for i in 0..block.row_count() {
            let mut row = Row::default();
            for &column in &self.columns {
                let sql_column = block
                    .columns()
                    .iter()
                    .find_map(|c| if c.name() == column { Some(c) } else { None })
                    .ok_or(anyhow!("Cannot get {} column", column))?;
                let field = match sql_column.sql_type() {
                    SqlType::String => Field::String(block.get::<_, _>(i, column)?),
                    SqlType::Float64 => Field::Float64(block.get::<_, _>(i, column)?),
                    SqlType::UInt64 => Field::UInt64(block.get::<_, _>(i, column)?),
                    SqlType::UInt32 => Field::UInt32(block.get::<_, _>(i, column)?),
                    SqlType::UInt8 => Field::UInt8(block.get::<_, _>(i, column)?),
                    SqlType::Int64 => Field::Int64(block.get::<_, _>(i, column)?),
                    SqlType::Int32 => Field::Int32(block.get::<_, _>(i, column)?),
                    SqlType::Int8 => Field::Int8(block.get::<_, _>(i, column)?),
                    SqlType::DateTime(_) => Field::DateTime(block.get::<_, _>(i, column)?),
                    _ => unreachable!("Type for column {} not implemented", column),
                };
                row.0.push(field);
            }
            items.push(row);
        }

        let inner_table = self.table.get_inner_mut().get_inner_mut();
        if inner_table.is_empty() {
            inner_table.set_items_stable(items);
            inner_table.set_selected_row(0);
        } else {
            inner_table.set_items_stable(items);
        }

        return Ok(());
    }

    pub fn new(
        context: ContextArc,
        view_name: &'static str,
        sort_by: &'static str,
        columns: Vec<&'static str>,
        query: String,
    ) -> Result<Self> {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_callback_context = context.clone();
        let update_callback = move || {
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(WorkerEvent::ViewQuery(view_name, query.clone()));
        };

        let columns = parse_columns(&columns);

        let mut table = ExtTableView::<Row, u8>::default();
        let inner_table = table.get_inner_mut().get_inner_mut();
        for (i, column) in columns.iter().enumerate() {
            inner_table.add_column(i as u8, column.to_string(), |c| c);
        }
        let sort_by_column = columns
            .iter()
            .enumerate()
            .find_map(|(i, c)| if *c == sort_by { Some(i) } else { None })
            .expect("sort_by column not found in columns");
        inner_table.sort_by(sort_by_column as u8, Ordering::Greater);

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = QueryResultView {
            table,
            columns,
            bg_runner,
        };
        return Ok(view);
    }
}

impl ViewWrapper for QueryResultView {
    wrap_impl_no_move!(self.table: ExtTableView<Row, u8>);
}

fn parse_columns(columns: &Vec<&'static str>) -> Vec<&'static str> {
    let mut result = Vec::new();
    for column in columns.iter() {
        let column_parts = column.split(' ').collect::<Vec<&str>>();
        let column_name;
        match column_parts.len() {
            1 => column_name = column,
            2 => column_name = &column_parts[1],
            _ => unreachable!("Only 'X' or 'X alias_X' is supported in columns list"),
        }
        result.push(*column_name);
    }
    return result;
}
