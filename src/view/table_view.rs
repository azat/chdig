//
// Copied from https://github.com/BonsaiDen/cursive_table_view
//
// And extended to support:
// - Adopt to recent cursive changes
// - Add ability not to follow selected item in the table
// - Column resize on mouse drag
// - Column removal on middle mouse press
// - Better navigation
//   - j/k -- for navigation
//   - PgUp/PgDown -- scroll the whole page
// - Calculate column width based on the input rows
//   - Add new constraint Min/MinMax
//

//! A basic table view implementation for [cursive](https://crates.io/crates/cursive).
#![deny(
    missing_docs,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_import_braces,
    unused_qualifications
)]

// STD Dependencies -----------------------------------------------------------
use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

// External Dependencies ------------------------------------------------------
use cursive::{
    Cursive, Printer, Rect, With,
    align::HAlign,
    direction::Direction,
    event::{Callback, Event, EventResult, Key, MouseButton, MouseEvent},
    theme,
    utils::markup::StyledString,
    vec::Vec2,
    view::{CannotFocus, View, scroll},
};

/// A trait for displaying and sorting items inside a
/// [`TableView`](struct.TableView.html).
pub trait TableViewItem<H>: Clone + Sized
where
    H: Eq + Hash + Copy + Clone + 'static,
{
    /// Method returning a string representation of the item for the
    /// specified column from type `H`.
    fn to_column(&self, column: H) -> String;

    /// Method comparing two items via their specified column from type `H`.
    fn cmp(&self, other: &Self, column: H) -> Ordering
    where
        Self: Sized;

    /// Method returning a styled string representation of the item for the
    /// specified column from type `H`. Default implementation returns unstyled text.
    fn to_column_styled(&self, column: H) -> StyledString {
        StyledString::plain(self.to_column(column))
    }
}

/// Callback used when a column is sorted.
///
/// It takes the column and the ordering as input.
///
/// This is a private type to help readability.
type OnSortCallback<H> = Arc<dyn Fn(&mut Cursive, H, Ordering) + Send + Sync>;

/// Callback taking as argument the row and the index of an element.
///
/// This is a private type to help readability.
type IndexCallback = Arc<dyn Fn(&mut Cursive, Option<usize>, Option<usize>) + Send + Sync>;

/// View to select an item among a list, supporting multiple columns for sorting.
///
/// # Examples
///
/// ```ignore
/// # extern crate cursive;
/// # use std::cmp::Ordering;
/// # use chdig::view::table_view::{TableView, TableViewItem};
/// # use cursive::align::HAlign;
/// # fn main() {
/// // Provide a type for the table's columns
/// #[derive(Copy, Clone, PartialEq, Eq, Hash)]
/// enum BasicColumn {
///     Name,
///     Count,
///     Rate
/// }
///
/// // Define the item type
/// #[derive(Clone, Debug)]
/// struct Foo {
///     name: String,
///     count: usize,
///     rate: usize
/// }
///
/// impl TableViewItem<BasicColumn> for Foo {
///
///     fn to_column(&self, column: BasicColumn) -> String {
///         match column {
///             BasicColumn::Name => self.name.to_string(),
///             BasicColumn::Count => format!("{}", self.count),
///             BasicColumn::Rate => format!("{}", self.rate)
///         }
///     }
///
///     fn cmp(&self, other: &Self, column: BasicColumn) -> Ordering where Self: Sized {
///         match column {
///             BasicColumn::Name => self.name.cmp(&other.name),
///             BasicColumn::Count => self.count.cmp(&other.count),
///             BasicColumn::Rate => self.rate.cmp(&other.rate)
///         }
///     }
///
/// }
///
/// // Configure the actual table with adaptive column widths
/// let table = TableView::<Foo, BasicColumn>::new()
///                      .column(BasicColumn::Name, "Name", |c| c.width_min(10))
///                      .column(BasicColumn::Count, "Count", |c| c.width_min_max(5, 10).align(HAlign::Center))
///                      .column(BasicColumn::Rate, "Rate", |c| {
///                          c.ordering(Ordering::Greater).align(HAlign::Right).width_min_max(4, 10)
///                      })
///                      .default_column(BasicColumn::Name);
/// # }
/// ```
pub struct TableView<T, H> {
    enabled: bool,
    scroll_core: scroll::Core,
    needs_relayout: bool,

    column_select: bool,
    columns: Vec<TableColumn<H>>,
    column_indicies: HashMap<H, usize>,

    focus: Option<usize>,
    items: Vec<T>,
    rows_to_items: Vec<usize>,

    on_sort: Option<OnSortCallback<H>>,
    // TODO Pass drawing offsets into the handlers so a popup menu
    // can be created easily?
    on_submit: Option<IndexCallback>,
    on_select: Option<IndexCallback>,

    // Column resize state
    resizing_column: Option<usize>,
    resize_start_x: usize,
    resize_start_width: usize,

    // Track last layout size for page up/down navigation
    last_size: Arc<Mutex<Vec2>>,

    // Cached content widths for Min/MinMax columns (calculated when items change)
    content_widths: HashMap<usize, usize>,
}

cursive::impl_scroller!(TableView < T, H > ::scroll_core);

impl<T, H> Default for TableView<T, H>
where
    T: TableViewItem<H> + PartialEq,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    /// Creates a new empty `TableView` without any columns.
    ///
    /// See [`TableView::new()`].
    fn default() -> Self {
        Self::new()
    }
}

impl<T, H> TableView<T, H>
where
    T: TableViewItem<H> + PartialEq,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    /// Sets the contained items of the table.
    ///
    /// The currently active sort order is preserved and will be applied to all
    /// items.
    ///
    /// Compared to `set_items`, the current selection will be preserved.
    /// (But this is only available for `T: PartialEq`.)
    pub fn set_items_stable(&mut self, items: Vec<T>) {
        // Preserve selection
        let new_location = self.item().and_then(|old_item| {
            let old_item = &self.items[old_item];
            items.iter().position(|new| new == old_item)
        });

        self.set_items_and_focus(items, new_location);
    }
}

#[allow(dead_code)]
impl<T, H> TableView<T, H>
where
    T: TableViewItem<H>,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    /// Creates a new empty `TableView` without any columns.
    ///
    /// A TableView should be accompanied by a enum of type `H` representing
    /// the table columns.
    pub fn new() -> Self {
        Self {
            enabled: true,
            scroll_core: scroll::Core::new(),
            needs_relayout: true,

            column_select: false,
            columns: Vec::new(),
            column_indicies: HashMap::new(),

            focus: None,
            items: Vec::new(),
            rows_to_items: Vec::new(),

            on_sort: None,
            on_submit: None,
            on_select: None,

            resizing_column: None,
            resize_start_x: 0,
            resize_start_width: 0,

            last_size: Arc::new(Mutex::new(Vec2 { x: 1, y: 1 })),
            content_widths: HashMap::new(),
        }
    }

    /// Adds a column for the specified table colum from type `H` along with
    /// a title for its visual display.
    ///
    /// The provided callback can be used to further configure the
    /// created [`TableColumn`](struct.TableColumn.html).
    pub fn column<S: Into<String>, C: FnOnce(TableColumn<H>) -> TableColumn<H>>(
        mut self,
        column: H,
        title: S,
        callback: C,
    ) -> Self {
        self.add_column(column, title, callback);
        self
    }

    /// Adds a column for the specified table colum from type `H` along with
    /// a title for its visual display.
    ///
    /// The provided callback can be used to further configure the
    /// created [`TableColumn`](struct.TableColumn.html).
    pub fn add_column<S: Into<String>, C: FnOnce(TableColumn<H>) -> TableColumn<H>>(
        &mut self,
        column: H,
        title: S,
        callback: C,
    ) {
        self.insert_column(self.columns.len(), column, title, callback);
    }

    /// Remove a column.
    pub fn remove_column(&mut self, i: usize) {
        // Update the existing indices
        for column in &self.columns[i + 1..] {
            *self.column_indicies.get_mut(&column.column).unwrap() -= 1;
        }

        let column = self.columns.remove(i);
        self.column_indicies.remove(&column.column);
        self.needs_relayout = true;
    }

    /// Adds a column for the specified table colum from type `H` along with
    /// a title for its visual display.
    ///
    /// The provided callback can be used to further configure the
    /// created [`TableColumn`](struct.TableColumn.html).
    pub fn insert_column<S: Into<String>, C: FnOnce(TableColumn<H>) -> TableColumn<H>>(
        &mut self,
        i: usize,
        column: H,
        title: S,
        callback: C,
    ) {
        // Update all existing indices
        for column in &self.columns[i..] {
            *self.column_indicies.get_mut(&column.column).unwrap() += 1;
        }

        self.column_indicies.insert(column, i);
        self.columns
            .insert(i, callback(TableColumn::new(column, title.into())));

        // Make the first colum the default one
        if self.columns.len() == 1 {
            self.set_default_column(column);
        }
        self.needs_relayout = true;
    }

    /// Sets the initially active column of the table.
    pub fn default_column(mut self, column: H) -> Self {
        self.set_default_column(column);
        self
    }

    /// Sets the initially active column of the table.
    pub fn set_default_column(&mut self, column: H) {
        if self.column_indicies.contains_key(&column) {
            for c in &mut self.columns {
                c.selected = c.column == column;
                if c.selected {
                    c.order = c.default_order;
                } else {
                    c.order = Ordering::Equal;
                }
            }
        }
    }

    /// Sorts the table using the specified table `column` and the passed
    /// `order`.
    pub fn sort_by(&mut self, column: H, order: Ordering) {
        if self.column_indicies.contains_key(&column) {
            for c in &mut self.columns {
                // Move selection back to the sorted column.
                c.selected = c.column == column;
                if c.selected {
                    c.order = order;
                } else {
                    c.order = Ordering::Equal;
                }
            }
        }

        self.sort_items(column, order);
    }

    /// Sorts the table using the currently active column and its
    /// ordering.
    pub fn sort(&mut self) {
        if let Some((column, order)) = self.order() {
            self.sort_items(column, order);
        }
    }

    /// Returns the currently active column that is used for sorting
    /// along with its ordering.
    ///
    /// Might return `None` if there are currently no items in the table
    /// and it has not been sorted yet.
    pub fn order(&self) -> Option<(H, Ordering)> {
        for c in &self.columns {
            if c.order != Ordering::Equal {
                return Some((c.column, c.order));
            }
        }
        None
    }

    /// Disables this view.
    ///
    /// A disabled view cannot be selected.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Re-enables this view.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Enable or disable this view.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Returns `true` if this view is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Sets a callback to be used when a selected column is sorted by
    /// pressing `<Enter>`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// table.set_on_sort(|siv: &mut Cursive, column: BasicColumn, order: Ordering| {
    ///
    /// });
    /// ```
    pub fn set_on_sort<F>(&mut self, cb: F)
    where
        F: Fn(&mut Cursive, H, Ordering) + Send + Sync + 'static,
    {
        self.on_sort = Some(Arc::new(move |s, h, o| cb(s, h, o)));
    }

    /// Sets a callback to be used when a selected column is sorted by
    /// pressing `<Enter>`.
    ///
    /// Chainable variant.
    ///
    /// # Example
    ///
    /// ```ignore
    /// table.on_sort(|siv: &mut Cursive, column: BasicColumn, order: Ordering| {
    ///
    /// });
    /// ```
    pub fn on_sort<F>(self, cb: F) -> Self
    where
        F: Fn(&mut Cursive, H, Ordering) + Send + Sync + 'static,
    {
        self.with(|t| t.set_on_sort(cb))
    }

    /// Sets a callback to be used when `<Enter>` is pressed while an item
    /// is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    ///
    /// # Example
    ///
    /// ```ignore
    /// table.set_on_submit(|siv: &mut Cursive, row: Option<usize>, index: Option<usize>| {
    ///
    /// });
    /// ```
    pub fn set_on_submit<F>(&mut self, cb: F)
    where
        F: Fn(&mut Cursive, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.on_submit = Some(Arc::new(move |s, row, index| cb(s, row, index)));
    }

    /// Sets a callback to be used when `<Enter>` is pressed while an item
    /// is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    ///
    /// Chainable variant.
    ///
    /// # Example
    ///
    /// ```ignore
    /// table.on_submit(|siv: &mut Cursive, row: Option<usize>, index: Option<usize>| {
    ///
    /// });
    /// ```
    pub fn on_submit<F>(self, cb: F) -> Self
    where
        F: Fn(&mut Cursive, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.with(|t| t.set_on_submit(cb))
    }

    /// Sets a callback to be used when an item is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    ///
    /// # Example
    ///
    /// ```ignore
    /// table.set_on_select(|siv: &mut Cursive, row: Option<usize>, index: Option<usize>| {
    ///
    /// });
    /// ```
    pub fn set_on_select<F>(&mut self, cb: F)
    where
        F: Fn(&mut Cursive, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.on_select = Some(Arc::new(move |s, row, index| cb(s, row, index)));
    }

    /// Sets a callback to be used when an item is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    ///
    /// Chainable variant.
    ///
    /// # Example
    ///
    /// ```ignore
    /// table.on_select(|siv: &mut Cursive, row: Option<usize>, index: Option<usize>| {
    ///
    /// });
    /// ```
    pub fn on_select<F>(self, cb: F) -> Self
    where
        F: Fn(&mut Cursive, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.with(|t| t.set_on_select(cb))
    }

    /// Removes all items from this view.
    pub fn clear(&mut self) {
        self.items.clear();
        self.rows_to_items.clear();
        self.focus = None;
        self.needs_relayout = true;
    }

    /// Returns the number of items in this table.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if this table has no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the index of the currently selected table row.
    pub fn row(&self) -> Option<usize> {
        if self.items.is_empty() {
            None
        } else {
            self.focus
        }
    }

    /// Selects the row at the specified index.
    pub fn set_selected_row(&mut self, row_index: usize) {
        self.focus = Some(row_index);
        self.scroll_core.scroll_to_y(row_index);
    }

    /// Selects the row at the specified index.
    ///
    /// Chainable variant.
    pub fn selected_row(self, row_index: usize) -> Self {
        self.with(|t| t.set_selected_row(row_index))
    }

    /// Sets the contained items of the table.
    ///
    /// The currently active sort order is preserved and will be applied to all
    /// items.
    pub fn set_items(&mut self, items: Vec<T>) {
        self.set_items_and_focus(items, None);
    }

    fn set_items_and_focus(&mut self, items: Vec<T>, new_location: Option<usize>) {
        self.items = items;
        self.rows_to_items = Vec::with_capacity(self.items.len());

        for i in 0..self.items.len() {
            self.rows_to_items.push(i);
        }

        if let Some((column, order)) = self.order() {
            // Preserve the selected column if possible.
            let selected_column = self.columns.iter().find(|c| c.selected).map(|c| c.column);
            self.sort_by(column, order);
            if let Some(column) = selected_column {
                for c in &mut self.columns {
                    c.selected = c.column == column;
                }
            }
        }

        // Calculate content widths after items are set and sorted
        self.calculate_content_widths();

        if let Some(new_location) = new_location {
            self.set_selected_item(new_location);
        }
        self.needs_relayout = true;
    }

    /// Calculate content widths for Min/MinMax columns from first 100 items.
    /// This is called when items are updated to cache the widths for layout.
    fn calculate_content_widths(&mut self) {
        const SAMPLE_SIZE: usize = 100;
        let sample_count = cmp::min(SAMPLE_SIZE, self.items.len());

        self.content_widths.clear();
        for (col_idx, column) in self.columns.iter().enumerate() {
            if let Some(TableColumnWidth::Min(_) | TableColumnWidth::MinMax(_, _)) =
                &column.requested_width
            {
                // Calculate max content width from first N items
                // Title width includes 2 chars for sorting indicator: " ▲"
                let mut max_width = column.title.len() + 2;
                for i in 0..sample_count {
                    let item_idx = self.rows_to_items[i];
                    let content = self.items[item_idx].to_column(column.column);
                    max_width = cmp::max(max_width, content.len());
                }
                self.content_widths.insert(col_idx, max_width);
            }
        }
    }

    /// Sets the contained items of the table.
    ///
    /// The order of the items will be preserved even when the table is sorted.
    ///
    /// Chainable variant.
    pub fn items(self, items: Vec<T>) -> Self {
        self.with(|t| t.set_items(items))
    }

    /// Returns a immmutable reference to the item at the specified index
    /// within the underlying storage vector.
    pub fn borrow_item(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }

    /// Returns a mutable reference to the item at the specified index within
    /// the underlying storage vector.
    pub fn borrow_item_mut(&mut self, index: usize) -> Option<&mut T> {
        self.items.get_mut(index)
    }

    /// Returns a immmutable reference to the items contained within the table.
    pub fn borrow_items(&mut self) -> &[T] {
        &self.items
    }

    /// Returns a mutable reference to the items contained within the table.
    ///
    /// Can be used to modify the items in place.
    pub fn borrow_items_mut(&mut self) -> &mut [T] {
        self.needs_relayout = true;
        &mut self.items
    }

    /// Returns the index of the currently selected item within the underlying
    /// storage vector.
    pub fn item(&self) -> Option<usize> {
        if let Some(focus) = self.focus {
            self.rows_to_items.get(focus).copied()
        } else {
            None
        }
    }

    /// Selects the item at the specified index within the underlying storage
    /// vector.
    pub fn set_selected_item(&mut self, item_index: usize) {
        // TODO optimize the performance for very large item lists
        if item_index < self.items.len() {
            for (row, item) in self.rows_to_items.iter().enumerate() {
                if *item == item_index {
                    self.focus = Some(row);
                    self.scroll_core.scroll_to_y(row);
                    break;
                }
            }
        }
    }

    /// Selects the item at the specified index within the underlying storage
    /// vector.
    ///
    /// Chainable variant.
    pub fn selected_item(self, item_index: usize) -> Self {
        self.with(|t| t.set_selected_item(item_index))
    }

    /// Inserts a new item into the table.
    ///
    /// The currently active sort order is preserved and will be applied to the
    /// newly inserted item.
    ///
    /// If no sort option is set, the item will be added to the end of the table.
    pub fn insert_item(&mut self, item: T) {
        self.insert_item_at(self.items.len(), item);
    }

    /// Inserts a new item into the table.
    ///
    /// The currently active sort order is preserved and will be applied to the
    /// newly inserted item.
    ///
    /// If no sort option is set, the item will be inserted at the given index.
    ///
    /// # Panics
    ///
    /// If `index > self.len()`.
    pub fn insert_item_at(&mut self, index: usize, item: T) {
        self.items.push(item);

        // Here we know self.items.len() > 0
        self.rows_to_items.insert(index, self.items.len() - 1);

        if let Some((column, order)) = self.order() {
            self.sort_by(column, order);
        }
        self.needs_relayout = true;
    }

    /// Removes the item at the specified index within the underlying storage
    /// vector and returns it.
    pub fn remove_item(&mut self, item_index: usize) -> Option<T> {
        if item_index < self.items.len() {
            // Move the selection if the currently selected item gets removed
            if let Some(selected_index) = self.item()
                && selected_index == item_index
            {
                self.focus_up(1);
            }

            // Remove the sorted reference to the item
            self.rows_to_items.retain(|i| *i != item_index);

            // Adjust remaining references
            for ref_index in &mut self.rows_to_items {
                if *ref_index > item_index {
                    *ref_index -= 1;
                }
            }
            self.needs_relayout = true;

            // Remove actual item from the underlying storage
            Some(self.items.remove(item_index))
        } else {
            None
        }
    }

    /// Removes all items from the underlying storage and returns them.
    pub fn take_items(&mut self) -> Vec<T> {
        self.set_selected_row(0);
        self.rows_to_items.clear();
        self.needs_relayout = true;
        self.items.drain(0..).collect()
    }
}

impl<T, H> TableView<T, H>
where
    T: TableViewItem<H>,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    fn draw_columns<C: Fn(&Printer<'_, '_>, &TableColumn<H>)>(
        &self,
        printer: &Printer<'_, '_>,
        callback: C,
    ) {
        let mut column_offset = 0;
        let column_count = self.columns.len();
        for (index, column) in self.columns.iter().enumerate() {
            let printer = &printer.offset((column_offset, 0)).focused(true);

            callback(printer, column);

            if 1 + index < column_count {
                printer.print((column.width + 1, 0), " ");
            }

            column_offset += column.width + 2;
        }
    }

    fn sort_items(&mut self, column: H, order: Ordering) {
        if !self.is_empty() {
            let old_item = self.item();

            let mut rows_to_items = self.rows_to_items.clone();
            rows_to_items.sort_by(|a, b| {
                if order == Ordering::Less {
                    self.items[*a].cmp(&self.items[*b], column)
                } else {
                    self.items[*b].cmp(&self.items[*a], column)
                }
            });
            self.rows_to_items = rows_to_items;

            if let Some(old_item) = old_item {
                self.set_selected_item(old_item);
            }
        }
    }

    fn draw_item(&self, printer: &Printer<'_, '_>, i: usize) {
        self.draw_columns(printer, |printer, column| {
            let value = self.items[self.rows_to_items[i]].to_column_styled(column.column);
            column.draw_row(printer, &value);
        });
    }

    fn on_focus_change(&self) -> EventResult {
        let row = self.row();
        let index = self.item();
        EventResult::Consumed(
            self.on_select
                .clone()
                .map(|cb| Callback::from_fn(move |s| cb(s, row, index))),
        )
    }

    fn focus_up(&mut self, n: usize) {
        self.focus = Some(self.focus.map_or(0, |x| x - cmp::min(x, n)));
    }

    fn focus_down(&mut self, n: usize) {
        let items = self.items.len().saturating_sub(1);
        self.focus = Some(self.focus.map_or(0, |x| cmp::min(x + n, items)));
    }

    fn active_column(&self) -> usize {
        self.columns.iter().position(|c| c.selected).unwrap_or(0)
    }

    fn column_cancel(&mut self) {
        self.column_select = false;
        for column in &mut self.columns {
            column.selected = column.order != Ordering::Equal;
        }
    }

    fn column_next(&mut self) -> bool {
        let column = self.active_column();
        if 1 + column < self.columns.len() {
            self.columns[column].selected = false;
            self.columns[column + 1].selected = true;
            true
        } else {
            false
        }
    }

    fn column_prev(&mut self) -> bool {
        let column = self.active_column();
        if column > 0 {
            self.columns[column].selected = false;
            self.columns[column - 1].selected = true;
            true
        } else {
            false
        }
    }

    fn column_select(&mut self) -> EventResult {
        let next = self.active_column();
        let column = self.columns[next].column;
        let current = self
            .columns
            .iter()
            .position(|c| c.order != Ordering::Equal)
            .unwrap_or(0);

        let order = if current != next {
            self.columns[next].default_order
        } else if self.columns[current].order == Ordering::Less {
            Ordering::Greater
        } else {
            Ordering::Less
        };

        self.sort_by(column, order);

        if let Some(on_sort) = &self.on_sort {
            let c = &self.columns[self.active_column()];
            let column = c.column;
            let order = c.order;

            let cb = on_sort.clone();
            EventResult::with_cb(move |s| cb(s, column, order))
        } else {
            EventResult::Consumed(None)
        }
    }

    fn column_for_x(&self, mut x: usize) -> Option<usize> {
        for (i, col) in self.columns.iter().enumerate() {
            x = match x.checked_sub(col.width) {
                None => return Some(i),
                Some(x) => x.checked_sub(3)?,
            };
        }

        None
    }

    /// Returns the column index and edge position if mouse is near a column boundary (resize handle)
    fn column_boundary_at(&self, x: usize) -> Option<(usize, usize)> {
        let mut offset = 0;
        for (i, col) in self.columns.iter().enumerate() {
            let right_edge = offset + col.width + 1;
            // Check if within 2 characters of the right edge
            if x >= right_edge.saturating_sub(1)
                && x <= right_edge + 1
                && i + 1 < self.columns.len()
            {
                return Some((i, offset));
            }
            offset = right_edge + 1;
        }
        None
    }

    fn draw_content(&self, printer: &Printer<'_, '_>) {
        for i in 0..self.rows_to_items.len() {
            let printer = printer.offset((0, i));
            let color = if Some(i) == self.focus && self.enabled {
                if !self.column_select && self.enabled && printer.focused {
                    theme::ColorStyle::highlight()
                } else {
                    theme::ColorStyle::highlight_inactive()
                }
            } else {
                theme::ColorStyle::primary()
            };

            if i < self.items.len() {
                printer.with_color(color, |printer| {
                    self.draw_item(printer, i);
                });
            }
        }
    }

    fn layout_content(&mut self, size: Vec2) {
        let column_count = self.columns.len();

        // Use cached content widths calculated when items were set
        // Collect column indices with their requested widths
        let mut sized_indices: Vec<usize> = Vec::new();
        let mut unsized_indices: Vec<usize> = Vec::new();

        for (idx, column) in self.columns.iter().enumerate() {
            if column.requested_width.is_some() {
                sized_indices.push(idx);
            } else {
                unsized_indices.push(idx);
            }
        }

        // Subtract one for the seperators between our columns (that's column_count - 1)
        let available_width = size.x.saturating_sub(column_count.saturating_sub(1) * 2);

        // Calculate widths for all requested columns
        let mut remaining_width = available_width;

        // Find all columns with Min (no max constraint) - they will share remaining space
        let min_cols: Vec<usize> = sized_indices
            .iter()
            .filter(|&&idx| {
                matches!(
                    self.columns[idx].requested_width.as_ref().unwrap(),
                    TableColumnWidth::Min(_)
                )
            })
            .copied()
            .collect();

        // Process all columns except Min columns first
        for &col_idx in &sized_indices {
            if min_cols.contains(&col_idx) && unsized_indices.is_empty() {
                // Skip Min columns for now - we'll process them at the end
                continue;
            }

            let column = &mut self.columns[col_idx];
            column.width = match *column.requested_width.as_ref().unwrap() {
                TableColumnWidth::Percent(width) => cmp::min(
                    (size.x as f32 / 100.0 * width as f32).ceil() as usize,
                    remaining_width,
                ),
                TableColumnWidth::Absolute(width) => width,
                TableColumnWidth::Min(min) => {
                    let content_width = self.content_widths.get(&col_idx).copied().unwrap_or(min);
                    cmp::max(min, content_width)
                }
                TableColumnWidth::MinMax(min, max) => {
                    let content_width = self.content_widths.get(&col_idx).copied().unwrap_or(min);
                    cmp::min(max, cmp::max(min, content_width))
                }
            };
            remaining_width = remaining_width.saturating_sub(self.columns[col_idx].width);
        }

        // Now distribute remaining width among all Min columns
        if !min_cols.is_empty() && unsized_indices.is_empty() {
            let width_per_min_col = remaining_width / min_cols.len();
            for &col_idx in &min_cols {
                let column = &mut self.columns[col_idx];
                if let TableColumnWidth::Min(min) = *column.requested_width.as_ref().unwrap() {
                    column.width = cmp::max(min, width_per_min_col);
                    remaining_width = remaining_width.saturating_sub(column.width);
                }
            }
        }

        // Spread the remaining with across the unsized columns
        let remaining_columns = unsized_indices.len();
        if remaining_columns > 0 {
            let width_per_column =
                (remaining_width as f32 / remaining_columns as f32).floor() as usize;
            for &col_idx in &unsized_indices {
                self.columns[col_idx].width = width_per_column;
            }
        }

        self.needs_relayout = false;
    }

    fn content_required_size(&mut self, req: Vec2) -> Vec2 {
        Vec2::new(req.x, self.rows_to_items.len())
    }

    fn on_inner_event(&mut self, event: Event) -> EventResult {
        let last_focus = self.focus;
        match event {
            Event::Key(Key::Right) => {
                if self.column_select {
                    if !self.column_next() {
                        return EventResult::Ignored;
                    }
                } else {
                    self.column_select = true;
                }
            }
            Event::Key(Key::Left) => {
                if self.column_select {
                    if !self.column_prev() {
                        return EventResult::Ignored;
                    }
                } else {
                    self.column_select = true;
                }
            }
            Event::Key(Key::Up) => {
                if self.column_select {
                    self.column_cancel();
                } else {
                    self.focus_up(1);
                }
            }
            Event::Key(Key::Down) => {
                if self.column_select {
                    self.column_cancel();
                } else {
                    self.focus_down(1);
                }
            }
            Event::Key(Key::PageUp) => {
                self.column_cancel();
                self.focus_up(10);
            }
            Event::Key(Key::PageDown) => {
                self.column_cancel();
                self.focus_down(10);
            }
            Event::Key(Key::Home) => {
                self.column_cancel();
                self.focus = None;
            }
            Event::Key(Key::End) => {
                self.column_cancel();
                self.focus = Some(self.items.len().saturating_sub(1));
            }
            Event::Key(Key::Enter) => {
                if self.column_select {
                    return self.column_select();
                } else if !self.is_empty() && self.on_submit.is_some() {
                    return self.on_submit_event();
                }
            }
            Event::Mouse {
                position,
                offset,
                event: MouseEvent::Press(MouseButton::Left),
            } if !self.is_empty()
                && position
                    .checked_sub(offset)
                    .is_some_and(|p| Some(p.y) == self.focus) =>
            {
                self.column_cancel();
                return self.on_submit_event();
            }
            Event::Mouse {
                position,
                offset,
                event: MouseEvent::Press(_),
            } if !self.is_empty() => match position.checked_sub(offset) {
                Some(position) if position.y < self.rows_to_items.len() => {
                    self.column_cancel();
                    self.focus = Some(position.y);
                }
                _ => return EventResult::Ignored,
            },
            _ => return EventResult::Ignored,
        }

        let focus = self.focus;

        if self.column_select {
            EventResult::Consumed(None)
        } else if !self.is_empty() && last_focus != focus {
            self.on_focus_change()
        } else {
            EventResult::Ignored
        }
    }

    fn inner_important_area(&self, size: Vec2) -> Rect {
        Rect::from_size((0, self.focus.unwrap_or_default()), (size.x, 1))
    }

    fn on_submit_event(&mut self) -> EventResult {
        if let Some(cb) = &self.on_submit {
            let cb = Arc::clone(cb);
            let row = self.row();
            let index = self.item();
            return EventResult::Consumed(Some(Callback::from_fn(move |s| cb(s, row, index))));
        }
        EventResult::Ignored
    }
}

impl<T, H> View for TableView<T, H>
where
    T: TableViewItem<H> + Send + Sync + 'static,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    fn draw(&self, printer: &Printer<'_, '_>) {
        self.draw_columns(printer, |printer, column| {
            let color = if self.enabled && (column.order != Ordering::Equal || column.selected) {
                if self.column_select && column.selected && self.enabled && printer.focused {
                    theme::ColorStyle::highlight()
                } else {
                    theme::ColorStyle::highlight_inactive()
                }
            } else {
                theme::ColorStyle::primary()
            };

            printer.with_color(color, |printer| {
                column.draw_header(printer);
            });
        });

        let printer = &printer.offset((0, 2)).focused(true);
        scroll::draw(self, printer, Self::draw_content);
    }

    fn layout(&mut self, size: Vec2) {
        *self.last_size.lock().unwrap() = size.saturating_sub((0, 2));
        scroll::layout(
            self,
            size.saturating_sub((0, 2)),
            self.needs_relayout,
            Self::layout_content,
            Self::content_required_size,
        );
    }

    fn take_focus(&mut self, _: Direction) -> Result<EventResult, CannotFocus> {
        self.enabled.then(EventResult::consumed).ok_or(CannotFocus)
    }

    fn on_event(&mut self, event: Event) -> EventResult {
        if !self.enabled {
            return EventResult::Ignored;
        }

        match event {
            // Handle j/k navigation
            Event::Char('k') => {
                return self.on_event(Event::Key(Key::Up));
            }
            Event::Char('j') => {
                return self.on_event(Event::Key(Key::Down));
            }
            // Handle page up/down navigation
            Event::Key(Key::PageUp) => {
                let new_row = self
                    .row()
                    .map(|r| {
                        let height = self.last_size.lock().unwrap().y;
                        if r > height { r - height + 1 } else { 0 }
                    })
                    .unwrap_or_default();
                self.set_selected_row(new_row);
                return EventResult::consumed();
            }
            Event::Key(Key::PageDown) => {
                let new_row = self
                    .row()
                    .map(|r| {
                        let len = self.len();
                        let height = self.last_size.lock().unwrap().y;

                        if len > height + r {
                            r + height - 1
                        } else if len > 0 {
                            len - 1
                        } else {
                            0
                        }
                    })
                    .unwrap_or_default();
                self.set_selected_row(new_row);
                return EventResult::consumed();
            }
            // Handle column resize start
            Event::Mouse {
                position,
                offset,
                event: MouseEvent::Press(MouseButton::Left),
            } if position
                .checked_sub(offset)
                .is_some_and(|p| p.y == 0 || p.y == 1) =>
            {
                if let Some(position) = position.checked_sub(offset) {
                    // Check if clicking on a column boundary to start resize
                    if let Some((col_idx, _)) = self.column_boundary_at(position.x) {
                        self.resizing_column = Some(col_idx);
                        self.resize_start_x = position.x;
                        self.resize_start_width = self.columns[col_idx].width;
                        return EventResult::Consumed(None);
                    }
                    // Otherwise handle column selection
                    if position.y == 0
                        && let Some(col) = self.column_for_x(position.x)
                    {
                        if self.column_select && self.columns[col].selected {
                            return self.column_select();
                        } else {
                            let active = self.active_column();
                            self.columns[active].selected = false;
                            self.columns[col].selected = true;
                            self.column_select = true;
                        }
                    }
                }
                EventResult::Ignored
            }
            // Handle column resize drag
            Event::Mouse {
                position,
                offset,
                event: MouseEvent::Hold(MouseButton::Left),
            } if self.resizing_column.is_some() => {
                if let Some(position) = position.checked_sub(offset)
                    && let Some(col_idx) = self.resizing_column
                {
                    let delta = position.x as isize - self.resize_start_x as isize;
                    let new_width = (self.resize_start_width as isize + delta).max(5) as usize;

                    // Update the column width and mark as absolute width
                    self.columns[col_idx].width = new_width;
                    self.columns[col_idx].requested_width =
                        Some(TableColumnWidth::Absolute(new_width));
                    self.needs_relayout = true;
                }
                EventResult::Consumed(None)
            }
            // Handle column resize end
            Event::Mouse {
                event: MouseEvent::Release(MouseButton::Left),
                ..
            } if self.resizing_column.is_some() => {
                self.resizing_column = None;
                EventResult::Consumed(None)
            }
            // Handle column removal on middle mouse press
            Event::Mouse {
                position,
                offset,
                event: MouseEvent::Press(MouseButton::Middle),
            } if position
                .checked_sub(offset)
                .is_some_and(|p| p.y == 0 || p.y == 1) =>
            {
                if let Some(position) = position.checked_sub(offset)
                    && let Some(col_idx) = self.column_for_x(position.x)
                    && self.columns.len() > 1
                {
                    self.remove_column(col_idx);
                    return EventResult::Consumed(None);
                }
                EventResult::Ignored
            }
            event => scroll::on_event(
                self,
                event.relativized((0, 2)),
                Self::on_inner_event,
                Self::inner_important_area,
            ),
        }
    }

    fn important_area(&self, size: Vec2) -> Rect {
        self.inner_important_area(size.saturating_sub((0, 2))) + (0, 2)
    }
}

/// A type used for the construction of columns in a
/// [`TableView`](struct.TableView.html).
pub struct TableColumn<H> {
    column: H,
    title: String,
    selected: bool,
    alignment: HAlign,
    order: Ordering,
    width: usize,
    default_order: Ordering,
    requested_width: Option<TableColumnWidth>,
}

enum TableColumnWidth {
    Percent(usize),
    Absolute(usize),
    /// Minimum width - will use content width but at least this value
    Min(usize),
    /// Minimum and maximum width - will use content width constrained to this range
    MinMax(usize, usize),
}

#[allow(dead_code)]
impl<H: Copy + Clone + 'static> TableColumn<H> {
    /// Sets the default ordering of the column.
    pub fn ordering(mut self, order: Ordering) -> Self {
        self.default_order = order;
        self
    }

    /// Sets the horizontal text alignment of the column.
    pub fn align(mut self, alignment: HAlign) -> Self {
        self.alignment = alignment;
        self
    }

    /// Sets how many characters of width this column will try to occupy.
    pub fn width(mut self, width: usize) -> Self {
        self.requested_width = Some(TableColumnWidth::Absolute(width));
        self
    }

    /// Sets what percentage of the width of the entire table this column will
    /// try to occupy.
    pub fn width_percent(mut self, width: usize) -> Self {
        self.requested_width = Some(TableColumnWidth::Percent(width));
        self
    }

    /// Sets minimum width for the column - will calculate actual width from content
    /// but use at least this value.
    pub fn width_min(mut self, min: usize) -> Self {
        self.requested_width = Some(TableColumnWidth::Min(min));
        self
    }

    /// Sets minimum and maximum width for the column - will calculate actual width
    /// from content but constrain it to this range.
    pub fn width_min_max(mut self, min: usize, max: usize) -> Self {
        self.requested_width = Some(TableColumnWidth::MinMax(min, max));
        self
    }

    fn new(column: H, title: String) -> Self {
        Self {
            column,
            title,
            selected: false,
            alignment: HAlign::Left,
            order: Ordering::Equal,
            width: 0,
            default_order: Ordering::Less,
            requested_width: None,
        }
    }

    fn draw_header(&self, printer: &Printer<'_, '_>) {
        let order = match self.order {
            Ordering::Less => "▲",
            Ordering::Greater => "▼",
            Ordering::Equal => " ",
        };

        let header = match self.alignment {
            HAlign::Left => format!(
                "{:<width$} {}",
                self.title,
                order,
                width = self.width.saturating_sub(2)
            ),
            HAlign::Right => format!(
                "{:>width$} {}",
                self.title,
                order,
                width = self.width.saturating_sub(2)
            ),
            HAlign::Center => format!(
                "{:^width$} {}",
                self.title,
                order,
                width = self.width.saturating_sub(2)
            ),
        };

        printer.print((0, 0), header.as_str());
    }

    fn draw_row(&self, printer: &Printer<'_, '_>, value: &StyledString) {
        let plain_text = value.source();
        let current_len = plain_text.len();
        let target_width = self.width;

        // Create a new styled string with proper alignment
        let mut styled = StyledString::new();

        if current_len < target_width {
            let padding = target_width - current_len;
            match self.alignment {
                HAlign::Left => {
                    styled.append(value.clone());
                    styled.append_plain(" ".repeat(padding + 1));
                }
                HAlign::Right => {
                    styled.append_plain(" ".repeat(padding));
                    styled.append(value.clone());
                    styled.append_plain(" ");
                }
                HAlign::Center => {
                    let left_padding = padding / 2;
                    let right_padding = padding - left_padding;
                    styled.append_plain(" ".repeat(left_padding));
                    styled.append(value.clone());
                    styled.append_plain(" ".repeat(right_padding + 1));
                }
            }
        } else {
            styled.append(value.clone());
            styled.append_plain(" ");
        }

        printer.print_styled((0, 0), &styled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, PartialEq, Eq, Hash)]
    enum SimpleColumn {
        Name,
    }

    #[allow(dead_code)]
    impl SimpleColumn {
        fn as_str(&self) -> &str {
            match *self {
                SimpleColumn::Name => "Name",
            }
        }
    }

    #[derive(Clone, Debug)]
    struct SimpleItem {
        name: String,
    }

    impl TableViewItem<SimpleColumn> for SimpleItem {
        fn to_column(&self, column: SimpleColumn) -> String {
            match column {
                SimpleColumn::Name => self.name.to_string(),
            }
        }

        fn cmp(&self, other: &Self, column: SimpleColumn) -> Ordering
        where
            Self: Sized,
        {
            match column {
                SimpleColumn::Name => self.name.cmp(&other.name),
            }
        }
    }

    fn setup_test_table() -> TableView<SimpleItem, SimpleColumn> {
        TableView::<SimpleItem, SimpleColumn>::new()
            .column(SimpleColumn::Name, "Name", |c| c.width_percent(20))
    }

    #[test]
    fn should_insert_into_existing_table() {
        let mut simple_table = setup_test_table();

        let mut simple_items = Vec::new();

        for i in 1..=10 {
            simple_items.push(SimpleItem {
                name: format!("{} - Name", i),
            });
        }

        // Insert First Batch of Items
        simple_table.set_items(simple_items);

        // Test for Additional item insertion
        simple_table.insert_item(SimpleItem {
            name: format!("{} Name", 11),
        });

        assert!(simple_table.len() == 11);
    }

    #[test]
    fn should_insert_into_empty_table() {
        let mut simple_table = setup_test_table();

        // Test for First item insertion
        simple_table.insert_item(SimpleItem {
            name: format!("{} Name", 1),
        });

        assert!(simple_table.len() == 1);
    }
}

/// This is the same as cursive::wrap_impl(), but without into_inner() method, that moves out the
/// value, since our views implements drop() and cannot be moved out.
#[macro_export]
macro_rules! wrap_impl_no_move {
    (self.$v:ident: $t:ty) => {
        type V = $t;

        fn with_view<F, R>(&self, f: F) -> ::std::option::Option<R>
        where
            F: ::std::ops::FnOnce(&Self::V) -> R,
        {
            ::std::option::Option::Some(f(&self.$v))
        }

        fn with_view_mut<F, R>(&mut self, f: F) -> ::std::option::Option<R>
        where
            F: ::std::ops::FnOnce(&mut Self::V) -> R,
        {
            ::std::option::Option::Some(f(&mut self.$v))
        }
    };
}
