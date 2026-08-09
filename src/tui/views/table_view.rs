//
// Port of src/view/table_view.rs (originally copied from
// https://github.com/BonsaiDen/cursive_table_view) onto the in-repo retained
// ratatui component framework (src/tui).
//
// Extensions kept from the cursive version:
// - Ability not to follow the selected item in the table (Home = follow head)
// - Column resize on mouse drag
// - Column removal on middle mouse press
// - j/k and PgUp/PgDown navigation
// - Column width calculated from the input rows (Min/MinMax constraints)
//

use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use ratatui::layout::{Position, Rect};
use ratatui::text::Line;

use crate::tui::app::App;
use crate::tui::component::{Canvas, Component};
use crate::tui::event::{Callback, Event, EventResult, Key, MouseButton, MouseEvent};
use crate::tui::style::{
    Color, Modifier, Style, StyledString, highlight, highlight_inactive, print_line, print_str,
    str_width,
};

/// Horizontal alignment of a column (replaces cursive::align::HAlign).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HAlign {
    Left,
    Center,
    Right,
}

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
type OnSortCallback<H> = Arc<dyn Fn(&mut App, H, Ordering) + Send + Sync>;

/// Callback taking as argument the row and the index of an element.
type IndexCallback = Arc<dyn Fn(&mut App, Option<usize>, Option<usize>) + Send + Sync>;

/// Callback used when a column is removed.
///
/// It takes the removed column as input.
type OnRemoveColumnCallback<H> = Arc<dyn Fn(&mut App, H) + Send + Sync>;

/// View to select an item among a list, supporting multiple columns for sorting.
pub struct TableView<T, H> {
    enabled: bool,

    column_select: bool,
    columns: Vec<TableColumn<H>>,
    column_indicies: HashMap<H, usize>,

    focus: Option<usize>,
    items: Vec<T>,
    rows_to_items: Vec<usize>,

    on_sort: Option<OnSortCallback<H>>,
    on_submit: Option<IndexCallback>,
    on_select: Option<IndexCallback>,
    on_remove_column: Option<OnRemoveColumnCallback<H>>,

    // Column resize state (x coordinates are absolute screen positions)
    resizing_column: Option<usize>,
    resize_start_x: u16,
    resize_start_width: usize,

    // Vertical scrolling over rows: index of the first visible row.
    // usize because item counts can exceed u16.
    scroll_offset: usize,

    // Geometry of the last draw, used to translate absolute mouse positions
    // and for page up/down navigation.
    last_area: Rect,
    last_header_y: u16,
    last_rows_area: Rect,
    last_viewport_height: usize,

    // Cached content widths for Min/MinMax columns (calculated when items change)
    content_widths: HashMap<usize, usize>,

    title: Option<String>,
}

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

            column_select: false,
            columns: Vec::new(),
            column_indicies: HashMap::new(),

            focus: None,
            items: Vec::new(),
            rows_to_items: Vec::new(),

            on_sort: None,
            on_submit: None,
            on_select: None,
            on_remove_column: None,

            resizing_column: None,
            resize_start_x: 0,
            resize_start_width: 0,

            scroll_offset: 0,

            last_area: Rect::default(),
            last_header_y: 0,
            last_rows_area: Rect::default(),
            last_viewport_height: 1,

            content_widths: HashMap::new(),
            title: None,
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
    pub fn set_on_sort<F>(&mut self, cb: F)
    where
        F: Fn(&mut App, H, Ordering) + Send + Sync + 'static,
    {
        self.on_sort = Some(Arc::new(move |app, h, o| cb(app, h, o)));
    }

    /// Sets a callback to be used when a selected column is sorted by
    /// pressing `<Enter>`.
    ///
    /// Chainable variant.
    pub fn on_sort<F>(mut self, cb: F) -> Self
    where
        F: Fn(&mut App, H, Ordering) + Send + Sync + 'static,
    {
        self.set_on_sort(cb);
        self
    }

    /// Sets a callback to be used when a column is removed
    /// (on middle mouse press over the column).
    pub fn set_on_remove_column<F>(&mut self, cb: F)
    where
        F: Fn(&mut App, H) + Send + Sync + 'static,
    {
        self.on_remove_column = Some(Arc::new(move |app, h| cb(app, h)));
    }

    /// Sets a callback to be used when `<Enter>` is pressed while an item
    /// is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    pub fn set_on_submit<F>(&mut self, cb: F)
    where
        F: Fn(&mut App, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.on_submit = Some(Arc::new(move |app, row, index| cb(app, row, index)));
    }

    /// Sets a callback to be used when `<Enter>` is pressed while an item
    /// is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    ///
    /// Chainable variant.
    pub fn on_submit<F>(mut self, cb: F) -> Self
    where
        F: Fn(&mut App, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.set_on_submit(cb);
        self
    }

    /// Sets a callback to be used when an item is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    pub fn set_on_select<F>(&mut self, cb: F)
    where
        F: Fn(&mut App, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.on_select = Some(Arc::new(move |app, row, index| cb(app, row, index)));
    }

    /// Sets a callback to be used when an item is selected.
    ///
    /// Both the currently selected row and the index of the corresponding item
    /// within the underlying storage vector will be given to the callback.
    ///
    /// Chainable variant.
    pub fn on_select<F>(mut self, cb: F) -> Self
    where
        F: Fn(&mut App, Option<usize>, Option<usize>) + Send + Sync + 'static,
    {
        self.set_on_select(cb);
        self
    }

    /// Removes all items from this view.
    pub fn clear(&mut self) {
        self.items.clear();
        self.rows_to_items.clear();
        self.focus = None;
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
        self.scroll_to_row(row_index);
    }

    /// Selects the row at the specified index.
    ///
    /// Chainable variant.
    pub fn selected_row(mut self, row_index: usize) -> Self {
        self.set_selected_row(row_index);
        self
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
    pub fn items(mut self, items: Vec<T>) -> Self {
        self.set_items(items);
        self
    }

    /// Sets the displayed title of an existing column.
    pub fn set_column_title<S: Into<String>>(&mut self, column: H, title: S) {
        if let Some(&i) = self.column_indicies.get(&column) {
            self.columns[i].title = title.into();
        }
    }

    /// Pins an existing column to a fixed (absolute) width so it does not
    /// rescale when content changes.
    pub fn set_column_width(&mut self, column: H, width: usize) {
        if let Some(&i) = self.column_indicies.get(&column) {
            self.columns[i].requested_width = Some(TableColumnWidth::Absolute(width));
            self.columns[i].width = width;
        }
    }

    /// Sets the title displayed above the table header (chainable).
    pub fn title<S: Into<String>>(mut self, title: S) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Sets the title displayed above the table header.
    pub fn set_title<S: Into<String>>(&mut self, title: S) {
        self.title = Some(title.into());
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
                    self.scroll_to_row(row);
                    break;
                }
            }
        }
    }

    /// Selects the item at the specified index within the underlying storage
    /// vector.
    ///
    /// Chainable variant.
    pub fn selected_item(mut self, item_index: usize) -> Self {
        self.set_selected_item(item_index);
        self
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
        self.items.drain(0..).collect()
    }
}

impl<T, H> TableView<T, H>
where
    T: TableViewItem<H>,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    fn title_height(&self) -> u16 {
        if self.title.is_some() { 1 } else { 0 }
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

    fn on_focus_change(&self) -> EventResult {
        let row = self.row();
        let index = self.item();
        EventResult::Consumed(
            self.on_select
                .clone()
                .map(|cb| Arc::new(move |app: &mut App| cb(app, row, index)) as Callback),
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
            EventResult::with_cb(move |app| cb(app, column, order))
        } else {
            EventResult::Consumed(None)
        }
    }

    /// Column under the given x offset (relative to the table's left edge).
    fn column_for_x(&self, mut x: usize) -> Option<usize> {
        for (i, col) in self.columns.iter().enumerate() {
            x = match x.checked_sub(col.width) {
                None => return Some(i),
                Some(x) => x.checked_sub(2)?,
            };
        }

        None
    }

    /// Returns the column index and edge position if mouse is near a column boundary (resize handle)
    fn column_boundary_at(&self, x: usize) -> Option<(usize, usize)> {
        let mut offset = 0;
        for (i, col) in self.columns.iter().enumerate() {
            // Match the draw code: separator at column.width + 1
            let separator_pos = offset + col.width + 1;
            // Check if within 2 characters of the separator
            if x >= separator_pos.saturating_sub(1)
                && x <= separator_pos + 1
                && i + 1 < self.columns.len()
            {
                return Some((i, offset));
            }
            // Match the draw code: next column at column.width + 2
            offset += col.width + 2;
        }
        None
    }

    /// Whether the (absolute) mouse position is on the header row or the
    /// blank separator row below it.
    fn header_contains(&self, position: Position) -> bool {
        self.last_area.contains(position)
            && (position.y == self.last_header_y
                || position.y == self.last_header_y.saturating_add(1))
    }

    /// Row index under the (absolute) mouse position.
    fn row_at(&self, position: Position) -> Option<usize> {
        if !self.last_rows_area.contains(position) {
            return None;
        }
        Some(self.scroll_offset + (position.y - self.last_rows_area.y) as usize)
    }

    /// Adjust the scroll offset so that `row` is visible in the viewport.
    fn scroll_to_row(&mut self, row: usize) {
        self.scroll_offset = crate::tui::scroll::keep_row_visible(
            self.scroll_offset,
            row,
            self.last_viewport_height,
        );
    }

    fn scroll_by(&mut self, dy: isize) -> EventResult {
        let max_offset = self
            .rows_to_items
            .len()
            .saturating_sub(self.last_viewport_height) as isize;
        let new_offset = (self.scroll_offset as isize + dy).clamp(0, max_offset) as usize;
        if new_offset == self.scroll_offset {
            return EventResult::Ignored;
        }
        self.scroll_offset = new_offset;
        EventResult::consumed()
    }

    /// Compute the actual column widths for the given available width
    /// (excluding the scrollbar).
    fn compute_column_widths(&mut self, size_x: usize) {
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
        let available_width = size_x.saturating_sub(column_count.saturating_sub(1) * 2);

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
                    (size_x as f32 / 100.0 * width as f32).ceil() as usize,
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
    }

    fn on_submit_event(&mut self) -> EventResult {
        if let Some(cb) = &self.on_submit {
            let cb = Arc::clone(cb);
            let row = self.row();
            let index = self.item();
            return EventResult::Consumed(Some(Arc::new(move |app: &mut App| cb(app, row, index))));
        }
        EventResult::Ignored
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        if !self.enabled {
            return EventResult::Ignored;
        }

        match event {
            // Handle j/k navigation
            Event::Char('k') => self.handle_event(&Event::Key(Key::Up)),
            Event::Char('j') => self.handle_event(&Event::Key(Key::Down)),
            // Handle page up/down navigation
            Event::Key(Key::PageUp) => {
                let new_row = self
                    .row()
                    .map(|r| {
                        let height = self.last_viewport_height;
                        if r > height { r - height + 1 } else { 0 }
                    })
                    .unwrap_or_default();
                self.set_selected_row(new_row);
                EventResult::consumed()
            }
            Event::Key(Key::PageDown) => {
                let new_row = self
                    .row()
                    .map(|r| {
                        let len = self.len();
                        let height = self.last_viewport_height;

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
                EventResult::consumed()
            }
            // Handle column resize start / column selection on the header
            Event::Mouse {
                position,
                event: MouseEvent::Press(MouseButton::Left),
            } if self.header_contains(*position) => {
                let x = (position.x - self.last_area.x) as usize;
                // Check if clicking on a column boundary to start resize
                if let Some((col_idx, _)) = self.column_boundary_at(x) {
                    self.resizing_column = Some(col_idx);
                    self.resize_start_x = position.x;
                    self.resize_start_width = self.columns[col_idx].width;
                    return EventResult::consumed();
                }
                // Otherwise handle column selection
                if position.y == self.last_header_y
                    && let Some(col) = self.column_for_x(x)
                {
                    if self.column_select && self.columns[col].selected {
                        return self.column_select();
                    }
                    let active = self.active_column();
                    self.columns[active].selected = false;
                    self.columns[col].selected = true;
                    self.column_select = true;
                    return EventResult::consumed();
                }
                EventResult::Ignored
            }
            // Handle column resize drag
            Event::Mouse {
                position,
                event: MouseEvent::Hold(MouseButton::Left),
            } if self.resizing_column.is_some() => {
                if let Some(col_idx) = self.resizing_column {
                    let delta = position.x as isize - self.resize_start_x as isize;
                    let new_width = (self.resize_start_width as isize + delta).max(5) as usize;

                    // Update the column width and mark as absolute width
                    self.columns[col_idx].width = new_width;
                    self.columns[col_idx].requested_width =
                        Some(TableColumnWidth::Absolute(new_width));
                }
                EventResult::consumed()
            }
            // Handle column resize end
            Event::Mouse {
                event: MouseEvent::Release(MouseButton::Left),
                ..
            } if self.resizing_column.is_some() => {
                self.resizing_column = None;
                EventResult::consumed()
            }
            // Handle column removal on middle mouse press
            Event::Mouse {
                position,
                event: MouseEvent::Press(MouseButton::Middle),
            } if self.header_contains(*position) => {
                let x = (position.x - self.last_area.x) as usize;
                if let Some(col_idx) = self.column_for_x(x)
                    && self.columns.len() > 1
                {
                    let column = self.columns[col_idx].column;
                    self.remove_column(col_idx);
                    let cb = self
                        .on_remove_column
                        .clone()
                        .map(|cb| Arc::new(move |app: &mut App| cb(app, column)) as Callback);
                    return EventResult::Consumed(cb);
                }
                EventResult::Ignored
            }
            Event::Mouse {
                event: MouseEvent::WheelUp,
                ..
            } => self.scroll_by(-3),
            Event::Mouse {
                event: MouseEvent::WheelDown,
                ..
            } => self.scroll_by(3),
            _ => self.on_inner_event(event),
        }
    }

    fn on_inner_event(&mut self, event: &Event) -> EventResult {
        let last_focus = self.focus;
        let mut scrolled = false;
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
            Event::Key(Key::Home) => {
                // "Follow head": no selection, view pinned to the top.
                self.column_cancel();
                self.focus = None;
                scrolled = self.scroll_offset != 0;
                self.scroll_offset = 0;
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
                event: MouseEvent::Press(MouseButton::Left),
            } if !self.is_empty()
                && self
                    .row_at(*position)
                    .is_some_and(|row| Some(row) == self.focus) =>
            {
                self.column_cancel();
                return self.on_submit_event();
            }
            Event::Mouse {
                position,
                event: MouseEvent::Press(_),
            } if !self.is_empty() => match self.row_at(*position) {
                Some(row) if row < self.rows_to_items.len() => {
                    self.column_cancel();
                    self.focus = Some(row);
                }
                _ => return EventResult::Ignored,
            },
            _ => return EventResult::Ignored,
        }

        if let Some(focus) = self.focus {
            self.scroll_to_row(focus);
        }

        if self.column_select {
            EventResult::Consumed(None)
        } else if !self.is_empty() && last_focus != self.focus {
            self.on_focus_change()
        } else if scrolled {
            EventResult::consumed()
        } else {
            EventResult::Ignored
        }
    }
}

impl<T, H> Component for TableView<T, H>
where
    T: TableViewItem<H> + Send + Sync + 'static,
    H: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.last_area = area;
        let title_height = self.title_height();
        let header_y = area.y.saturating_add(title_height);
        self.last_header_y = header_y;

        // Title row (if any), header row, blank separator row, then content.
        let header_height = title_height + 2;
        let viewport = area.height.saturating_sub(header_height) as usize;
        self.last_viewport_height = viewport;

        let total_rows = self.rows_to_items.len();
        let scrollbar = viewport > 0 && total_rows > viewport;
        let content_w = area.width.saturating_sub(scrollbar as u16);
        self.compute_column_widths(content_w as usize);

        self.scroll_offset = self.scroll_offset.min(total_rows.saturating_sub(viewport));

        let rows_y = area.y.saturating_add(header_height);
        self.last_rows_area = Rect::new(area.x, rows_y, area.width, viewport as u16);

        if let Some(title) = &self.title {
            // Bright in the focused pane, dim otherwise (the pane indicator,
            // together with the highlighted separators)
            let title_color = if focused {
                Color::LightCyan
            } else {
                Color::Cyan
            };
            let mut styled = StyledString::new();
            styled.append_plain("\u{2500}\u{2500}\u{2500} ");
            styled.append_styled(
                title.clone(),
                Style::default()
                    .fg(title_color)
                    .add_modifier(Modifier::BOLD),
            );
            styled.append_styled(
                format!(" ({})", self.items.len()),
                Style::default().fg(title_color),
            );
            styled.append_plain(" \u{2500}\u{2500}\u{2500}");
            let offset = (area.width as usize).saturating_sub(styled.width()) / 2;
            print_line(
                canvas.buf,
                area.x + offset as u16,
                area.y,
                area,
                &styled.first_line(),
            );
        }

        // Header
        let mut column_offset: usize = 0;
        for column in &self.columns {
            if column_offset >= content_w as usize {
                break;
            }
            let style = if self.enabled && (column.order != Ordering::Equal || column.selected) {
                if self.column_select && column.selected && self.enabled && focused {
                    highlight()
                } else {
                    highlight_inactive()
                }
            } else {
                Style::default()
            };
            let x = area.x + column_offset as u16;
            let cell_w = (column.width + 1).min(content_w as usize - column_offset) as u16;
            let cell = Rect::new(x, header_y, cell_w, 1);
            print_str(canvas.buf, x, header_y, cell, &column.header_text(), style);
            column_offset += column.width + 2;
        }

        // Rows
        let end = cmp::min(self.scroll_offset + viewport, total_rows);
        for i in self.scroll_offset..end {
            let y = rows_y + (i - self.scroll_offset) as u16;
            let selected = Some(i) == self.focus && self.enabled;
            let row_style = if selected {
                if !self.column_select && focused {
                    highlight()
                } else {
                    highlight_inactive()
                }
            } else {
                Style::default()
            };

            if selected {
                // Highlight the full row, not only the cell text.
                for x in area.left()..area.left() + content_w {
                    if let Some(cell) = canvas.buf.cell_mut((x, y)) {
                        cell.set_symbol(" ").set_style(row_style);
                    }
                }
            }

            let item = &self.items[self.rows_to_items[i]];
            let mut column_offset: usize = 0;
            for column in &self.columns {
                if column_offset >= content_w as usize {
                    break;
                }
                let value = item.to_column_styled(column.column);
                let mut line = column.aligned_line(&value);
                if selected {
                    // Selection style overrides any per-span styling.
                    for span in &mut line.spans {
                        span.style = row_style;
                    }
                }
                let x = area.x + column_offset as u16;
                let cell_w = (column.width + 1).min(content_w as usize - column_offset) as u16;
                let cell = Rect::new(x, y, cell_w, 1);
                print_line(canvas.buf, x, y, cell, &line);
                column_offset += column.width + 2;
            }
        }

        // Scrollbar
        if scrollbar && area.width > 0 {
            crate::tui::scroll::draw_scrollbar_v(
                canvas.buf,
                area.right() - 1,
                rows_y,
                total_rows,
                viewport,
                self.scroll_offset,
            );
        }
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        self.handle_event(event)
    }

    fn take_focus(&mut self) -> bool {
        self.enabled
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

/// Width constraint of a column.
#[derive(Clone, Copy, Debug)]
pub enum TableColumnWidth {
    /// Percentage of the width of the entire table.
    Percent(usize),
    /// Fixed width.
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

    fn header_text(&self) -> String {
        let order = match self.order {
            Ordering::Less => "▲",
            Ordering::Greater => "▼",
            Ordering::Equal => " ",
        };

        match self.alignment {
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
        }
    }

    /// Single line of the cell value padded/aligned to the column width
    /// (+1 trailing space, like the cursive version).
    fn aligned_line(&self, value: &StyledString) -> Line<'static> {
        let current_len = str_width(&value.source());
        let target_width = self.width;

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

        styled.first_line()
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
