use cursive::{
    event::{Event, EventResult, Key},
    inner_getters,
    vec::Vec2,
    view::{View, ViewWrapper},
    wrap_impl, Cursive,
};
use cursive_table_view;
use std::cmp::Ordering;
use std::hash::Hash;

/// A wrapper for cursive_table_view with more shortcuts:
///
/// - j/k -- for navigation
/// - PgUp/PgDown -- scroll the whole page
pub struct TableView<T, H> {
    inner_view: cursive_table_view::TableView<T, H>,
    last_size: Vec2,
}

pub use cursive_table_view::TableColumn;
pub use cursive_table_view::TableViewItem;

impl<T, H> TableView<T, H>
where
    T: 'static + cursive_table_view::TableViewItem<H>,
    H: 'static + Eq + Hash + Copy + Clone,
{
    inner_getters!(self.inner_view: cursive_table_view::TableView<T, H>);

    pub fn new() -> Self {
        return TableView {
            inner_view: cursive_table_view::TableView::new(),
            last_size: Vec2 { x: 1, y: 1 },
        };
    }

    /// Wrapper to make it able to use with dot notation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut view = TableView::<QueryProcess, QueryProcessesColumn>::new();
    /// inner_view
    ///     .column(QueryProcessesColumn::QueryId, "QueryId", |c| c.width(10))
    ///     .column(QueryProcessesColumn::Cpu, "CPU", |c| c.width(8));
    /// ```
    pub fn column<S: Into<String>, C: FnOnce(TableColumn<H>) -> TableColumn<H>>(
        mut self,
        column: H,
        title: S,
        callback: C,
    ) -> Self {
        self.inner_view.add_column(column, title, callback);
        self
    }

    /// Just a wrapper to cursive_table_view for simplicity
    pub fn insert_column<S: Into<String>, C: FnOnce(TableColumn<H>) -> TableColumn<H>>(
        &mut self,
        i: usize,
        column: H,
        title: S,
        callback: C,
    ) {
        self.inner_view.insert_column(i, column, title, callback);
    }

    /// Just a wrapper to cursive_table_view for simplicity
    pub fn sort_by(&mut self, column: H, order: Ordering) {
        self.inner_view.sort_by(column, order);
    }

    /// Just a wrapper to cursive_table_view for simplicity
    pub fn set_on_submit<F>(&mut self, cb: F)
    where
        F: Fn(&mut Cursive, usize, usize) + 'static,
    {
        self.inner_view.set_on_submit(cb);
    }
}

impl<T, H> ViewWrapper for TableView<T, H>
where
    T: 'static + cursive_table_view::TableViewItem<H>,
    H: 'static + Eq + Hash + Copy + Clone,
{
    wrap_impl!(self.inner_view: cursive_table_view::TableView<T, H>);

    fn wrap_layout(&mut self, size: Vec2) {
        self.last_size = size;

        assert!(self.last_size.y > 2);
        // header and borders
        self.last_size.y -= 2;

        self.inner_view.layout(size);
    }

    fn wrap_on_event(&mut self, event: Event) -> EventResult {
        match event {
            // Basic bindings
            Event::Char('k') => return self.inner_view.on_event(Event::Key(Key::Up)),
            Event::Char('j') => return self.inner_view.on_event(Event::Key(Key::Down)),
            // cursive_table_view scrolls only 10 rows, rebind to scroll the whole page
            Event::Key(Key::PageUp) => {
                let row = self.inner_view.row().unwrap_or_default();
                let height = self.last_size.y;
                let new_row = if row > height { row - height + 1 } else { 0 };
                self.inner_view.set_selected_row(new_row);
                return EventResult::Consumed(None);
            }
            Event::Key(Key::PageDown) => {
                let row = self.inner_view.row().unwrap_or_default();
                let len = self.inner_view.len();
                let height = self.last_size.y;
                let new_row = if len - row > height {
                    row + height - 1
                } else if len > 0 {
                    len - 1
                } else {
                    0
                };
                self.inner_view.set_selected_row(new_row);
                return EventResult::Consumed(None);
            }
            _ => {}
        }
        return self.inner_view.on_event(event);
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
