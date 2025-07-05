use cursive::{
    event::{Event, EventResult, Key},
    inner_getters,
    vec::Vec2,
    view::{View, ViewWrapper},
    views::OnEventView,
    wrap_impl,
};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// A wrapper for cursive_table_view with more shortcuts:
///
/// - j/k -- for navigation
/// - PgUp/PgDown -- scroll the whole page
pub struct ExtTableView<T, H> {
    inner_view: OnEventView<cursive_table_view::TableView<T, H>>,
    last_size: Arc<Mutex<Vec2>>,
}

pub use cursive_table_view::TableViewItem;

impl<T, H> ExtTableView<T, H>
where
    T: 'static + cursive_table_view::TableViewItem<H> + Sync + Send,
    H: 'static + Eq + Hash + Copy + Clone + Sync + Send,
{
    inner_getters!(self.inner_view: OnEventView<cursive_table_view::TableView<T, H>>);
}

impl<T, H> Default for ExtTableView<T, H>
where
    T: 'static + cursive_table_view::TableViewItem<H> + Sync + Send,
    H: 'static + Eq + Hash + Copy + Clone + Sync + Send,
{
    fn default() -> Self {
        let table_view = cursive_table_view::TableView::new();

        let last_size = Arc::new(Mutex::new(Vec2 { x: 1, y: 1 }));
        // FIXME: rewrite it to capture_it() or similar [1]
        //   [1]: https://github.com/rust-lang/rfcs/issues/2407
        let last_size_clone_1 = last_size.clone();
        let last_size_clone_2 = last_size.clone();

        let event_view = OnEventView::new(table_view)
            .on_event_inner('k', |v, _| {
                v.on_event(Event::Key(Key::Up));
                return Some(EventResult::consumed());
            })
            .on_event_inner('j', |v, _| {
                v.on_event(Event::Key(Key::Down));
                return Some(EventResult::consumed());
            })
            .on_pre_event_inner(Key::PageUp, move |v, _| {
                let new_row = v
                    .row()
                    .map(|r| {
                        let height = last_size_clone_1.lock().unwrap().y;
                        let new_row = if r > height { r - height + 1 } else { 0 };
                        return new_row;
                    })
                    .unwrap_or_default();
                v.set_selected_row(new_row);

                return Some(EventResult::consumed());
            })
            .on_pre_event_inner(Key::PageDown, move |v, _| {
                let new_row = v
                    .row()
                    .map(|r| {
                        let len = v.len();
                        let height = last_size_clone_2.lock().unwrap().y;

                        if len > height + r {
                            r + height - 1
                        } else if len > 0 {
                            len - 1
                        } else {
                            0
                        }
                    })
                    .unwrap_or_default();
                v.set_selected_row(new_row);

                return Some(EventResult::consumed());
            });

        return Self {
            inner_view: event_view,
            last_size,
        };
    }
}

impl<T, H> ViewWrapper for ExtTableView<T, H>
where
    T: 'static + cursive_table_view::TableViewItem<H> + Sync + Send,
    H: 'static + Eq + Hash + Copy + Clone + Sync + Send,
{
    wrap_impl!(self.inner_view: OnEventView<cursive_table_view::TableView<T, H>>);

    fn wrap_layout(&mut self, size: Vec2) {
        *self.last_size.lock().unwrap() = size;
        self.inner_view.layout(size);
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
