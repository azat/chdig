use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use cursive::{view::ViewWrapper, Cursive};
use cursive_table_view;

use crate::view::{TableColumn, TableView, TableViewItem};
use crate::wrap_impl_no_move;

pub struct UpdatingTableView<T, H> {
    inner_view: TableView<T, H>,
    update_interval: Duration,

    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl<T, H> Drop for UpdatingTableView<T, H> {
    fn drop(&mut self) {
        log::debug!("Stopping updates");
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
        log::debug!("Updates stopped");
    }
}

impl<T, H> UpdatingTableView<T, H>
where
    T: 'static + TableViewItem<H>,
    H: 'static + Eq + Hash + Copy + Clone,
{
    pub fn new<C: Fn() + std::marker::Send + 'static>(
        update_interval: Duration,
        update_callback: C,
    ) -> Self {
        let mut view = Self {
            inner_view: TableView::new(),
            update_interval,
            thread: None,
            cv: Arc::new((Mutex::new(false), Condvar::new())),
        };
        view.start(update_callback);
        return view;
    }

    fn start<C: Fn() + std::marker::Send + 'static>(&mut self, update_callback: C) {
        let interval = self.update_interval;
        let cv = self.cv.clone();
        self.thread = Some(std::thread::spawn(move || loop {
            update_callback();

            let result = cv.1.wait_timeout(cv.0.lock().unwrap(), interval).unwrap();
            let exit = *result.0;
            if exit {
                break;
            }
        }));
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
        self.inner_view
            .get_inner_mut()
            .add_column(column, title, callback);
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
        self.inner_view
            .get_inner_mut()
            .insert_column(i, column, title, callback);
    }

    /// Just a wrapper to cursive_table_view for simplicity
    pub fn sort_by(&mut self, column: H, order: Ordering) {
        self.inner_view.get_inner_mut().sort_by(column, order);
    }

    /// Just a wrapper to cursive_table_view for simplicity
    pub fn set_on_submit<F>(&mut self, cb: F)
    where
        F: Fn(&mut Cursive, usize, usize) + 'static,
    {
        self.inner_view.get_inner_mut().set_on_submit(cb);
    }

    /// Implementation of inner_getters!()
    ///
    /// NOTE: inner_getters() cannot be used since we want to return self.inner_view.inner_view.
    pub fn get_inner(&self) -> &cursive_table_view::TableView<T, H> {
        return self.inner_view.get_inner();
    }
    pub fn get_inner_mut(&mut self) -> &mut cursive_table_view::TableView<T, H> {
        return self.inner_view.get_inner_mut();
    }
}

impl<T, H> ViewWrapper for UpdatingTableView<T, H>
where
    T: 'static + TableViewItem<H>,
    H: 'static + Eq + Hash + Copy + Clone,
{
    wrap_impl_no_move!(self.inner_view: TableView<T, H>);
}
