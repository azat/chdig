use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::wrap_impl_no_move;
use cursive::{inner_getters, view::ViewWrapper};

pub struct UpdatingView<V> {
    inner_view: V,
    update_interval: Duration,

    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl<V> Drop for UpdatingView<V> {
    fn drop(&mut self) {
        log::debug!("Stopping updates");
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
        log::debug!("Updates stopped");
    }
}

impl<V> UpdatingView<V>
where
    V: ViewWrapper + Default + 'static,
{
    inner_getters!(self.inner_view: V);

    pub fn new<C: Fn() + std::marker::Send + 'static>(
        update_interval: Duration,
        update_callback: C,
    ) -> Self {
        let mut view = Self {
            inner_view: V::default(),
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
}

impl<V> ViewWrapper for UpdatingView<V>
where
    V: ViewWrapper + 'static,
{
    wrap_impl_no_move!(self.inner_view: V);
}
