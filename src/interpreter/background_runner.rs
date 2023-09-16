use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Runs periodic tasks in background thread.
///
/// It is OK to suppress unused warning for this code, since it join the thread in drop()
/// correctly, example:
///
/// ```rust
/// pub struct Context {
///     #[allow(unused)]
///     bg_runner: BackgroundRunner,
/// }
/// ```
///
pub struct BackgroundRunner {
    interval: Duration,
    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for BackgroundRunner {
    fn drop(&mut self) {
        log::debug!("Stopping updates");
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
        log::debug!("Updates stopped");
    }
}

impl BackgroundRunner {
    pub fn new(interval: Duration) -> Self {
        return Self {
            interval,
            thread: None,
            cv: Arc::new((Mutex::new(false), Condvar::new())),
        };
    }

    pub fn start<C: Fn() + std::marker::Send + 'static>(&mut self, callback: C) {
        let interval = self.interval;
        let cv = self.cv.clone();
        self.thread = Some(std::thread::spawn(move || loop {
            callback();

            let result = cv.1.wait_timeout(cv.0.lock().unwrap(), interval).unwrap();
            let exit = *result.0;
            if exit {
                break;
            }
        }));
    }

    pub fn schedule(&mut self) {
        self.cv.1.notify_one();
    }
}
