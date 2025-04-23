use std::sync::{atomic, Arc, Condvar, Mutex};
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
    force: Arc<atomic::AtomicBool>,
    exit: Arc<Mutex<bool>>,
    cv: Arc<(Mutex<()>, Condvar)>,
}

impl Drop for BackgroundRunner {
    fn drop(&mut self) {
        log::debug!("Stopping updates");
        *self.exit.lock().unwrap() = true;
        self.cv.1.notify_all();
        self.thread.take().unwrap().join().unwrap();
        log::debug!("Updates stopped");
    }
}

impl BackgroundRunner {
    pub fn new(
        interval: Duration,
        cv: Arc<(Mutex<()>, Condvar)>,
        force: Arc<atomic::AtomicBool>,
    ) -> Self {
        return Self {
            interval,
            thread: None,
            force,
            exit: Arc::new(Mutex::new(false)),
            cv,
        };
    }

    pub fn start<C: Fn(bool) + std::marker::Send + 'static>(&mut self, callback: C) {
        let interval = self.interval;
        let cv = self.cv.clone();
        let exit = self.exit.clone();
        let force = self.force.clone();
        self.thread = Some(std::thread::spawn(move || loop {
            let was_force = force.swap(false, atomic::Ordering::SeqCst);
            callback(was_force);

            let _ = cv.1.wait_timeout(cv.0.lock().unwrap(), interval).unwrap();
            if *exit.lock().unwrap() {
                break;
            }
        }));
        // Explicitly trigger at least one update with force
        self.schedule();
    }

    pub fn schedule(&mut self) {
        self.force.store(true, atomic::Ordering::SeqCst);
        self.cv.1.notify_all();
    }
}
