use std::sync::{Arc, Condvar, Mutex, atomic};
use std::thread;
use std::time::{Duration, Instant};

/// Runs periodic tasks in background thread.
///
/// The condvar is shared between all runners, so a notification is only a
/// hint to wake up and check your own signals: the private force flag (set by
/// schedule()) and the shared refresh generation (bumped by
/// Context::trigger_view_refresh() for every runner subscribed to it).
/// Wakeups without either signal go back to sleep until the interval elapses,
/// so runners never fire on other runners' notifications.
///
/// It is OK to suppress unused warning for this code, since it join the thread in drop()
/// correctly, example:
///
/// ``rust
/// pub struct SomeView {
///     #[allow(unused)]
///     bg_runner: BackgroundRunner,
/// }
/// ``
///
pub struct BackgroundRunner {
    interval: Duration,
    thread: Option<thread::JoinHandle<()>>,
    force: Arc<atomic::AtomicBool>,
    generation: Arc<atomic::AtomicU64>,
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
        generation: Arc<atomic::AtomicU64>,
    ) -> Self {
        return Self {
            interval,
            thread: None,
            force: Arc::new(atomic::AtomicBool::new(false)),
            generation,
            exit: Arc::new(Mutex::new(false)),
            cv,
        };
    }

    pub fn start<C: Fn(bool) + std::marker::Send + 'static>(&mut self, callback: C) {
        let interval = self.interval;
        let cv = self.cv.clone();
        let exit = self.exit.clone();
        let force = self.force.clone();
        let generation = self.generation.clone();
        self.thread = Some(std::thread::spawn(move || {
            let mut seen_generation = generation.load(atomic::Ordering::SeqCst);
            loop {
                let was_force = force.swap(false, atomic::Ordering::SeqCst);
                let current_generation = generation.load(atomic::Ordering::SeqCst);
                let was_refresh = current_generation != seen_generation;
                seen_generation = current_generation;
                callback(was_force || was_refresh);

                if *exit.lock().unwrap() {
                    break;
                }

                let deadline = Instant::now() + interval;
                loop {
                    let timeout = deadline.saturating_duration_since(Instant::now());
                    if timeout.is_zero() {
                        break;
                    }
                    let (guard, result) = cv.1.wait_timeout(cv.0.lock().unwrap(), timeout).unwrap();
                    drop(guard);
                    if *exit.lock().unwrap()
                        || result.timed_out()
                        || force.load(atomic::Ordering::SeqCst)
                        || generation.load(atomic::Ordering::SeqCst) != seen_generation
                    {
                        break;
                    }
                }
                if *exit.lock().unwrap() {
                    break;
                }
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
