use anyhow::Result;
use ratatui::Frame;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Position, Rect, Size};
use ratatui::widgets::{Clear, Widget};
use std::any::Any;
use std::io::Stdout;
use std::time::Duration;

use super::component::{Boxed, Canvas, Component};
use super::event::{Callback, Event, EventResult};

/// Callback executed on the UI thread against the App (replaces
/// cursive::CbSink for background workers).
pub type UiCallback = Box<dyn FnOnce(&mut App) + Send>;

#[cfg(unix)]
mod waker {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::sync::Arc;

    /// Self-pipe interrupting the UI loop's poll(2): the loop can sleep on
    /// the terminal fd and still wake instantly on worker callbacks.
    #[derive(Clone)]
    pub struct Waker {
        write_fd: Arc<OwnedFd>,
    }

    pub struct WakeSource {
        read_fd: OwnedFd,
    }

    pub fn pair() -> (Waker, WakeSource) {
        let mut fds = [0i32; 2];
        // SAFETY: out-array of the right size.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "pipe(): {}", std::io::Error::last_os_error());
        for fd in fds {
            unsafe {
                libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        // SAFETY: fresh fds, exclusively owned here.
        let (read_fd, write_fd) =
            unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        (
            Waker {
                write_fd: Arc::new(write_fd),
            },
            WakeSource { read_fd },
        )
    }

    impl Waker {
        pub fn wake(&self) {
            // EAGAIN on a full pipe is fine: a wakeup is already pending.
            unsafe { libc::write(self.write_fd.as_raw_fd(), b"w".as_ptr().cast(), 1) };
        }
    }

    impl WakeSource {
        pub fn raw_fd(&self) -> RawFd {
            self.read_fd.as_raw_fd()
        }

        pub fn drain(&self) {
            let mut buf = [0u8; 64];
            loop {
                let n = unsafe {
                    libc::read(self.read_fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len())
                };
                if n <= 0 {
                    break;
                }
            }
        }
    }
}

#[cfg(not(unix))]
mod waker {
    #[derive(Clone)]
    pub struct Waker;

    pub struct WakeSource;

    pub fn pair() -> (Waker, WakeSource) {
        (Waker, WakeSource)
    }

    impl Waker {
        pub fn wake(&self) {}
    }
}

/// Worker-side handle: queues a callback and wakes the UI loop.
#[derive(Clone)]
pub struct UiSink {
    tx: crossbeam_channel::Sender<UiCallback>,
    waker: waker::Waker,
}

impl UiSink {
    pub fn send(&self, cb: UiCallback) -> Result<(), crossbeam_channel::SendError<UiCallback>> {
        self.tx.send(cb)?;
        self.waker.wake();
        Ok(())
    }
}

pub enum LayerPosition {
    Center,
    FullScreen,
    At(u16, u16),
}

struct Layer {
    view: Boxed,
    position: LayerPosition,
}

pub struct App {
    root: Option<Boxed>,
    layers: Vec<Layer>,
    global_callbacks: Vec<(Event, Callback)>,
    cb_sink: UiSink,
    cb_source: crossbeam_channel::Receiver<UiCallback>,
    wake_source: waker::WakeSource,
    user_data: Option<Box<dyn Any>>,
    screen_size: Size,
    needs_clear: bool,
    running: bool,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    pub fn new() -> Self {
        let (tx, cb_source) = crossbeam_channel::unbounded();
        let (waker, wake_source) = waker::pair();
        Self {
            root: None,
            layers: Vec::new(),
            global_callbacks: Vec::new(),
            cb_sink: UiSink { tx, waker },
            cb_source,
            wake_source,
            user_data: None,
            screen_size: Size::default(),
            needs_clear: false,
            running: true,
        }
    }

    pub fn cb_sink(&self) -> &UiSink {
        &self.cb_sink
    }

    pub fn quit(&mut self) {
        self.running = false;
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Force a full terminal repaint on the next draw (after an external
    /// program used the screen).
    pub fn complete_clear(&mut self) {
        self.needs_clear = true;
    }

    pub fn set_user_data<T: Any>(&mut self, data: T) {
        self.user_data = Some(Box::new(data));
    }

    pub fn user_data<T: Any>(&mut self) -> Option<&mut T> {
        self.user_data.as_mut()?.downcast_mut()
    }

    pub fn screen_size(&self) -> Size {
        self.screen_size
    }

    pub fn add_global_callback<E, F>(&mut self, event: E, cb: F)
    where
        E: Into<Event>,
        F: Fn(&mut App) + Send + Sync + 'static,
    {
        self.global_callbacks
            .push((event.into(), std::sync::Arc::new(cb)));
    }

    /// The base fullscreen view (the first `add_fullscreen_layer`).
    pub fn set_root<V: Component + 'static>(&mut self, view: V) {
        self.root = Some(Boxed::new(view));
    }

    fn push_layer(&mut self, view: Boxed, position: LayerPosition) {
        let mut view = view;
        // Focus the new layer's first focusable widget, otherwise containers
        // keep focus on their first (often non-focusable) child and key
        // presses leak to the global shortcuts.
        view.take_focus();
        self.layers.push(Layer { view, position });
    }

    pub fn add_layer<V: Component + 'static>(&mut self, view: V) {
        self.push_layer(Boxed::new(view), LayerPosition::Center);
    }

    pub fn add_fullscreen_layer<V: Component + 'static>(&mut self, view: V) {
        if self.root.is_none() {
            self.set_root(view);
        } else {
            self.push_layer(Boxed::new(view), LayerPosition::FullScreen);
        }
    }

    pub fn add_layer_at<V: Component + 'static>(&mut self, x: u16, y: u16, view: V) {
        self.push_layer(Boxed::new(view), LayerPosition::At(x, y));
    }

    pub fn pop_layer(&mut self) -> Option<Boxed> {
        self.layers.pop().map(|l| l.view)
    }

    /// Number of layers including the root (cursive's screen().len()).
    pub fn screen_len(&self) -> usize {
        self.layers.len() + self.root.is_some() as usize
    }

    /// Remove the topmost layer containing a view named `name`.
    /// Returns false if no such layer exists.
    pub fn remove_layer_by_name(&mut self, name: &str) -> bool {
        for i in (0..self.layers.len()).rev() {
            let mut found = false;
            super::component::call_on_any(self.layers[i].view.0.as_mut(), name, &mut |_| {
                found = true;
            });
            if found {
                self.layers.remove(i);
                return true;
            }
        }
        false
    }

    pub fn call_on_name<V: Component, F, R>(&mut self, name: &str, cb: F) -> Option<R>
    where
        F: FnOnce(&mut V) -> R,
    {
        let mut cb = Some(cb);
        let mut result = None;
        {
            let mut visit = |comp: &mut dyn Component| {
                if let Some(v) = comp.downcast_mut::<V>()
                    && let Some(cb) = cb.take()
                {
                    result = Some(cb(v));
                }
            };
            for layer in self.layers.iter_mut().rev() {
                super::component::call_on_any(layer.view.0.as_mut(), name, &mut visit);
            }
            if let Some(root) = &mut self.root {
                super::component::call_on_any(root.0.as_mut(), name, &mut visit);
            }
        }
        result
    }

    /// Move focus to the named view. Returns false when not found.
    pub fn focus_name(&mut self, name: &str) -> bool {
        for layer in self.layers.iter_mut().rev() {
            if layer.view.focus_name(name) {
                return true;
            }
        }
        if let Some(root) = &mut self.root {
            return root.focus_name(name);
        }
        false
    }

    pub fn has_view(&mut self, name: &str) -> bool {
        self.focus_name(name)
    }

    pub fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        self.screen_size = Size::new(area.width, area.height);
        let mut canvas = Canvas {
            buf: frame.buffer_mut(),
            cursor: None,
        };

        let layers_len = self.layers.len();
        if let Some(root) = &mut self.root {
            root.draw(&mut canvas, area, layers_len == 0);
        }
        for i in 0..layers_len {
            let focused = i + 1 == layers_len;
            let layer = &mut self.layers[i];
            let rect = match layer.position {
                LayerPosition::FullScreen => area,
                LayerPosition::Center => {
                    let max =
                        Size::new(area.width.saturating_sub(2), area.height.saturating_sub(2));
                    let size = layer.view.required_size(max);
                    Rect::new(
                        area.x + (area.width.saturating_sub(size.width)) / 2,
                        area.y + (area.height.saturating_sub(size.height)) / 2,
                        size.width,
                        size.height,
                    )
                }
                LayerPosition::At(x, y) => {
                    let size = layer.view.required_size(Size::new(
                        area.width.saturating_sub(x),
                        area.height.saturating_sub(y),
                    ));
                    Rect::new(x, y, size.width, size.height)
                }
            };
            if !matches!(layer.position, LayerPosition::At(..)) {
                Clear.render(rect, canvas.buf);
            }
            layer.view.draw(&mut canvas, rect, focused);
        }

        if let Some((x, y)) = canvas.cursor {
            frame.set_cursor_position(Position::new(x, y));
        }
    }

    pub fn on_event(&mut self, event: Event) {
        let result = if let Some(layer) = self.layers.last_mut() {
            layer.view.on_event(&event)
        } else if let Some(root) = &mut self.root {
            root.on_event(&event)
        } else {
            EventResult::Ignored
        };

        match result {
            EventResult::Consumed(Some(cb)) => cb(self),
            EventResult::Consumed(None) => {}
            EventResult::Ignored => {
                let callbacks: Vec<Callback> = self
                    .global_callbacks
                    .iter()
                    .filter(|(trigger, _)| *trigger == event)
                    .map(|(_, cb)| cb.clone())
                    .collect();
                for cb in callbacks {
                    cb(self);
                }
            }
        }
    }

    /// Drain pending worker callbacks (public for headless test harnesses).
    /// Returns true when any callback ran.
    pub fn process_callbacks(&mut self) -> bool {
        let mut any = false;
        while let Ok(cb) = self.cb_source.try_recv() {
            cb(self);
            any = true;
        }
        any
    }

    /// Main loop: draw, wait for input/worker callbacks, dispatch.
    ///
    /// The wait is a poll(2) over the terminal fd and the callback self-pipe,
    /// so the loop sleeps until there is actual work and wakes instantly for
    /// both. crossterm is entered only when the terminal fd is readable (or
    /// on the periodic tick, which also picks up SIGWINCH resizes delivered
    /// to crossterm's internal pipe). No input reader thread, so nested
    /// full-screen apps (flamelens) can take over `event::read()`.
    pub fn run(
        &mut self,
        terminal: &mut ratatui::Terminal<CrosstermBackend<Stdout>>,
    ) -> Result<()> {
        // The draw diff starts from an all-blank back buffer: blank cells are
        // never emitted, so without an explicit clear the previous terminal
        // content shows through.
        terminal.clear()?;
        let mut dirty = true;
        let mut idle_ticks = 0u32;
        // The tick is only a heartbeat: input, worker callbacks, SIGWINCH
        // and log records all wake the poll below through fds. The
        // console-visible per-tick repaint is the non-unix fallback for log
        // records (no self-pipe there); beyond that the tick just caps how
        // long a missed-wakeup bug could freeze the UI, so an idle chdig
        // draws nothing.
        const TICK_MS: i32 = 1000;
        const CONSOLE_TICKS: u32 = 1;
        const BACKSTOP_TICKS: u32 = 30;
        let tty = TtyFd::open();
        #[cfg(unix)]
        let winch = WinchPipe::new();
        // Log records land in the console ring buffer without a UI wakeup:
        // the writer pings the self-pipe instead (no-op waker on non-unix,
        // where the console-visible tick below picks them up).
        {
            let waker = self.cb_sink.waker.clone();
            super::logger::set_ui_waker(Box::new(move || waker.wake()));
        }
        while self.running {
            if self.process_callbacks() {
                dirty = true;
            }
            if super::logger::take_pending_redraw() && super::logger::console_visible(self) {
                dirty = true;
            }
            if !self.running {
                break;
            }
            if self.needs_clear {
                self.needs_clear = false;
                terminal.clear()?;
                dirty = true;
            }
            if dirty
                || idle_ticks >= BACKSTOP_TICKS
                || (idle_ticks >= CONSOLE_TICKS && super::logger::console_visible(self))
            {
                terminal.draw(|frame| self.draw(frame))?;
                dirty = false;
                idle_ticks = 0;
            }

            #[cfg(unix)]
            let (tty_ready, timed_out) = self.wait_for_wakeup(&tty, winch.as_ref(), TICK_MS);
            #[cfg(not(unix))]
            let (tty_ready, timed_out) = self.wait_for_wakeup(&tty, TICK_MS);
            if timed_out {
                idle_ticks += 1;
            }
            if tty_ready {
                // 1ms, not zero: crossterm skips reading the fd entirely on a
                // zero timeout, and this bounds its sub-millisecond poll spin.
                while crossterm::event::poll(Duration::from_millis(1))? {
                    dirty = true;
                    if let Some(event) = Event::from_crossterm(crossterm::event::read()?) {
                        self.on_event(event);
                    }
                    if self.needs_clear {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns (terminal readable/resized, timed out).
    #[cfg(unix)]
    fn wait_for_wakeup(
        &self,
        tty: &TtyFd,
        winch: Option<&WinchPipe>,
        timeout_ms: i32,
    ) -> (bool, bool) {
        use std::os::fd::AsRawFd;
        let make = |fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let mut fds = [
            make(tty.0.as_ref().map(|f| f.as_raw_fd()).unwrap_or(0)),
            make(self.wake_source.raw_fd()),
            make(winch.map(|w| w.read_fd.as_raw_fd()).unwrap_or(-1)),
        ];
        // SAFETY: fds is a valid array of 3 pollfds (negative fds are ignored).
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), 3, timeout_ms) };
        if rc < 0 {
            // EINTR and friends: treat as a spurious wakeup.
            return (false, false);
        }
        if rc == 0 {
            return (false, true);
        }
        if fds[1].revents != 0 {
            self.wake_source.drain();
        }
        let mut input = fds[0].revents != 0;
        if fds[2].revents != 0 {
            if let Some(winch) = winch {
                winch.drain();
            }
            input = true;
        }
        (input, false)
    }

    #[cfg(not(unix))]
    fn wait_for_wakeup(&self, _tty: &TtyFd, timeout_ms: i32) -> (bool, bool) {
        // No self-pipe: fall back to a plain input poll; callbacks are picked
        // up on its timeout.
        match crossterm::event::poll(Duration::from_millis(timeout_ms as u64)) {
            Ok(true) => (true, false),
            _ => (false, true),
        }
    }
}

/// The terminal input fd for poll(2): what crossterm reads from
/// (use-dev-tty), with stdin as the fallback.
struct TtyFd(Option<std::fs::File>);

impl TtyFd {
    fn open() -> Self {
        TtyFd(std::fs::File::open("/dev/tty").ok())
    }
}

/// SIGWINCH self-pipe: resizes are delivered to crossterm's internal pipe
/// which this loop does not poll, so subscribe to the signal too (signal-hook
/// keeps crossterm's own handler working).
#[cfg(unix)]
struct WinchPipe {
    read_fd: std::os::fd::OwnedFd,
    _sig: signal_hook::SigId,
}

#[cfg(unix)]
impl WinchPipe {
    fn new() -> Option<Self> {
        use std::os::fd::FromRawFd;
        let mut fds = [0i32; 2];
        // SAFETY: out-array of the right size.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return None;
        }
        for fd in fds {
            unsafe {
                libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        // SAFETY: fresh fds, exclusively owned here.
        let (read_fd, write_fd) = unsafe {
            (
                std::os::fd::OwnedFd::from_raw_fd(fds[0]),
                std::os::fd::OwnedFd::from_raw_fd(fds[1]),
            )
        };
        let sig = signal_hook::low_level::pipe::register(libc::SIGWINCH, write_fd).ok()?;
        Some(WinchPipe { read_fd, _sig: sig })
    }

    fn drain(&self) {
        use std::os::fd::AsRawFd;
        let mut buf = [0u8; 64];
        loop {
            let n =
                unsafe { libc::read(self.read_fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                break;
            }
        }
    }
}

/// Terminal setup/teardown for the main app (raw mode + alternate screen).
pub struct TerminalGuard;

impl TerminalGuard {
    pub fn enter() -> Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::EnterAlternateScreen,
            crossterm::event::EnableMouseCapture,
            crossterm::cursor::Hide,
        )?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::event::DisableMouseCapture,
            crossterm::style::ResetColor,
            crossterm::cursor::Show,
            crossterm::terminal::LeaveAlternateScreen,
            // The frame must be wiped explicitly: with terminals/multiplexers
            // where the alternate screen is disabled (tmux alternate-screen
            // off) leaving it is a no-op and the UI would stay on screen.
            crossterm::cursor::MoveTo(0, 0),
            crossterm::terminal::Clear(crossterm::terminal::ClearType::All),
        );
        let _ = crossterm::terminal::disable_raw_mode();
    }
}
