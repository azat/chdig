use cursive::event::Key;
use cursive::view::{Nameable, Resizable};
use cursive::views;

mod interpreter;
mod view;

use crate::interpreter::{options, Context, ContextArc, WorkerEvent};

#[tokio::main]
async fn main() {
    let options = options::parse();
    let mut siv = cursive::default();

    let context: ContextArc = Context::new(options, siv.cb_sink().clone()).await;
    let context_ref = context.clone();

    let theme = view::utils::make_cursive_theme_from_therminal(&siv);
    siv.set_theme(theme);

    view::utils::add_menu(&mut siv);
    view::utils::add_fuzzy_shortcuts(&mut siv);

    // TODO: Bindings:
    // - C-J - show the end of the queries (like in top(1))
    siv.add_global_callback('q', view::utils::pop_ui);
    siv.add_global_callback('F', |siv: &mut cursive::Cursive| {
        siv.user_data::<ContextArc>()
            .unwrap()
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::ShowServerFlameGraph);
    });
    siv.add_global_callback(Key::Backspace, view::utils::pop_ui);
    // NOTE: Do not find to Esc, since this breaks other bindings (Home/End/...)`
    siv.add_global_callback(Key::F1, view::utils::show_help_dialog);
    // TODO: build the same as DebugView but with public crate for logging.
    // siv.add_global_callback('~', Cursive::toggle_debug_console);
    siv.set_user_data(context);
    // TODO: disable mouse support (sigh)

    // TODO: Bindings:
    // - space - multiquery selection (KILL, flamegraphs, logs, ...)
    //
    // TODO:
    // - update the table
    siv.add_layer(
        views::Dialog::around(
            view::ProcessesView::new(context_ref.clone())
                .expect("Cannot get processlist")
                .with_name("processes")
                .min_size((500, 200)),
        )
        .title(format!(
            "processlist ({})",
            context_ref.lock().unwrap().server_version
        )),
    );

    // TODO: add custom timer since 1 update per second is too much
    // siv.set_fps(1);

    // TODO: std::panic::set_hook() that will reset the terminal back
    siv.run();
}
