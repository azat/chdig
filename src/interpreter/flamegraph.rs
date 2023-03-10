use crate::interpreter::clickhouse::Columns;
use anyhow::{Error, Result};
use ncurses;
use std::io::Write;
use std::process::Command;
use tempfile::NamedTempFile;

pub fn show(block: Columns) -> Result<()> {
    let data = block
        .rows()
        .map(|x| {
            vec![
                x.get::<String, _>(0).unwrap(),
                x.get::<u64, _>(1).unwrap().to_string(),
            ]
            .join(" ")
        })
        .collect::<Vec<String>>()
        .join("\n");

    if data.trim().is_empty() {
        // TODO: error in a popup
        return Err(Error::msg("Flamegraph is empty"));
    } else {
        // NOTE: stdin cannot be used since this it is interactive
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(data.as_bytes())?;

        // TODO: replace with builtin implementation (flamegraphs rendering in Rust)
        let mut child = Command::new("chdig-tfg")
            .env("TERMINFO", "/lib/terminfo")
            .arg("-t")
            .arg("pyspy")
            .arg(tmp_file.path().to_str().unwrap())
            .spawn()
            .or_else(|e| {
                Err(Error::msg(format!(
                    "Cannot find/execute chdig-tfg. Check that chdig-tfg is in PATH ({})",
                    e
                )))
            })?;

        let result = child.wait()?;
        // NOTE: tfg does not handle resize correctly and when the screen becomes smaller it fails
        // with _curses.error in addwstr(), and even ignoring this is not enough, since there will
        // be no correct re-draw anyway.
        // And this means that it will not have status WIFSIGNALED, since on SIGWINCH it will
        // eventually exit(1).
        //
        // So what we can do for tfg right now is to re-exec it after SIGWINCH.
        if !result.success() {
            return Err(Error::msg(format!(
                "Error while executing chdig-tfg: {:?} (Note, tfg cannot handle screen changes correctly, have you resizing your terminal?)",
                result
            )));
        }

        // After tfg arrows stops working, fix it:
        ncurses::keypad(ncurses::stdscr(), true);
        // If something else will not work take a look at [1].
        //
        //   [1]: https://github.com/gyscos/cursive/blob/1a0cc868b41232c0d5290a11b4b987ffed757798/cursive/src/backends/curses/n.rs#L115
    }

    return Ok(());
}
