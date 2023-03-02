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
        // TODO: replace with builtin implementation
        // TODO: handle SIGWINCH
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(data.as_bytes())?;

        // NOTE: stdin cannot be used since this it is interactive
        Command::new("chdig-tfg")
            .env("TERMINFO", "/lib/terminfo")
            .arg("-t")
            .arg("pyspy")
            .arg(tmp_file.path().to_str().unwrap())
            .status()
            .or_else(|e| {
                Err(Error::msg(format!(
                    "Cannot find/execute chdig-tfg. Check that chdig-tfg is in PATH ({})",
                    e
                )))
            })?;

        // After tfg arrows stops working, fix it:
        ncurses::keypad(ncurses::stdscr(), true);
        // If something else will not work take a look at [1].
        //
        //   [1]: https://github.com/gyscos/cursive/blob/1a0cc868b41232c0d5290a11b4b987ffed757798/cursive/src/backends/curses/n.rs#L115
    }

    return Ok(());
}
