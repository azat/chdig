use crate::interpreter::clickhouse::Columns;
use anyhow::{Error, Result};
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
        let _ = Command::new("chdig-tfg")
            .env("TERMINFO", "/lib/terminfo")
            .arg("-t")
            .arg("pyspy")
            .arg(tmp_file.path().to_str().unwrap())
            .status();

        // FIXME:
        // - after tfg some shortcuts are broken
    }

    return Ok(());
}
