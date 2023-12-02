use anyhow::Result;
use pyo3::types::PyDict;

include!(env!("PYOXIDIZE_PYTHON_CONFIG_FILE"));

pub fn flameshow(profile_data: &str, profile_name: &str) -> Result<()> {
    let mut config = default_python_config();

    // Do not inherit arguments from chdig to python
    config.argv = Some(Vec::new());

    let interp = pyembed::MainPythonInterpreter::new(config)?;
    interp.with_gil(|py| {
        let locals = PyDict::new(py);
        locals.set_item("profile_data", profile_data).unwrap();
        locals.set_item("profile_name", profile_name).unwrap();
        py.run(
            &format!(
                "
from flameshow.parsers import parse
from flameshow.render import FlameshowApp

# flameshow.parseres.parse() accept bytes (and will actually fail without any errors for str)
profile_data = profile_data.encode()
profile = parse(profile_data, profile_name)
app = FlameshowApp(profile)
app.run()",
            ),
            None,
            Some(locals),
        )
        .unwrap();
        // NOTE: we cannot return error since it leads to SIGSEGV (even with simple python code)
    });
    return Ok(());
}
