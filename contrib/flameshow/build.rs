use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // env
    let out_dir = env::var("OUT_DIR")?;
    let src_dir = env::var("CARGO_MANIFEST_DIR")?;
    let target = env::var("TARGET")?;
    let profile = env::var("PROFILE")?;

    // output
    let pyembedded_dir = format!("{}/pyembedded", out_dir);
    let pyo3_config_path = format!("{}/pyo3-build-config-file.txt", pyembedded_dir);
    let pyoxidize_python_config_path = format!("{}/default_python_config.rs", pyembedded_dir);
    let pyoxidize_packed_resource_path = format!(
        "{}/build/{}/{}/resources/packed-resources",
        src_dir, target, profile
    );
    let target_dir = format!("{}/build", src_dir);

    // pyoxidizer build resources
    if !Path::new(&pyoxidize_packed_resource_path).exists() {
        let output = Command::new("pyoxidizer")
            .arg("build")
            .arg("resources")
            .arg("--path")
            .arg(&src_dir)
            .arg("--target-triple")
            .arg(&target)
            .args(if profile == "release" {
                Vec::from(["--release"])
            } else {
                Vec::new()
            })
            .output()
            .expect("Cannot generate resources");
        assert!(output.status.success(), "{:?}", output);
    }

    // pyoxidizer generate-python-embedding-artifacts
    if !Path::new(&pyembedded_dir).exists() {
        let output = Command::new("pyoxidizer")
            .arg("generate-python-embedding-artifacts")
            .arg("--target-triple")
            .arg(&target)
            .arg(&pyembedded_dir)
            .output()
            .expect("Cannot generate artifacts");
        assert!(output.status.success(), "{:?}", output);

        // Remove libm from pyo3 config (causes undefined symbols during linking)
        let pyo3_config = fs::read_to_string(&pyo3_config_path)
            .map_err(|e| format!("Cannot read {} ({})", &pyo3_config_path, e))?;
        let pyo3_config =
            pyo3_config.replace("extra_build_script_line=cargo:rustc-link-lib=m\n", "");
        fs::write(&pyo3_config_path, pyo3_config)?;

        // Update location to the packed-resources
        let pyoxidize_python_config = fs::read_to_string(&pyoxidize_python_config_path)
            .map_err(|e| format!("Cannot read {} ({})", &pyoxidize_python_config_path, e))?;
        let pyoxidize_python_config =
            pyoxidize_python_config.replace("packed-resources", &pyoxidize_packed_resource_path);
        fs::write(&pyoxidize_python_config_path, pyoxidize_python_config)?;
    }

    // I have no idea how to pass flags to the child build.rs,
    // so I'm creating more generic name for the config
    let pyo3_config_alias = format!(
        "{}/{}-{}.txt",
        target_dir,
        Path::new(&pyo3_config_path)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap(),
        target,
    );
    let _status = fs::remove_file(&pyo3_config_alias);
    fs::copy(&pyo3_config_path, &pyo3_config_alias).map_err(|e| {
        format!(
            "Cannot copy {} to {} ({})",
            &pyo3_config_path, &pyo3_config_alias, e
        )
    })?;

    // NOTE: we cannot configure pyo3 from the flameshow build script (sigh)
    // that's why we have symlink above.
    //
    // configure pyo3
    // println!("cargo:rustc-env=PYO3_CONFIG_FILE={}", &pyo3_config_path);

    // configure pyoxidize
    println!(
        "cargo:rustc-env=PYOXIDIZE_PYTHON_CONFIG_FILE={}",
        &pyoxidize_python_config_path
    );

    return Ok(());
}
