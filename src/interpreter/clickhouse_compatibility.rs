use semver::{Version, VersionReq};

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
pub enum ClickHouseCompatibilitySettings {
    storage_system_stack_trace_pipe_read_timeout_ms = 1,
}

const SETTINGS: [(&str, ClickHouseCompatibilitySettings); 1] = [(
    ">=23.5",
    ClickHouseCompatibilitySettings::storage_system_stack_trace_pipe_read_timeout_ms,
)];

pub struct ClickHouseCompatibility {
    mask: u64,
}

impl ClickHouseCompatibility {
    pub fn new(version_string: String) -> Self {
        // Version::parse() supports only x.y.z and nothing more.
        let ver_maj_min_patch = version_string.split('.').collect::<Vec<&str>>()[0..3].join(".");
        log::debug!("Version (maj.min.patch): {}", ver_maj_min_patch);

        let version = Version::parse(ver_maj_min_patch.as_str())
            .expect(&format!("Cannot parse version: {}", ver_maj_min_patch));
        let mut mask: u64 = 0;

        for setting in &SETTINGS {
            let version_requirement = VersionReq::parse(setting.0).expect(&format!(
                "Cannot parse version requirements for {:?}",
                setting.1
            ));
            if version_requirement.matches(&version) {
                mask |= setting.1 as u64;
                log::warn!("Apply setting {:?}", setting.1);
            }
        }

        return Self { mask };
    }

    pub fn has(&self, setting: ClickHouseCompatibilitySettings) -> bool {
        return (self.mask & setting as u64) != 0;
    }
}
