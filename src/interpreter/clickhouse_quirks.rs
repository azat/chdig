use semver::{Version, VersionReq};

#[derive(Debug, Clone, Copy)]
pub enum ClickHouseAvailableQuirks {
    ProcessedElapsed = 1,
}

const QUIRKS: [(&str, ClickHouseAvailableQuirks); 1] = [
    // https://github.com/ClickHouse/ClickHouse/pull/46047
    //
    // NOTE: I use here 22.13 because I have such version in production, which is more or less the
    // same as 23.1
    (
        ">=22.13, <23.2",
        ClickHouseAvailableQuirks::ProcessedElapsed,
    ),
];

pub struct ClickHouseQuirks {
    // Return more verbose version for the UI
    version_string: String,
    mask: u64,
}

impl ClickHouseQuirks {
    pub fn new(version_string: String) -> Self {
        // Version::parse() supports only x.y.z and nothing more.
        let ver_maj_min_patch = version_string.split('.').collect::<Vec<&str>>()[0..3].join(".");
        log::debug!("Version (maj.min.patch): {}", ver_maj_min_patch);

        let version = Version::parse(ver_maj_min_patch.as_str())
            .expect(&format!("Cannot parse version: {}", ver_maj_min_patch));
        let mut mask: u64 = 0;

        for quirk in &QUIRKS {
            let version_requirement = VersionReq::parse(quirk.0).expect(&format!(
                "Cannot parse version requirements for {:?}",
                quirk.1
            ));
            if version_requirement.matches(&version) {
                mask |= quirk.1 as u64;
                log::warn!("Apply quirk {:?}", quirk.1);
            }
        }

        return Self {
            version_string,
            mask,
        };
    }

    pub fn get_version(&self) -> String {
        return self.version_string.clone();
    }

    pub fn has(&self, quirk: ClickHouseAvailableQuirks) -> bool {
        return (self.mask & quirk as u64) != 0;
    }
}
