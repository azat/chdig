use semver::{Version, VersionReq};

#[derive(Debug, Clone, Copy)]
pub enum ClickHouseAvailableQuirks {
    ProcessesElapsed = 1,
    ProcessesCurrentDatabase = 2,
    AsynchronousMetricsTotalIndexGranularityBytesInMemoryAllocated = 3,
    TraceLogHasSymbols = 4,
    SystemReplicasUUID = 8,
    QueryLogPeakThreadsUsage = 16,
    ProcessesPeakThreadsUsage = 32,
}

// List of quirks (that requires workaround) or new features.
const QUIRKS: [(&str, ClickHouseAvailableQuirks); 7] = [
    // https://github.com/ClickHouse/ClickHouse/pull/46047
    //
    // NOTE: I use here 22.13 because I have such version in production, which is more or less the
    // same as 23.1
    (
        ">=22.13, <23.2",
        ClickHouseAvailableQuirks::ProcessesElapsed,
    ),
    // https://github.com/ClickHouse/ClickHouse/pull/22365
    ("<21.4", ClickHouseAvailableQuirks::ProcessesCurrentDatabase),
    // https://github.com/ClickHouse/ClickHouse/pull/80861
    (
        ">=24.11, <25.6",
        ClickHouseAvailableQuirks::AsynchronousMetricsTotalIndexGranularityBytesInMemoryAllocated,
    ),
    (">=25.1", ClickHouseAvailableQuirks::TraceLogHasSymbols),
    (">=25.11", ClickHouseAvailableQuirks::SystemReplicasUUID),
    // peak_threads_usage is available in system.query_log since 23.8
    (
        ">=23.8",
        ClickHouseAvailableQuirks::QueryLogPeakThreadsUsage,
    ),
    // peak_threads_usage is available in system.processes since 25.11
    (
        ">=25.11",
        ClickHouseAvailableQuirks::ProcessesPeakThreadsUsage,
    ),
];

pub struct ClickHouseQuirks {
    // Return more verbose version for the UI
    version_string: String,
    mask: u64,
}

// Custom matcher, that will properly handle prerelease.
// https://github.com/dtolnay/semver/issues/323#issuecomment-2432169904
fn version_matches(version: &semver::Version, req: &semver::VersionReq) -> bool {
    if req.matches(version) {
        return true;
    }

    // This custom matching logic is needed, because semver cannot compare different version with pre-release tags
    let mut version_without_pre = version.clone();
    version_without_pre.pre = "".parse().unwrap();
    for comp in &req.comparators {
        if comp.matches(version) {
            continue;
        }

        // If major & minor & patch are the same (or omitted),
        // this means there is a mismatch on the pre-release tag
        if comp.major == version.major
            && comp.minor.is_none_or(|m| m == version.minor)
            && comp.patch.is_none_or(|p| p == version.patch)
        {
            return false;
        }

        // Otherwise, compare without pre-release tags
        let mut comp_without_pre = comp.clone();
        comp_without_pre.pre = "".parse().unwrap();
        if !comp_without_pre.matches(&version_without_pre) {
            return false;
        }
    }
    true
}

impl ClickHouseQuirks {
    pub fn new(version_string: String) -> Self {
        // Version::parse() supports only x.y.z and nothing more, but we don't need anything more,
        // only .minor may include new features.
        let components = version_string
            .strip_prefix('v')
            .unwrap_or(&version_string)
            .split('.')
            .collect::<Vec<&str>>();
        let mut ver_maj_min_patch_pre = components[0..3].join(".");
        let version_pre = components.last().unwrap_or(&"-testing");
        if !version_pre.ends_with("-stable") {
            log::warn!(
                "Non-stable version detected ({}), treating as older/development version",
                version_string
            );
            ver_maj_min_patch_pre.push_str(&format!(
                "-{}",
                version_pre
                    .split('-')
                    .collect::<Vec<&str>>()
                    .last()
                    .unwrap_or(&"alpha")
            ));
        }
        log::debug!("Version (maj.min.patch.pre): {}", ver_maj_min_patch_pre);

        let version = Version::parse(ver_maj_min_patch_pre.as_str())
            .unwrap_or_else(|_| panic!("Cannot parse version: {}", ver_maj_min_patch_pre));
        log::debug!("Version: {}", version);

        let mut mask: u64 = 0;
        for quirk in &QUIRKS {
            let version_requirement = VersionReq::parse(quirk.0)
                .unwrap_or_else(|_| panic!("Cannot parse version requirements for {:?}", quirk.1));
            if version_matches(&version, &version_requirement) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stable_version() {
        let quirks = ClickHouseQuirks::new("25.11.1.1-stable".to_string());
        assert_eq!(quirks.get_version(), "25.11.1.1-stable");
        assert!(quirks.has(ClickHouseAvailableQuirks::SystemReplicasUUID));
        assert!(quirks.has(ClickHouseAvailableQuirks::ProcessesPeakThreadsUsage));
        assert!(quirks.has(ClickHouseAvailableQuirks::TraceLogHasSymbols));
    }

    #[test]
    fn test_testing_version() {
        let quirks = ClickHouseQuirks::new("25.11.1.1-testing".to_string());
        assert_eq!(quirks.get_version(), "25.11.1.1-testing");
        assert!(!quirks.has(ClickHouseAvailableQuirks::SystemReplicasUUID));
        assert!(!quirks.has(ClickHouseAvailableQuirks::ProcessesPeakThreadsUsage));
    }

    #[test]
    fn test_next_testing_prerelease_version() {
        let quirks = ClickHouseQuirks::new("25.12.1.1-testing".to_string());
        assert_eq!(quirks.get_version(), "25.12.1.1-testing");
        assert!(quirks.has(ClickHouseAvailableQuirks::SystemReplicasUUID));
        assert!(quirks.has(ClickHouseAvailableQuirks::ProcessesPeakThreadsUsage));
    }

    #[test]
    fn test_version_with_v_prefix() {
        let quirks = ClickHouseQuirks::new("v25.11.1.1-stable".to_string());
        assert_eq!(quirks.get_version(), "v25.11.1.1-stable");
        assert!(quirks.has(ClickHouseAvailableQuirks::SystemReplicasUUID));
    }

    // Here are the tests only for version_matches(), in other aspects we are relying on semver tests
}
