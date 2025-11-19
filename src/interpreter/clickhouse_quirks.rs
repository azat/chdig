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

impl ClickHouseQuirks {
    pub fn new(version_string: String) -> Self {
        // Version::parse() supports only x.y.z and nothing more.
        let ver_maj_min_patch = version_string.split('.').collect::<Vec<&str>>()[0..3].join(".");
        log::debug!("Version (maj.min.patch): {}", ver_maj_min_patch);

        let version = Version::parse(ver_maj_min_patch.as_str())
            .unwrap_or_else(|_| panic!("Cannot parse version: {}", ver_maj_min_patch));
        let mut mask: u64 = 0;

        for quirk in &QUIRKS {
            let version_requirement = VersionReq::parse(quirk.0)
                .unwrap_or_else(|_| panic!("Cannot parse version requirements for {:?}", quirk.1));
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
