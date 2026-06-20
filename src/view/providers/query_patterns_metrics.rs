// Metrics available for the "Query patterns" view. Each metric drives both the
// sortable "total" column and the per-bucket heatmap on the right.
//
// agg_expr   — final aggregate over the grouped rows (e.g. "sum(memory_usage)").
// bucket_value/bucket_agg — per-row expression and aggregation function used by
//                          `{bucket_agg}Resample(...)` to build the heatmap.
// unit       — how the "total" value is rendered (counts must not be byte-formatted).

use crate::view::Unit;
use std::sync::OnceLock;

pub struct Metric {
    pub key: &'static str,
    pub label: &'static str,
    pub agg_expr: &'static str,
    pub bucket_value: &'static str,
    pub bucket_agg: &'static str,
    pub unit: Unit,
}

pub const METRICS: &[Metric] = &[
    Metric {
        key: "count",
        label: "count",
        agg_expr: "count()",
        bucket_value: "1",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "avg_duration",
        label: "avg duration",
        agg_expr: "avg(query_duration_ms)",
        bucket_value: "query_duration_ms",
        bucket_agg: "avg",
        unit: Unit::Milliseconds,
    },
    Metric {
        key: "max_duration",
        label: "max duration",
        agg_expr: "max(query_duration_ms)",
        bucket_value: "query_duration_ms",
        bucket_agg: "max",
        unit: Unit::Milliseconds,
    },
    Metric {
        key: "total_duration",
        label: "total duration",
        agg_expr: "sum(query_duration_ms)",
        bucket_value: "query_duration_ms",
        bucket_agg: "sum",
        unit: Unit::Milliseconds,
    },
    Metric {
        key: "cpu_time",
        label: "cpu time",
        agg_expr: "sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])",
        bucket_value: "ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']",
        bucket_agg: "sum",
        unit: Unit::Microseconds,
    },
    Metric {
        key: "read_bytes",
        label: "read bytes",
        agg_expr: "sum(read_bytes)",
        bucket_value: "read_bytes",
        bucket_agg: "sum",
        unit: Unit::Bytes,
    },
    Metric {
        key: "written_bytes",
        label: "written bytes",
        agg_expr: "sum(written_bytes)",
        bucket_value: "written_bytes",
        bucket_agg: "sum",
        unit: Unit::Bytes,
    },
    Metric {
        key: "avg_written_rows",
        label: "avg written rows",
        agg_expr: "avg(written_rows)",
        bucket_value: "written_rows",
        bucket_agg: "avg",
        unit: Unit::Count,
    },
    Metric {
        key: "result_bytes",
        label: "result bytes",
        agg_expr: "sum(result_bytes)",
        bucket_value: "result_bytes",
        bucket_agg: "sum",
        unit: Unit::Bytes,
    },
    Metric {
        key: "network_bytes",
        label: "network bytes",
        agg_expr: "sum(ProfileEvents['NetworkReceiveBytes']+ProfileEvents['NetworkSendBytes'])",
        bucket_value: "ProfileEvents['NetworkReceiveBytes']+ProfileEvents['NetworkSendBytes']",
        bucket_agg: "sum",
        unit: Unit::Bytes,
    },
    Metric {
        key: "memory",
        label: "memory",
        agg_expr: "sum(memory_usage)",
        bucket_value: "memory_usage",
        bucket_agg: "sum",
        unit: Unit::Bytes,
    },
    Metric {
        key: "max_memory",
        label: "max memory",
        agg_expr: "max(memory_usage)",
        bucket_value: "memory_usage",
        bucket_agg: "max",
        unit: Unit::Bytes,
    },
    Metric {
        key: "network_wait",
        label: "network wait",
        agg_expr: "sum(ProfileEvents['NetworkSendElapsedMicroseconds']+ProfileEvents['NetworkReceiveElapsedMicroseconds'])",
        bucket_value: "ProfileEvents['NetworkSendElapsedMicroseconds']+ProfileEvents['NetworkReceiveElapsedMicroseconds']",
        bucket_agg: "sum",
        unit: Unit::Microseconds,
    },
    Metric {
        key: "io_time",
        label: "io time",
        agg_expr: "sum(ProfileEvents['DiskReadElapsedMicroseconds']+ProfileEvents['DiskWriteElapsedMicroseconds'])",
        bucket_value: "ProfileEvents['DiskReadElapsedMicroseconds']+ProfileEvents['DiskWriteElapsedMicroseconds']",
        bucket_agg: "sum",
        unit: Unit::Microseconds,
    },
    Metric {
        key: "io_wait",
        label: "io wait",
        agg_expr: "sum(ProfileEvents['OSIOWaitMicroseconds'])",
        bucket_value: "ProfileEvents['OSIOWaitMicroseconds']",
        bucket_agg: "sum",
        unit: Unit::Microseconds,
    },
    Metric {
        key: "zk_txns",
        label: "zk txns",
        agg_expr: "sum(ProfileEvents['ZooKeeperTransactions'])",
        bucket_value: "ProfileEvents['ZooKeeperTransactions']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "parts_inserted",
        label: "parts inserted",
        agg_expr: "sum(ProfileEvents['InsertedCompactParts']+ProfileEvents['InsertedWideParts'])",
        bucket_value: "ProfileEvents['InsertedCompactParts']+ProfileEvents['InsertedWideParts']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "parts_inserted_avg",
        label: "parts inserted avg",
        agg_expr: "avg(ProfileEvents['InsertedCompactParts']+ProfileEvents['InsertedWideParts'])",
        bucket_value: "ProfileEvents['InsertedCompactParts']+ProfileEvents['InsertedWideParts']",
        bucket_agg: "avg",
        unit: Unit::Count,
    },
    Metric {
        key: "marks_load_time",
        label: "marks load time",
        agg_expr: "sum(ProfileEvents['WaitMarksLoadMicroseconds'])",
        bucket_value: "ProfileEvents['WaitMarksLoadMicroseconds']",
        bucket_agg: "sum",
        unit: Unit::Microseconds,
    },
    Metric {
        key: "selected_parts",
        label: "selected parts",
        agg_expr: "sum(ProfileEvents['SelectedParts'])",
        bucket_value: "ProfileEvents['SelectedParts']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "selected_ranges",
        label: "selected ranges",
        agg_expr: "sum(ProfileEvents['SelectedRanges'])",
        bucket_value: "ProfileEvents['SelectedRanges']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "selected_marks",
        label: "selected marks",
        agg_expr: "sum(ProfileEvents['SelectedMarks'])",
        bucket_value: "ProfileEvents['SelectedMarks']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "exceptions",
        label: "exceptions",
        agg_expr: "sum(exception_code!=0)",
        bucket_value: "exception_code!=0",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "open_files",
        label: "open files",
        agg_expr: "sum(ProfileEvents['FileOpen'])",
        bucket_value: "ProfileEvents['FileOpen']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "external_processing_files",
        label: "spill files",
        agg_expr: "sum(ProfileEvents['ExternalProcessingFilesTotal'])",
        bucket_value: "ProfileEvents['ExternalProcessingFilesTotal']",
        bucket_agg: "sum",
        unit: Unit::Count,
    },
    Metric {
        key: "threads_peak",
        label: "threads (peak)",
        agg_expr: "max(peak_threads_usage)",
        bucket_value: "peak_threads_usage",
        bucket_agg: "max",
        unit: Unit::Count,
    },
    Metric {
        key: "threads_total",
        label: "threads (total)",
        agg_expr: "max(length(thread_ids))",
        bucket_value: "length(thread_ids)",
        bucket_agg: "max",
        unit: Unit::Count,
    },
];

pub const DEFAULT_METRIC_KEY: &str = "memory";

pub fn find(key: &str) -> Option<&'static Metric> {
    METRICS.iter().find(|m| m.key == key)
}

/// Per-metric hidden column names `(key, total_col, heatmap_col)` for the fat
/// "Query patterns" query, where every metric is computed at once and the view
/// switches between them client-side. Names must be `&'static str` (the view
/// keys columns by static name) yet are derived from `key`, so they are leaked
/// once for the whole process (bounded by METRICS.len(), independent of how
/// many times the view is opened).
pub fn metric_columns() -> &'static [(&'static str, &'static str, &'static str)] {
    static CELL: OnceLock<Vec<(&'static str, &'static str, &'static str)>> = OnceLock::new();
    CELL.get_or_init(|| {
        METRICS
            .iter()
            .map(|m| {
                let total: &'static str = Box::leak(format!("_total_{}", m.key).into_boxed_str());
                let hm: &'static str = Box::leak(format!("_hm_{}", m.key).into_boxed_str());
                (m.key, total, hm)
            })
            .collect()
    })
}

/// `(total_col, heatmap_col)` for the given metric key.
pub fn cols_for(key: &str) -> (&'static str, &'static str) {
    let cols = metric_columns()
        .iter()
        .find(|c| c.0 == key)
        .unwrap_or(&metric_columns()[0]);
    (cols.1, cols.2)
}

pub fn default_metric() -> &'static Metric {
    find(DEFAULT_METRIC_KEY).unwrap_or(&METRICS[0])
}
