use crate::interpreter::Query;
use crate::interpreter::clickhouse::{MetricLogRow, QueryMetricRow, column_as_string};
use anyhow::Result;
use chrono::{DateTime, Local};
use chrono_tz::Tz;
use clickhouse_rs::{Block, types::ColumnType};
use perfetto_protos::android_log::AndroidLogPacket;
use perfetto_protos::android_log::android_log_packet::LogEvent;
use perfetto_protos::android_log_constants::AndroidLogPriority;
use perfetto_protos::clock_snapshot::ClockSnapshot;
use perfetto_protos::clock_snapshot::clock_snapshot::Clock;
use perfetto_protos::counter_descriptor::CounterDescriptor;
use perfetto_protos::counter_descriptor::counter_descriptor::Unit;
use perfetto_protos::debug_annotation::DebugAnnotation;
use perfetto_protos::debug_annotation::debug_annotation as da;
use perfetto_protos::interned_data::InternedData;
use perfetto_protos::process_tree::ProcessTree;
use perfetto_protos::process_tree::process_tree::Process as PtProcess;
use perfetto_protos::profile_common::{Callstack, Frame, InternedString, Mapping};
use perfetto_protos::profile_packet::StreamingProfilePacket;
use perfetto_protos::thread_descriptor::ThreadDescriptor as PerfettoThreadDescriptor;
use perfetto_protos::trace::Trace;
use perfetto_protos::trace_packet::TracePacket;
use perfetto_protos::trace_packet::trace_packet::Data;
use perfetto_protos::track_descriptor::TrackDescriptor;
use perfetto_protos::track_descriptor::track_descriptor::Static_or_dynamic_name;
use perfetto_protos::track_event::TrackEvent;
use perfetto_protos::track_event::track_event::{Counter_value_field, Name_field, Type};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use protobuf::{EnumOrUnknown, Message, MessageField};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const SEQUENCE_ID: u32 = 1;
// Sequence-scoped clock (>=64), mapped to BOOTTIME via ClockSnapshot.
// All TrackEvent packets (slices, counters) use this clock on SEQUENCE_ID.
//
// Clock timeline notes:
// - Clock 128 is sequence-scoped: the ClockSnapshot on SEQUENCE_ID defines it
//   ONLY for that sequence. Other sequences cannot use it (see add_stack_traces).
// - The ClockSnapshot must be the first packet (timestamp=0, self-referencing).
//   Using a non-zero timestamp in clock 6 (BOOTTIME) instead doesn't work reliably.
// - The first make_packet() call emits SEQ_INCREMENTAL_STATE_CLEARED (flags=1).
//   This is safe because it's a TrackDescriptor without a timestamp (processed
//   inline before the ClockSnapshot enters the sort queue).
// - Never emit SEQ_INCREMENTAL_STATE_CLEARED on timestamped packets sharing this
//   sequence — it destroys the clock mapping for all subsequent packets.
const CLOCK_ID_UNIXTIME: u32 = 128;

// Perfetto requires each outer compressed_packets TracePacket (including the
// two field ids and sizes) to be <= 512 KiB. Cap the *pre-compression* inner
// payload below that limit; deflate output is bounded by inner size (worst
// case is identity), so this guarantees the outer wire size fits.
const COMPRESS_BATCH_LIMIT: usize = 480 * 1024;

struct Sample {
    callstack_iid: u64,
    timestamp_us: i64,
}

/// A built trace on disk. Temporary traces are anonymous (the file is
/// unlinked at creation), so the kernel reclaims them no matter how the
/// process dies — only the fd held here keeps the data alive.
pub struct TraceFile {
    file: Arc<File>,
    size: u64,
}

impl TraceFile {
    pub fn size(&self) -> u64 {
        self.size
    }

    fn reader(&self) -> TraceReader {
        TraceReader {
            file: self.file.clone(),
            pos: 0,
        }
    }
}

// Independent cursor over the shared trace fd: concurrent HTTP requests must
// not share a file position, and an anonymous file has no path to reopen.
struct TraceReader {
    file: Arc<File>,
    pos: u64,
}

impl std::io::Read for TraceReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = read_at(&self.file, self.pos, buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(not(unix))]
fn read_at(mut file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;
    file.read(buf)
}

pub struct PerfettoTraceBuilder {
    // Packets are streamed to disk as they are added (a Trace is just a
    // repeated TracePacket, so concatenated single-packet Trace messages form
    // a valid trace), keeping only the interning state in memory.
    out: BufWriter<File>,
    write_error: Option<protobuf::Error>,
    next_uuid: u64,
    next_sequence_id: u32,
    first_event_emitted: bool,

    function_name_iids: HashMap<String, u64>,
    frame_iids: HashMap<(u64, u64), u64>,
    callstack_iids: HashMap<Vec<u64>, u64>,
    next_intern_id: u64,

    // Streamed stack-trace state, flushed by finalize_stack_traces() in build()
    stack_mapping_iid: Option<u64>,
    stack_callstacks_by_hash: HashMap<(String, u64), u64>,
    stack_interned_strings: Vec<InternedString>,
    stack_interned_frames: Vec<Frame>,
    stack_interned_callstacks: Vec<Callstack>,
    // Global: trace_type → samples
    stack_samples_by_type: HashMap<String, Vec<Sample>>,
    // Per-server: (host_name, trace_type) → samples
    stack_samples_by_host_type: HashMap<(String, String), Vec<Sample>>,

    host_uuids: HashMap<String, u64>,
    // (host_name, category) → category track uuid
    host_category_uuids: HashMap<(String, &'static str), u64>,
    // host_name → synthetic pid, used to resolve hostnames for AndroidLogPacket
    // events via ProcessTree (legacy pid/tid based resolution, unrelated to the
    // TrackEvent uuid tracks above).
    host_pids: HashMap<String, i32>,
    // (parent uuid, name) → uuid, parent 0 = process track. Streamed add_*
    // methods run once per block, so tracks must be cached across calls.
    track_uuids: HashMap<(u64, String), u64>,
    // Running totals of cumulative counters (per track uuid), must survive
    // across blocks as well.
    counter_totals: HashMap<u64, i64>,
    per_server: bool,
    text_log_android: bool,

    // Batching state for compressed_packets. When compress=true, every
    // add_*-emitted TracePacket goes through pending instead of being written
    // straight to disk; flush_batch() deflates the accumulated inner Trace
    // and emits a single outer compressed_packets TracePacket.
    compress: bool,
    pending: Vec<TracePacket>,
    pending_size: usize,
}

impl PerfettoTraceBuilder {
    pub fn new(
        path: PathBuf,
        per_server: bool,
        text_log_android: bool,
        compress: bool,
    ) -> Result<Self> {
        let file = File::create(&path)?;
        Ok(Self::with_output(
            file,
            per_server,
            text_log_android,
            compress,
        ))
    }

    /// Trace in an anonymous temporary file (unlinked at creation): it can
    /// never outlive the process, even on SIGKILL. The HTTP server reads it
    /// through the fd kept in the TraceFile returned by build().
    pub fn new_temp(per_server: bool, text_log_android: bool, compress: bool) -> Result<Self> {
        let file = tempfile::tempfile()?;
        Ok(Self::with_output(
            file,
            per_server,
            text_log_android,
            compress,
        ))
    }

    fn with_output(file: File, per_server: bool, text_log_android: bool, compress: bool) -> Self {
        let mut builder = PerfettoTraceBuilder {
            out: BufWriter::new(file),
            write_error: None,
            next_uuid: 1,
            next_sequence_id: SEQUENCE_ID + 1,
            first_event_emitted: false,

            function_name_iids: HashMap::new(),
            frame_iids: HashMap::new(),
            callstack_iids: HashMap::new(),
            next_intern_id: 1,

            stack_mapping_iid: None,
            stack_callstacks_by_hash: HashMap::new(),
            stack_interned_strings: Vec::new(),
            stack_interned_frames: Vec::new(),
            stack_interned_callstacks: Vec::new(),
            stack_samples_by_type: HashMap::new(),
            stack_samples_by_host_type: HashMap::new(),

            host_uuids: HashMap::new(),
            host_category_uuids: HashMap::new(),
            host_pids: HashMap::new(),
            track_uuids: HashMap::new(),
            counter_totals: HashMap::new(),
            per_server,
            text_log_android,
            compress,
            pending: Vec::new(),
            pending_size: 0,
        };

        // ClockSnapshot with timestamp=0 in its own clock (self-referencing).
        // The trace processor resolves this specially for ClockSnapshot packets,
        // placing it at the very start of the trace (time 0). Always written
        // uncompressed at the head so this special handling is preserved.
        let cs = Self::make_clock_snapshot();
        let mut cs_pkt = TracePacket::new();
        cs_pkt.set_trusted_packet_sequence_id(SEQUENCE_ID);
        cs_pkt.sequence_flags = Some(1 | 2);
        cs_pkt.timestamp = Some(0);
        cs_pkt.timestamp_clock_id = Some(CLOCK_ID_UNIXTIME);
        cs_pkt.data = Some(Data::ClockSnapshot(cs));
        builder.write_packet_raw(cs_pkt);

        builder
    }

    // All add_* methods stay infallible: the first write error is remembered
    // and surfaced by build(), later writes become no-ops (ferror() pattern).
    fn write_packet(&mut self, pkt: TracePacket) {
        if self.write_error.is_some() {
            return;
        }
        if !self.compress {
            self.write_packet_raw(pkt);
            return;
        }
        let size = pkt.compute_size() as usize;
        // Plus the per-packet wire framing (field tag + length varint) for
        // both the inner Trace and the outer single-packet Trace concat.
        let framed = size + 8;
        if self.pending_size + framed > COMPRESS_BATCH_LIMIT && !self.pending.is_empty() {
            self.flush_batch();
        }
        self.pending_size += framed;
        self.pending.push(pkt);
    }

    /// Write a TracePacket to disk uncompressed, framed as a single-packet
    /// Trace message (compatible with the concatenated-Trace on-disk format).
    fn write_packet_raw(&mut self, pkt: TracePacket) {
        if self.write_error.is_some() {
            return;
        }
        let mut trace = Trace::new();
        trace.packet.push(pkt);
        if let Err(e) = trace.write_to_writer(&mut self.out) {
            self.write_error = Some(e);
        }
    }

    /// Deflate the pending inner packets and emit one outer TracePacket
    /// carrying the bytes in `compressed_packets`. Perfetto's trace processor
    /// transparently decompresses these and merges them back into the stream.
    fn flush_batch(&mut self) {
        if self.pending.is_empty() || self.write_error.is_some() {
            return;
        }

        let mut inner = Trace::new();
        inner.packet = std::mem::take(&mut self.pending);
        self.pending_size = 0;

        let inner_bytes = match inner.write_to_bytes() {
            Ok(b) => b,
            Err(e) => {
                self.write_error = Some(e);
                return;
            }
        };

        let mut encoder = ZlibEncoder::new(Vec::with_capacity(inner_bytes.len()), Compression::fast());
        if let Err(e) = encoder.write_all(&inner_bytes) {
            self.write_error = Some(e.into());
            return;
        }
        let blob = match encoder.finish() {
            Ok(b) => b,
            Err(e) => {
                self.write_error = Some(e.into());
                return;
            }
        };

        let mut outer = TracePacket::new();
        outer.set_compressed_packets(blob);
        self.write_packet_raw(outer);
    }

    fn alloc_uuid(&mut self) -> u64 {
        let uuid = self.next_uuid;
        self.next_uuid += 1;
        uuid
    }

    fn make_packet(&mut self) -> TracePacket {
        let mut pkt = TracePacket::new();
        pkt.set_trusted_packet_sequence_id(SEQUENCE_ID);
        if !self.first_event_emitted {
            pkt.sequence_flags = Some(1); // SEQ_INCREMENTAL_STATE_CLEARED
            self.first_event_emitted = true;
        } else {
            pkt.sequence_flags = Some(2); // SEQ_NEEDS_INCREMENTAL_STATE
        }
        pkt
    }

    fn make_event_packet(&mut self, ts_ns: u64) -> TracePacket {
        let mut pkt = self.make_packet();
        pkt.timestamp = Some(ts_ns);
        pkt.timestamp_clock_id = Some(CLOCK_ID_UNIXTIME);
        pkt
    }

    fn add_process_track(&mut self, uuid: u64, name: &str) {
        let mut pkt = self.make_packet();
        let mut td = TrackDescriptor::new();
        td.uuid = Some(uuid);
        td.static_or_dynamic_name = Some(Static_or_dynamic_name::Name(name.to_string()));
        pkt.data = Some(Data::TrackDescriptor(td));
        self.write_packet(pkt);
    }

    fn add_child_track(&mut self, uuid: u64, parent_uuid: u64, name: &str) {
        let mut pkt = self.make_packet();
        let mut td = TrackDescriptor::new();
        td.uuid = Some(uuid);
        td.parent_uuid = Some(parent_uuid);
        td.static_or_dynamic_name = Some(Static_or_dynamic_name::Name(name.to_string()));
        pkt.data = Some(Data::TrackDescriptor(td));
        self.write_packet(pkt);
    }

    fn add_counter_track(&mut self, uuid: u64, parent_uuid: u64, name: &str, unit: Unit) {
        let mut pkt = self.make_packet();
        let mut td = TrackDescriptor::new();
        td.uuid = Some(uuid);
        td.parent_uuid = Some(parent_uuid);
        td.static_or_dynamic_name = Some(Static_or_dynamic_name::Name(name.to_string()));
        let mut cd = CounterDescriptor::new();
        cd.unit = Some(EnumOrUnknown::new(unit));
        td.counter = MessageField::some(cd);
        pkt.data = Some(Data::TrackDescriptor(td));
        self.write_packet(pkt);
    }

    fn add_slice_begin(
        &mut self,
        track_uuid: u64,
        name: &str,
        ts_ns: u64,
        annotations: Vec<DebugAnnotation>,
    ) {
        let mut pkt = self.make_event_packet(ts_ns);
        let mut te = TrackEvent::new();
        te.type_ = Some(EnumOrUnknown::new(Type::TYPE_SLICE_BEGIN));
        te.track_uuid = Some(track_uuid);
        te.name_field = Some(Name_field::Name(name.to_string()));
        te.debug_annotations = annotations;
        pkt.data = Some(Data::TrackEvent(te));
        self.write_packet(pkt);
    }

    fn add_slice_end(&mut self, track_uuid: u64, ts_ns: u64) {
        let mut pkt = self.make_event_packet(ts_ns);
        let mut te = TrackEvent::new();
        te.type_ = Some(EnumOrUnknown::new(Type::TYPE_SLICE_END));
        te.track_uuid = Some(track_uuid);
        pkt.data = Some(Data::TrackEvent(te));
        self.write_packet(pkt);
    }

    fn add_instant(
        &mut self,
        track_uuid: u64,
        name: &str,
        ts_ns: u64,
        annotations: Vec<DebugAnnotation>,
    ) {
        let mut pkt = self.make_event_packet(ts_ns);
        let mut te = TrackEvent::new();
        te.type_ = Some(EnumOrUnknown::new(Type::TYPE_INSTANT));
        te.track_uuid = Some(track_uuid);
        te.name_field = Some(Name_field::Name(name.to_string()));
        te.debug_annotations = annotations;
        pkt.data = Some(Data::TrackEvent(te));
        self.write_packet(pkt);
    }

    fn add_counter_value(&mut self, track_uuid: u64, ts_ns: u64, value: i64) {
        let mut pkt = self.make_event_packet(ts_ns);
        let mut te = TrackEvent::new();
        te.type_ = Some(EnumOrUnknown::new(Type::TYPE_COUNTER));
        te.track_uuid = Some(track_uuid);
        te.counter_value_field = Some(Counter_value_field::CounterValue(value));
        pkt.data = Some(Data::TrackEvent(te));
        self.write_packet(pkt);
    }

    /// Returns (unit, scale_factor) for a ProfileEvent name.
    /// Scale factor converts the raw value to the unit's base
    /// (e.g. microseconds × 1000 → nanoseconds for UNIT_TIME_NS).
    fn unit_for_event(name: &str) -> (Unit, i64) {
        if name.ends_with("Bytes") {
            (Unit::UNIT_SIZE_BYTES, 1)
        } else if name.ends_with("Microseconds") {
            (Unit::UNIT_TIME_NS, 1000)
        } else if name.ends_with("Milliseconds") {
            (Unit::UNIT_TIME_NS, 1_000_000)
        } else if name.ends_with("Nanoseconds") {
            (Unit::UNIT_TIME_NS, 1)
        } else {
            (Unit::UNIT_UNSPECIFIED, 1)
        }
    }

    fn make_annotation_str(name: &str, value: &str) -> DebugAnnotation {
        let mut ann = DebugAnnotation::new();
        ann.name_field = Some(da::Name_field::Name(name.to_string()));
        ann.value = Some(da::Value::StringValue(value.to_string()));
        ann
    }

    fn make_annotation_int(name: &str, value: i64) -> DebugAnnotation {
        let mut ann = DebugAnnotation::new();
        ann.name_field = Some(da::Name_field::Name(name.to_string()));
        ann.value = Some(da::Value::IntValue(value));
        ann
    }

    fn datetime_to_ns(dt: &DateTime<Local>) -> Option<u64> {
        dt.timestamp_nanos_opt().map(|ns| ns as u64)
    }

    fn log_level_to_prio(level: &str) -> AndroidLogPriority {
        match level {
            "Fatal" | "Critical" => AndroidLogPriority::PRIO_FATAL,
            "Error" => AndroidLogPriority::PRIO_ERROR,
            "Warning" => AndroidLogPriority::PRIO_WARN,
            "Information" => AndroidLogPriority::PRIO_INFO,
            "Debug" => AndroidLogPriority::PRIO_DEBUG,
            _ => AndroidLogPriority::PRIO_VERBOSE,
        }
    }

    // --- High-level methods ---

    pub fn add_queries(&mut self, queries: &[Query]) {
        for q in queries {
            let host_uuid = self.get_or_create_host_uuid(&q.host_name);
            let user_uuid = self.child_track_uuid(host_uuid, &q.user);

            let start_ns = match Self::datetime_to_ns(&q.query_start_time_microseconds) {
                Some(ns) => ns,
                None => {
                    log::warn!("Perfetto: query {} has invalid start time", q.query_id);
                    continue;
                }
            };
            let end_ns = match Self::datetime_to_ns(&q.query_end_time_microseconds) {
                Some(ns) => ns,
                None => {
                    log::warn!("Perfetto: query {} has invalid end time", q.query_id);
                    continue;
                }
            };

            let label = if q.normalized_query.chars().count() > 80 {
                let truncated: String = q.normalized_query.chars().take(80).collect();
                format!("{}...", truncated)
            } else {
                q.normalized_query.clone()
            };

            let mut annotations = vec![
                Self::make_annotation_str("query_id", &q.query_id),
                Self::make_annotation_str("initial_query_id", &q.initial_query_id),
                Self::make_annotation_str("user", &q.user),
                Self::make_annotation_str("database", &q.current_database),
                Self::make_annotation_int("memory", q.memory),
                Self::make_annotation_int("threads", q.threads as i64),
            ];
            if !q.original_query.is_empty() {
                annotations.push(Self::make_annotation_str("query", &q.original_query));
            }

            self.add_slice_begin(user_uuid, &label, start_ns, annotations);
            self.add_slice_end(user_uuid, end_ns);
        }
    }

    fn get_or_create_host_uuid(&mut self, host_name: &str) -> u64 {
        if let Some(&uuid) = self.host_uuids.get(host_name) {
            return uuid;
        }
        let uuid = self.alloc_uuid();
        self.add_process_track(uuid, host_name);
        self.host_uuids.insert(host_name.to_string(), uuid);
        uuid
    }

    /// Synthetic pid identifying `host_name` in the trace's legacy process
    /// table, so AndroidLogPacket events (which key processes by pid, not
    /// TrackEvent uuid) can show the hostname as their process name.
    fn get_or_create_host_pid(&mut self, host_name: &str) -> i32 {
        if let Some(&pid) = self.host_pids.get(host_name) {
            return pid;
        }
        let pid = 100_000 + self.host_pids.len() as i32;
        let mut pkt = self.make_packet();
        let mut process = PtProcess::new();
        process.pid = Some(pid);
        process.cmdline.push(host_name.to_string());
        let mut pt = ProcessTree::new();
        pt.processes.push(process);
        pkt.data = Some(Data::ProcessTree(pt));
        self.write_packet(pkt);
        self.host_pids.insert(host_name.to_string(), pid);
        pid
    }

    fn get_host_category_track(&mut self, host_name: &str, category: &'static str) -> Option<u64> {
        if !self.per_server || host_name.is_empty() {
            return None;
        }
        let host_uuid = self.get_or_create_host_uuid(host_name);
        let key = (host_name.to_string(), category);
        if let Some(&uuid) = self.host_category_uuids.get(&key) {
            Some(uuid)
        } else {
            let uuid = self.alloc_uuid();
            self.add_child_track(uuid, host_uuid, category);
            self.host_category_uuids.insert(key, uuid);
            Some(uuid)
        }
    }

    fn process_track_uuid(&mut self, name: &str) -> u64 {
        self.child_track_uuid(0, name)
    }

    fn child_track_uuid(&mut self, parent_uuid: u64, name: &str) -> u64 {
        if let Some(&uuid) = self.track_uuids.get(&(parent_uuid, name.to_string())) {
            return uuid;
        }
        let uuid = self.alloc_uuid();
        if parent_uuid == 0 {
            self.add_process_track(uuid, name);
        } else {
            self.add_child_track(uuid, parent_uuid, name);
        }
        self.track_uuids
            .insert((parent_uuid, name.to_string()), uuid);
        uuid
    }

    fn counter_track_uuid(&mut self, parent_uuid: u64, name: &str, unit: Unit) -> u64 {
        if let Some(&uuid) = self.track_uuids.get(&(parent_uuid, name.to_string())) {
            return uuid;
        }
        let uuid = self.alloc_uuid();
        self.add_counter_track(uuid, parent_uuid, name, unit);
        self.track_uuids
            .insert((parent_uuid, name.to_string()), uuid);
        uuid
    }

    fn add_counter_increment(&mut self, track_uuid: u64, ts_ns: u64, increment: i64) {
        let total = self.counter_totals.entry(track_uuid).or_default();
        *total += increment;
        let value = *total;
        self.add_counter_value(track_uuid, ts_ns, value);
    }

    pub fn add_otel_spans<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        // Group spans by operation_name → thread track under a single process track
        let process_uuid = self.process_track_uuid("OpenTelemetry Spans");

        for i in 0..columns.row_count() {
            let operation_name: String = columns.get(i, "operation_name").unwrap_or_default();
            let start_us: u64 = match columns.get(i, "start_time_us") {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("Perfetto: otel_span row {} start_time_us: {}", i, e);
                    continue;
                }
            };
            let finish_us: u64 = match columns.get(i, "finish_time_us") {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("Perfetto: otel_span row {} finish_time_us: {}", i, e);
                    continue;
                }
            };
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let host_name: String = columns.get(i, "host_name").unwrap_or_default();

            let start_ns = start_us.saturating_mul(1000);
            let end_ns = finish_us.saturating_mul(1000);

            let track_uuid =
                self.child_track_uuid(process_uuid, &format!("Processor: {}", operation_name));

            let annotations = vec![Self::make_annotation_str("query_id", &query_id)];

            self.add_slice_begin(track_uuid, &operation_name, start_ns, annotations.clone());
            self.add_slice_end(track_uuid, end_ns);

            if let Some(cat_uuid) = self.get_host_category_track(&host_name, "OpenTelemetry Spans")
            {
                let server_track = self.child_track_uuid(cat_uuid, &operation_name);
                self.add_slice_begin(server_track, &operation_name, start_ns, annotations);
                self.add_slice_end(server_track, end_ns);
            }
        }
    }

    pub fn add_trace_log_counters<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("ProfileEvent Counters");

        for i in 0..columns.row_count() {
            let event: String = columns.get(i, "event").unwrap_or_default();
            let increment: i64 = columns.get(i, "increment").unwrap_or(0);
            let host_name: String = columns.get(i, "host_name").unwrap_or_default();
            let timestamp_ns: u64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                    Err(e) => {
                        log::warn!(
                            "Perfetto: trace_log row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };

            let (unit, scale) = Self::unit_for_event(&event);
            let scaled_increment = increment * scale;
            let track_uuid = self.counter_track_uuid(process_uuid, &event, unit);
            self.add_counter_increment(track_uuid, timestamp_ns, scaled_increment);

            if let Some(cat_uuid) =
                self.get_host_category_track(&host_name, "ProfileEvent Counters")
            {
                let track_uuid = self.counter_track_uuid(cat_uuid, &event, unit);
                self.add_counter_increment(track_uuid, timestamp_ns, scaled_increment);
            }
        }
    }

    pub fn add_query_metrics(&mut self, rows: &[QueryMetricRow]) {
        if rows.is_empty() {
            return;
        }

        let process_uuid = self.process_track_uuid("Query Metrics");

        for row in rows {
            // memory_usage / peak_memory_usage
            for (name, value, unit) in [
                ("memory_usage", row.memory_usage, Unit::UNIT_SIZE_BYTES),
                (
                    "peak_memory_usage",
                    row.peak_memory_usage,
                    Unit::UNIT_SIZE_BYTES,
                ),
            ] {
                let track_uuid = self.counter_track_uuid(process_uuid, name, unit);
                self.add_counter_value(track_uuid, row.timestamp_ns, value);

                if let Some(cat_uuid) =
                    self.get_host_category_track(&row.host_name, "Query Metrics")
                {
                    let server_track = self.counter_track_uuid(cat_uuid, name, unit);
                    self.add_counter_value(server_track, row.timestamp_ns, value);
                }
            }

            // ProfileEvent_* metrics
            for (name, value) in &row.profile_events {
                let (unit, scale) = Self::unit_for_event(name);
                let scaled_value = *value as i64 * scale;
                let track_uuid = self.counter_track_uuid(process_uuid, name, unit);
                self.add_counter_value(track_uuid, row.timestamp_ns, scaled_value);

                if let Some(cat_uuid) =
                    self.get_host_category_track(&row.host_name, "Query Metrics")
                {
                    let server_track = self.counter_track_uuid(cat_uuid, name, unit);
                    self.add_counter_value(server_track, row.timestamp_ns, scaled_value);
                }
            }
        }
    }

    pub fn add_part_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Part Log");

        for i in 0..columns.row_count() {
            let event_type: String = column_as_string(columns, i, "event_type").unwrap_or_default();
            let event_time: DateTime<Tz> = match columns.get(i, "event_time_microseconds") {
                Ok(v) => v,
                Err(e) => {
                    log::warn!(
                        "Perfetto: part_log row {} event_time_microseconds: {}",
                        i,
                        e
                    );
                    continue;
                }
            };
            let duration_ms: u64 = columns.get(i, "duration_ms").unwrap_or(0);
            let database: String = columns.get(i, "database").unwrap_or_default();
            let table: String = columns.get(i, "table").unwrap_or_default();
            let part_name: String = columns.get(i, "part_name").unwrap_or_default();
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let rows: u64 = columns.get(i, "rows").unwrap_or(0);
            let size_in_bytes: u64 = columns.get(i, "size_in_bytes").unwrap_or(0);
            let host_name: String = columns.get(i, "host_name").unwrap_or_default();

            let table_key = format!("{}.{}", database, table);
            let track_uuid = self.child_track_uuid(process_uuid, &table_key);

            let end_ns = match event_time.with_timezone(&Local).timestamp_nanos_opt() {
                Some(ns) => ns as u64,
                None => {
                    log::warn!("Perfetto: part_log row {} timestamp overflow", i);
                    continue;
                }
            };
            let start_ns = end_ns.saturating_sub(duration_ms * 1_000_000);

            let label = format!("{} {}", event_type, part_name);
            let annotations = vec![
                Self::make_annotation_str("query_id", &query_id),
                Self::make_annotation_str("part_name", &part_name),
                Self::make_annotation_int("rows", rows as i64),
                Self::make_annotation_int("size_in_bytes", size_in_bytes as i64),
            ];

            self.add_slice_begin(track_uuid, &label, start_ns, annotations.clone());
            self.add_slice_end(track_uuid, end_ns);

            if let Some(cat_uuid) = self.get_host_category_track(&host_name, "Part Log") {
                let server_track = self.child_track_uuid(cat_uuid, &table_key);
                self.add_slice_begin(server_track, &label, start_ns, annotations);
                self.add_slice_end(server_track, end_ns);
            }
        }
    }

    pub fn add_query_thread_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Query Threads");

        for i in 0..columns.row_count() {
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let thread_name: String = columns.get(i, "thread_name").unwrap_or_default();
            let host_name: String = columns.get(i, "host_name").unwrap_or_default();
            let timestamp_ns: u64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                    Err(e) => {
                        log::warn!(
                            "Perfetto: query_thread_log row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };
            let duration_ms: u64 = columns.get(i, "query_duration_ms").unwrap_or(0);
            let peak_memory: i64 = columns.get(i, "peak_memory_usage").unwrap_or(0);

            let names: Vec<String> = columns.get(i, "profile_event_names").unwrap_or_default();
            let values: Vec<u64> = columns.get(i, "profile_event_values").unwrap_or_default();

            let track_uuid = self.child_track_uuid(process_uuid, &thread_name);

            let end_ns = timestamp_ns;
            let start_ns = end_ns.saturating_sub(duration_ms * 1_000_000);

            let mut annotations = vec![
                Self::make_annotation_str("query_id", &query_id),
                Self::make_annotation_str("thread_name", &thread_name),
                Self::make_annotation_int("peak_memory_usage", peak_memory),
            ];

            // Add top ProfileEvents as annotations
            let mut pe: Vec<(String, u64)> = names.into_iter().zip(values).collect();
            pe.sort_by(|a, b| b.1.cmp(&a.1));
            for (name, value) in pe.iter().take(10) {
                if *value > 0 {
                    annotations.push(Self::make_annotation_int(name, *value as i64));
                }
            }

            self.add_slice_begin(track_uuid, &query_id, start_ns, annotations.clone());
            self.add_slice_end(track_uuid, end_ns);

            if let Some(cat_uuid) = self.get_host_category_track(&host_name, "Query Threads") {
                let server_track = self.child_track_uuid(cat_uuid, &thread_name);
                self.add_slice_begin(server_track, &query_id, start_ns, annotations);
                self.add_slice_end(server_track, end_ns);
            }
        }
    }

    pub fn add_text_logs<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Query Logs");

        let mut alp = if self.text_log_android {
            Some(AndroidLogPacket::new())
        } else {
            None
        };

        for i in 0..columns.row_count() {
            let level: String = column_as_string(columns, i, "level").unwrap_or_default();
            let logger_name: String = columns.get(i, "logger_name").unwrap_or_default();
            let message: String = columns.get(i, "message").unwrap_or_default();
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let host_name: String = columns.get(i, "host_name").unwrap_or_default();
            let thread_id: u64 = columns.get(i, "thread_id").unwrap_or(0);
            let timestamp_ns: u64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                    Err(e) => {
                        log::warn!(
                            "Perfetto: text_log row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };

            let track_uuid = self.child_track_uuid(process_uuid, &level);

            let annotations = vec![
                Self::make_annotation_str("query_id", &query_id),
                Self::make_annotation_str("level", &level),
                Self::make_annotation_str("logger", &logger_name),
            ];

            self.add_instant(track_uuid, &message, timestamp_ns, annotations.clone());

            if let Some(cat_uuid) = self.get_host_category_track(&host_name, "Query Logs") {
                let server_track = self.child_track_uuid(cat_uuid, &level);
                self.add_instant(server_track, &message, timestamp_ns, annotations);
            }

            if let Some(ref mut alp) = alp {
                let pid = if !host_name.is_empty() {
                    Some(self.get_or_create_host_pid(&host_name))
                } else {
                    None
                };
                let mut event = LogEvent::new();
                event.timestamp = Some(timestamp_ns);
                event.tag = Some(logger_name);
                event.message = Some(message);
                event.prio = Some(EnumOrUnknown::new(Self::log_level_to_prio(&level)));
                event.pid = pid;
                event.tid = Some(thread_id as i32);
                alp.events.push(event);
            }
        }

        if let Some(alp) = alp.filter(|a| !a.events.is_empty()) {
            let first_ts = alp.events[0].timestamp.unwrap_or(0);
            let mut pkt = self.make_event_packet(first_ts);
            pkt.data = Some(Data::AndroidLog(alp));
            self.write_packet(pkt);
        }
    }

    pub fn add_metric_log(&mut self, rows: &[MetricLogRow]) {
        if rows.is_empty() {
            return;
        }

        let process_uuid = self.process_track_uuid("Metric Log");

        for row in rows {
            for (name, value) in &row.profile_events {
                let (unit, scale) = Self::unit_for_event(name);
                let scaled = *value as i64 * scale;
                let track_uuid = self.counter_track_uuid(process_uuid, name, unit);
                self.add_counter_increment(track_uuid, row.timestamp_ns, scaled);
            }

            for (name, value) in &row.current_metrics {
                let (unit, scale) = Self::unit_for_event(name);
                let track_uuid = self.counter_track_uuid(process_uuid, name, unit);
                self.add_counter_value(track_uuid, row.timestamp_ns, *value * scale);
            }
        }
    }

    pub fn add_asynchronous_metric_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Async Metrics");

        for i in 0..columns.row_count() {
            let metric: String = columns.get(i, "metric").unwrap_or_default();
            let value: f64 = columns.get(i, "value").unwrap_or(0.0);
            let timestamp_ns: u64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                    Err(e) => {
                        log::warn!(
                            "Perfetto: asynchronous_metric_log row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };

            let track_uuid = self.counter_track_uuid(process_uuid, &metric, Unit::UNIT_UNSPECIFIED);

            self.add_counter_value(track_uuid, timestamp_ns, value as i64);
        }
    }

    pub fn add_asynchronous_insert_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Async Inserts");

        for i in 0..columns.row_count() {
            let database: String = columns.get(i, "database").unwrap_or_default();
            let table: String = columns.get(i, "table").unwrap_or_default();
            let format: String = columns.get(i, "format").unwrap_or_default();
            let status: String = column_as_string(columns, i, "status").unwrap_or_default();
            let bytes: u64 = columns.get(i, "bytes").unwrap_or(0);
            let exception: String = columns.get(i, "exception").unwrap_or_default();
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();

            let start_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(e) => {
                    log::warn!(
                        "Perfetto: asynchronous_insert_log row {} event_time_microseconds: {}",
                        i,
                        e
                    );
                    continue;
                }
            };
            let end_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "flush_time_microseconds") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(_) => start_ns,
            };

            let table_key = format!("{}.{}", database, table);
            let track_uuid = self.child_track_uuid(process_uuid, &table_key);

            let label = format!("{} ({})", table_key, status);
            let mut annotations = vec![
                Self::make_annotation_str("query_id", &query_id),
                Self::make_annotation_str("format", &format),
                Self::make_annotation_str("status", &status),
                Self::make_annotation_int("bytes", bytes as i64),
            ];
            if !exception.is_empty() {
                annotations.push(Self::make_annotation_str("exception", &exception));
            }

            self.add_slice_begin(track_uuid, &label, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn add_error_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Error Log");

        for i in 0..columns.row_count() {
            let error: String = columns.get(i, "error").unwrap_or_default();
            let code: i64 = columns.get(i, "code").unwrap_or(0);
            let value: u64 = columns.get(i, "value").unwrap_or(0);
            let remote: u8 = columns.get(i, "remote").unwrap_or(0);
            let last_error_message: String =
                columns.get(i, "last_error_message").unwrap_or_default();
            let timestamp_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "event_time") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(e) => {
                    log::warn!("Perfetto: error_log row {} event_time: {}", i, e);
                    continue;
                }
            };

            let track_uuid = self.child_track_uuid(process_uuid, &error);

            let mut annotations = vec![
                Self::make_annotation_int("code", code),
                Self::make_annotation_int("value", value as i64),
                Self::make_annotation_int("remote", remote as i64),
            ];
            if !last_error_message.is_empty() {
                annotations.push(Self::make_annotation_str(
                    "last_error_message",
                    &last_error_message,
                ));
            }

            self.add_instant(track_uuid, &error, timestamp_ns, annotations);
        }
    }

    pub fn add_s3_queue_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("S3 Queue");
        let track_uuid = self.child_track_uuid(process_uuid, "files");

        for i in 0..columns.row_count() {
            let file_name: String = columns.get(i, "file_name").unwrap_or_default();
            let rows_processed: u64 = columns.get(i, "rows_processed").unwrap_or(0);
            let status: String = column_as_string(columns, i, "status").unwrap_or_default();
            let exception: String = columns.get(i, "exception").unwrap_or_default();

            let start_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "processing_start_time") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(_) => continue,
            };
            let end_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "processing_end_time") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(_) => start_ns,
            };

            let mut annotations = vec![
                Self::make_annotation_str("file_name", &file_name),
                Self::make_annotation_int("rows_processed", rows_processed as i64),
                Self::make_annotation_str("status", &status),
            ];
            if !exception.is_empty() {
                annotations.push(Self::make_annotation_str("exception", &exception));
            }

            self.add_slice_begin(track_uuid, &file_name, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn add_azure_queue_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Azure Queue");

        for i in 0..columns.row_count() {
            let database: String = columns.get(i, "database").unwrap_or_default();
            let table: String = columns.get(i, "table").unwrap_or_default();
            let file_name: String = columns.get(i, "file_name").unwrap_or_default();
            let rows_processed: u64 = columns.get(i, "rows_processed").unwrap_or(0);
            let status: String = column_as_string(columns, i, "status").unwrap_or_default();
            let exception: String = columns.get(i, "exception").unwrap_or_default();

            let start_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "processing_start_time") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(_) => continue,
            };
            let end_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "processing_end_time") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(_) => start_ns,
            };

            let table_key = format!("{}.{}", database, table);
            let track_uuid = self.child_track_uuid(process_uuid, &table_key);

            let mut annotations = vec![
                Self::make_annotation_str("file_name", &file_name),
                Self::make_annotation_int("rows_processed", rows_processed as i64),
                Self::make_annotation_str("status", &status),
            ];
            if !exception.is_empty() {
                annotations.push(Self::make_annotation_str("exception", &exception));
            }

            self.add_slice_begin(track_uuid, &file_name, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn add_blob_storage_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Blob Storage");

        for i in 0..columns.row_count() {
            let event_type: String = column_as_string(columns, i, "event_type").unwrap_or_default();
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let disk_name: String = columns.get(i, "disk_name").unwrap_or_default();
            let bucket: String = columns.get(i, "bucket").unwrap_or_default();
            let remote_path: String = columns.get(i, "remote_path").unwrap_or_default();
            let data_size: u64 = columns.get(i, "data_size").unwrap_or(0);
            let error: String = columns.get(i, "error").unwrap_or_default();
            let timestamp_ns: u64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                    Err(e) => {
                        log::warn!(
                            "Perfetto: blob_storage_log row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };

            let track_uuid = self.child_track_uuid(process_uuid, &event_type);

            let mut annotations = vec![
                Self::make_annotation_str("query_id", &query_id),
                Self::make_annotation_str("disk_name", &disk_name),
                Self::make_annotation_str("bucket", &bucket),
                Self::make_annotation_str("remote_path", &remote_path),
                Self::make_annotation_int("data_size", data_size as i64),
            ];
            if !error.is_empty() {
                annotations.push(Self::make_annotation_str("error", &error));
            }

            self.add_instant(track_uuid, &event_type, timestamp_ns, annotations);
        }
    }

    pub fn add_background_pool_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Background Pool");

        for i in 0..columns.row_count() {
            let log_name: String = columns.get(i, "log_name").unwrap_or_default();
            let database: String = columns.get(i, "database").unwrap_or_default();
            let table: String = columns.get(i, "table").unwrap_or_default();
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let duration_ms: u64 = columns.get(i, "duration_ms").unwrap_or(0);
            let error: String = columns.get(i, "error").unwrap_or_default();
            let exception: String = columns.get(i, "exception").unwrap_or_default();
            let end_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(e) => {
                    log::warn!(
                        "Perfetto: background_schedule_pool_log row {} event_time_microseconds: {}",
                        i,
                        e
                    );
                    continue;
                }
            };
            let start_ns = end_ns.saturating_sub(duration_ms * 1_000_000);

            let track_uuid = self.child_track_uuid(process_uuid, &log_name);

            let mut annotations = vec![
                Self::make_annotation_str("database", &database),
                Self::make_annotation_str("table", &table),
                Self::make_annotation_str("query_id", &query_id),
            ];
            if !error.is_empty() {
                annotations.push(Self::make_annotation_str("error", &error));
            }
            if !exception.is_empty() {
                annotations.push(Self::make_annotation_str("exception", &exception));
            }

            let label = format!("{}.{}", database, table);
            self.add_slice_begin(track_uuid, &label, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn add_session_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("Sessions");

        for i in 0..columns.row_count() {
            let session_type: String = column_as_string(columns, i, "type").unwrap_or_default();
            let user: String = columns.get(i, "user").unwrap_or_default();
            let auth_type: String = columns.get(i, "auth_type").unwrap_or_default();
            let interface: String = column_as_string(columns, i, "interface").unwrap_or_default();
            let client_address: String = columns.get(i, "client_address").unwrap_or_default();
            let client_name: String = columns.get(i, "client_name").unwrap_or_default();
            let failure_reason: String = columns.get(i, "failure_reason").unwrap_or_default();
            let timestamp_ns: u64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                    Err(e) => {
                        log::warn!(
                            "Perfetto: session_log row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };

            let track_uuid = self.child_track_uuid(process_uuid, &session_type);

            let mut annotations = vec![
                Self::make_annotation_str("user", &user),
                Self::make_annotation_str("auth_type", &auth_type),
                Self::make_annotation_str("interface", &interface),
                Self::make_annotation_str("client_address", &client_address),
                Self::make_annotation_str("client_name", &client_name),
            ];
            if !failure_reason.is_empty() {
                annotations.push(Self::make_annotation_str("failure_reason", &failure_reason));
            }

            let label = format!("{} ({})", session_type, user);
            self.add_instant(track_uuid, &label, timestamp_ns, annotations);
        }
    }

    pub fn add_aggregated_zookeeper_log<K: ColumnType>(&mut self, columns: &Block<K>) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.process_track_uuid("ZooKeeper");

        for i in 0..columns.row_count() {
            let operation: String = column_as_string(columns, i, "operation").unwrap_or_default();
            let count: u64 = columns.get(i, "count").unwrap_or(0);
            let average_latency: f64 = columns.get(i, "average_latency").unwrap_or(0.0);
            let parent_path: String = columns.get(i, "parent_path").unwrap_or_default();
            let component: String = columns.get(i, "component").unwrap_or_default();

            let timestamp_ns: u64 = match columns.get::<DateTime<Tz>, _>(i, "event_time") {
                Ok(dt) => dt.with_timezone(&Local).timestamp_nanos_opt().unwrap_or(0) as u64,
                Err(e) => {
                    log::warn!(
                        "Perfetto: aggregated_zookeeper_log row {} event_time: {}",
                        i,
                        e
                    );
                    continue;
                }
            };

            let count_track = self.counter_track_uuid(
                process_uuid,
                &format!("{} count", operation),
                Unit::UNIT_UNSPECIFIED,
            );
            let latency_track = self.counter_track_uuid(
                process_uuid,
                &format!("{} avg_latency", operation),
                Unit::UNIT_UNSPECIFIED,
            );

            self.add_counter_value(count_track, timestamp_ns, count as i64);
            self.add_counter_value(latency_track, timestamp_ns, average_latency as i64);

            // Also emit an instant with annotations for the detail
            if !parent_path.is_empty() || !component.is_empty() {
                let error_names: Vec<String> = columns.get(i, "error_names").unwrap_or_default();
                let error_counts: Vec<u32> = columns.get(i, "error_counts").unwrap_or_default();

                let mut annotations = vec![
                    Self::make_annotation_str("parent_path", &parent_path),
                    Self::make_annotation_str("component", &component),
                    Self::make_annotation_int("count", count as i64),
                ];
                for (en, ec) in error_names.iter().zip(error_counts.iter()) {
                    annotations.push(Self::make_annotation_int(en, *ec as i64));
                }

                // Use count_track for the instant
                self.add_instant(count_track, &operation, timestamp_ns, annotations);
            }
        }
    }

    fn alloc_intern_id(&mut self) -> u64 {
        let id = self.next_intern_id;
        self.next_intern_id += 1;
        id
    }

    // Add CPU/Real/Memory stack trace samples as StreamingProfilePacket.
    //
    // Perfetto profiling timeline pitfalls (hard-won lessons):
    // - Clock 128 is sequence-scoped: a ClockSnapshot on seq 1 does NOT help seq 2+.
    // - Built-in clocks (e.g. BOOTTIME=6) also fail on non-main sequences in practice.
    // - SEQ_INCREMENTAL_STATE_CLEARED nukes clock mappings on the sequence — never
    //   use it on the main sequence after the ClockSnapshot.
    // - StreamingProfilePacket timestamps come from ThreadDescriptor.reference_timestamp_us
    //   + timestamp_delta_us, NOT from TracePacket.timestamp. If reference_timestamp_us
    //   is unset, all samples land at time 0.
    // - Samples go into cpu_profile_stack_sample table, not perf_sample.
    //
    // The working approach: each trace type gets its own sequence with a ThreadDescriptor
    // that carries reference_timestamp_us (microseconds). No clock_id needed on the
    // packets — timing is entirely from reference_timestamp_us + deltas.
    fn stack_mapping_iid(&mut self) -> u64 {
        if let Some(iid) = self.stack_mapping_iid {
            return iid;
        }
        let iid = self.alloc_intern_id();
        self.stack_mapping_iid = Some(iid);
        iid
    }

    /// Intern each unique stack once; samples reference them by
    /// (host_name, stack_hash), see stack_traces_for_perfetto_sql().
    /// Must be fed before add_stack_samples().
    pub fn add_stack_frames<K: ColumnType>(&mut self, stacks: &Block<K>) {
        let mapping_iid = self.stack_mapping_iid();

        for i in 0..stacks.row_count() {
            let host_name: String = stacks.get(i, "host_name").unwrap_or_default();
            let stack_hash: u64 = stacks.get(i, "stack_hash").unwrap_or_default();
            let stack: Vec<String> = stacks.get(i, "stack").unwrap_or_default();

            if stack.is_empty() {
                continue;
            }

            // Intern each frame in the stack
            let mut frame_ids = Vec::with_capacity(stack.len());
            for func_name in &stack {
                let func_iid = *self
                    .function_name_iids
                    .entry(func_name.clone())
                    .or_insert_with(|| {
                        let iid = self.next_intern_id;
                        self.next_intern_id += 1;
                        let mut is = InternedString::new();
                        is.iid = Some(iid);
                        is.str = Some(func_name.as_bytes().to_vec());
                        self.stack_interned_strings.push(is);
                        iid
                    });

                let frame_key = (func_iid, mapping_iid);
                let frame_iid = *self.frame_iids.entry(frame_key).or_insert_with(|| {
                    let iid = self.next_intern_id;
                    self.next_intern_id += 1;
                    let mut f = Frame::new();
                    f.iid = Some(iid);
                    f.function_name_id = Some(func_iid);
                    f.mapping_id = Some(mapping_iid);
                    self.stack_interned_frames.push(f);
                    iid
                });

                frame_ids.push(frame_iid);
            }

            let callstack_iid =
                *self
                    .callstack_iids
                    .entry(frame_ids.clone())
                    .or_insert_with(|| {
                        let iid = self.next_intern_id;
                        self.next_intern_id += 1;
                        let mut cs = Callstack::new();
                        cs.iid = Some(iid);
                        cs.frame_ids = frame_ids;
                        self.stack_interned_callstacks.push(cs);
                        iid
                    });

            self.stack_callstacks_by_hash
                .insert((host_name, stack_hash), callstack_iid);
        }
    }

    /// Accumulates samples (compact, 16B each); the profile sequences are
    /// emitted by finalize_stack_traces() from build().
    pub fn add_stack_samples<K: ColumnType>(&mut self, samples: &Block<K>) {
        for i in 0..samples.row_count() {
            let trace_type: String = column_as_string(samples, i, "trace_type").unwrap_or_default();
            let stack_hash: u64 = samples.get(i, "stack_hash").unwrap_or_default();
            let host_name: String = samples.get(i, "host_name").unwrap_or_default();

            let Some(&callstack_iid) = self
                .stack_callstacks_by_hash
                .get(&(host_name.clone(), stack_hash))
            else {
                continue;
            };

            let timestamp_us: i64 =
                match samples.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
                    Ok(dt) => dt.with_timezone(&Local).timestamp_micros(),
                    Err(e) => {
                        log::warn!(
                            "Perfetto: stack trace row {} event_time_microseconds: {}",
                            i,
                            e
                        );
                        continue;
                    }
                };

            self.stack_samples_by_type
                .entry(trace_type.clone())
                .or_default()
                .push(Sample {
                    callstack_iid,
                    timestamp_us,
                });

            if self.per_server && !host_name.is_empty() {
                self.stack_samples_by_host_type
                    .entry((host_name, trace_type))
                    .or_default()
                    .push(Sample {
                        callstack_iid,
                        timestamp_us,
                    });
            }
        }
    }

    fn finalize_stack_traces(&mut self) {
        let samples_by_type = std::mem::take(&mut self.stack_samples_by_type);
        let samples_by_host_type = std::mem::take(&mut self.stack_samples_by_host_type);
        if samples_by_type.is_empty() && samples_by_host_type.is_empty() {
            return;
        }
        let interned_strings = std::mem::take(&mut self.stack_interned_strings);
        let interned_frames = std::mem::take(&mut self.stack_interned_frames);
        let interned_callstacks = std::mem::take(&mut self.stack_interned_callstacks);

        // Build one dummy mapping
        let mut mapping = Mapping::new();
        mapping.iid = Some(self.stack_mapping_iid());

        // Each trace_type gets its own sequence with a dedicated ThreadDescriptor.
        // Sample timestamps come from ThreadDescriptor.reference_timestamp_us + deltas,
        // so profiling packets don't need clock_id/timestamp (avoids sequence-scoped
        // clock 128 resolution issues on non-main sequences).
        for (trace_type, samples) in &samples_by_type {
            let name = format!("{} Samples", trace_type);
            self.emit_streaming_profile(
                &name,
                samples,
                &interned_strings,
                &interned_frames,
                &interned_callstacks,
                &mapping,
            );
        }

        for ((host, trace_type), samples) in &samples_by_host_type {
            let name = format!("{}: {} Samples", host, trace_type);
            self.emit_streaming_profile(
                &name,
                samples,
                &interned_strings,
                &interned_frames,
                &interned_callstacks,
                &mapping,
            );
        }
    }

    fn emit_streaming_profile(
        &mut self,
        thread_name: &str,
        samples: &[Sample],
        interned_strings: &[InternedString],
        interned_frames: &[Frame],
        interned_callstacks: &[Callstack],
        mapping: &Mapping,
    ) {
        if samples.is_empty() {
            return;
        }

        let seq_id = self.next_sequence_id;
        self.next_sequence_id += 1;
        let fake_tid = seq_id as i32;

        let mut td = PerfettoThreadDescriptor::new();
        td.pid = Some(1);
        td.tid = Some(fake_tid);
        td.thread_name = Some(thread_name.to_string());
        td.reference_timestamp_us = Some(samples[0].timestamp_us);

        let mut desc_pkt = TracePacket::new();
        desc_pkt.set_trusted_packet_sequence_id(seq_id);
        desc_pkt.sequence_flags = Some(1 | 2);
        desc_pkt.trusted_pid = Some(1);
        desc_pkt.data = Some(Data::ThreadDescriptor(td));
        self.write_packet(desc_pkt);

        let mut callstack_iids = Vec::with_capacity(samples.len());
        let mut timestamp_deltas = Vec::with_capacity(samples.len());

        let mut prev_us = samples[0].timestamp_us;
        for (idx, s) in samples.iter().enumerate() {
            callstack_iids.push(s.callstack_iid);
            if idx == 0 {
                timestamp_deltas.push(0);
            } else {
                timestamp_deltas.push(s.timestamp_us - prev_us);
                prev_us = s.timestamp_us;
            }
        }

        let mut spp = StreamingProfilePacket::new();
        spp.callstack_iid = callstack_iids;
        spp.timestamp_delta_us = timestamp_deltas;

        let mut interned_data = InternedData::new();
        interned_data.function_names = interned_strings.to_vec();
        interned_data.frames = interned_frames.to_vec();
        interned_data.callstacks = interned_callstacks.to_vec();
        interned_data.mappings = vec![mapping.clone()];

        let mut pkt = TracePacket::new();
        pkt.set_trusted_packet_sequence_id(seq_id);
        pkt.sequence_flags = Some(2);
        pkt.trusted_pid = Some(1);
        pkt.interned_data = MessageField::some(interned_data);
        pkt.data = Some(Data::StreamingProfilePacket(spp));
        self.write_packet(pkt);
    }

    /// Build a ClockSnapshot mapping all clocks with an identity transform.
    /// All at timestamp 0 with 1ns multiplier, so raw nanosecond values pass through as-is.
    /// Built-in clocks 1 (MONOTONIC), 3 (REALTIME), 6 (BOOTTIME) are needed because
    /// some packet types (e.g. AndroidLogPacket) have their timestamps resolved
    /// internally via built-in clocks.
    fn make_clock_snapshot() -> ClockSnapshot {
        let mut cs = ClockSnapshot::new();
        let make_clock = |id: u32| -> Clock {
            let mut c = Clock::new();
            c.clock_id = Some(id);
            c.timestamp = Some(0);
            c.unit_multiplier_ns = Some(1);
            c.is_incremental = Some(false);
            c
        };
        cs.clocks = vec![
            make_clock(CLOCK_ID_UNIXTIME), // 128 - sequence-scoped
            make_clock(1),                 // BUILTIN_CLOCK_MONOTONIC
            make_clock(3),                 // BUILTIN_CLOCK_REALTIME
            make_clock(6),                 // BUILTIN_CLOCK_BOOTTIME
        ];
        cs
    }

    pub fn build(mut self) -> Result<TraceFile> {
        self.finalize_stack_traces();
        self.flush_batch();
        if let Some(e) = self.write_error {
            return Err(e.into());
        }
        let file = self
            .out
            .into_inner()
            .map_err(|e| anyhow::anyhow!("flush failed: {}", e.error()))?;
        let size = file.metadata()?.len();
        Ok(TraceFile {
            file: Arc::new(file),
            size,
        })
    }
}

pub struct PerfettoServer {
    // Replacing the trace drops the previous TraceFile, releasing the last
    // fd of its anonymous temp file (in-flight requests hold their own Arc)
    trace_file: Arc<Mutex<Option<TraceFile>>>,
    #[allow(dead_code)]
    server_thread: Option<std::thread::JoinHandle<()>>,
}

impl PerfettoServer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let trace_file: Arc<Mutex<Option<TraceFile>>> = Arc::new(Mutex::new(None));
        let trace_file_clone = trace_file.clone();

        let server_thread = std::thread::spawn(move || {
            let server = match tiny_http::Server::http("127.0.0.1:9001") {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Failed to start Perfetto HTTP server on port 9001: {}", e);
                    return;
                }
            };
            log::info!("Perfetto HTTP server listening on port 9001");

            for request in server.incoming_requests() {
                let url = request.url().to_string();
                log::trace!("Perfetto HTTP request: {} {}", request.method(), url);

                if request.method() == &tiny_http::Method::Options {
                    let response = tiny_http::Response::empty(200)
                        .with_header(
                            "Access-Control-Allow-Origin: *"
                                .parse::<tiny_http::Header>()
                                .unwrap(),
                        )
                        .with_header(
                            "Access-Control-Allow-Methods: GET, POST, OPTIONS"
                                .parse::<tiny_http::Header>()
                                .unwrap(),
                        )
                        .with_header(
                            "Access-Control-Allow-Headers: *"
                                .parse::<tiny_http::Header>()
                                .unwrap(),
                        );
                    request.respond(response).ok();
                    continue;
                }

                if url == "/trace" {
                    // Fresh cursor per request: tiny_http streams the file, and
                    // concurrent requests must not share a position. The shared
                    // fd keeps the trace readable even if replaced mid-stream.
                    let trace = trace_file_clone
                        .lock()
                        .unwrap()
                        .as_ref()
                        .map(|t| (t.reader(), t.size()));
                    match trace {
                        Some((reader, len)) => {
                            let response = tiny_http::Response::new(
                                tiny_http::StatusCode(200),
                                Vec::new(),
                                reader,
                                Some(len as usize),
                                None,
                            )
                            .with_header(
                                "Content-Type: application/octet-stream"
                                    .parse::<tiny_http::Header>()
                                    .unwrap(),
                            )
                            .with_header(
                                "Access-Control-Allow-Origin: *"
                                    .parse::<tiny_http::Header>()
                                    .unwrap(),
                            );
                            request.respond(response).ok();
                        }
                        None => {
                            let response =
                                tiny_http::Response::from_string("No trace data available")
                                    .with_status_code(404)
                                    .with_header(
                                        "Access-Control-Allow-Origin: *"
                                            .parse::<tiny_http::Header>()
                                            .unwrap(),
                                    );
                            request.respond(response).ok();
                        }
                    }
                } else {
                    let response = tiny_http::Response::from_string("Not Found")
                        .with_status_code(404)
                        .with_header(
                            "Access-Control-Allow-Origin: *"
                                .parse::<tiny_http::Header>()
                                .unwrap(),
                        );
                    request.respond(response).ok();
                }
            }
        });

        PerfettoServer {
            trace_file,
            server_thread: Some(server_thread),
        }
    }

    pub fn set_trace_file(&self, file: TraceFile) {
        *self.trace_file.lock().unwrap() = Some(file);
    }

    pub fn get_perfetto_url(&self) -> String {
        "https://ui.perfetto.dev/#!/?url=http://127.0.0.1:9001/trace".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    fn read_trace(path: &std::path::Path) -> Trace {
        let bytes = std::fs::read(path).unwrap();
        Trace::parse_from_bytes(&bytes).unwrap()
    }

    fn expand(trace: Trace) -> Vec<TracePacket> {
        let mut out = Vec::new();
        for pkt in trace.packet {
            if pkt.has_compressed_packets() {
                let mut dec = ZlibDecoder::new(pkt.compressed_packets());
                let mut buf = Vec::new();
                dec.read_to_end(&mut buf).unwrap();
                let inner = Trace::parse_from_bytes(&buf).unwrap();
                out.extend(expand(inner));
            } else {
                out.push(pkt);
            }
        }
        out
    }

    fn emit_packets(b: &mut PerfettoTraceBuilder, n: u64) {
        for i in 0..n {
            b.add_process_track(i + 100, &format!("track-{}", i));
        }
    }

    #[test]
    fn test_compressed_roundtrip_matches_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let p_un = dir.path().join("un.pftrace");
        let p_c = dir.path().join("c.pftrace");

        let mut b_un = PerfettoTraceBuilder::new(p_un.clone(), false, false, false).unwrap();
        let mut b_c = PerfettoTraceBuilder::new(p_c.clone(), false, false, true).unwrap();
        emit_packets(&mut b_un, 1000);
        emit_packets(&mut b_c, 1000);
        b_un.build().unwrap();
        b_c.build().unwrap();

        let un = expand(read_trace(&p_un));
        let c = expand(read_trace(&p_c));
        assert_eq!(un.len(), c.len());
        for (a, b) in un.iter().zip(c.iter()) {
            assert_eq!(a.write_to_bytes().unwrap(), b.write_to_bytes().unwrap());
        }

        // Compressed file should collapse to fewer top-level packets
        // (ClockSnapshot stays raw + at least one outer compressed_packets pkt).
        let top_un = read_trace(&p_un).packet.len();
        let top_c = read_trace(&p_c).packet.len();
        assert!(top_c < top_un, "top_c={} top_un={}", top_c, top_un);
        assert!(top_c >= 2, "expected ClockSnapshot + compressed batch");
    }

    #[test]
    fn test_compressed_outer_packets_fit_perfetto_limit() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.pftrace");
        let mut b = PerfettoTraceBuilder::new(p.clone(), false, false, true).unwrap();
        // Enough packets to span several batches.
        emit_packets(&mut b, 50_000);
        b.build().unwrap();

        for pkt in read_trace(&p).packet {
            if pkt.has_compressed_packets() {
                let size = pkt.compute_size() as usize;
                assert!(size <= 512 * 1024, "outer packet {} > 512 KiB", size);
            }
        }
    }
}
