use crate::interpreter::Query;
use crate::interpreter::clickhouse::{Columns, QueryMetricRow};
use chrono::{DateTime, Local};
use chrono_tz::Tz;
use perfetto_protos::clock_snapshot::ClockSnapshot;
use perfetto_protos::clock_snapshot::clock_snapshot::Clock;
use perfetto_protos::counter_descriptor::CounterDescriptor;
use perfetto_protos::counter_descriptor::counter_descriptor::Unit;
use perfetto_protos::debug_annotation::DebugAnnotation;
use perfetto_protos::debug_annotation::debug_annotation as da;
use perfetto_protos::trace::Trace;
use perfetto_protos::trace_packet::TracePacket;
use perfetto_protos::trace_packet::trace_packet::Data;
use perfetto_protos::track_descriptor::TrackDescriptor;
use perfetto_protos::track_descriptor::track_descriptor::Static_or_dynamic_name;
use perfetto_protos::track_event::TrackEvent;
use perfetto_protos::track_event::track_event::{Counter_value_field, Name_field, Type};
use protobuf::{EnumOrUnknown, Message, MessageField};
use std::collections::HashMap;
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

pub struct PerfettoTraceBuilder {
    packets: Vec<TracePacket>,
    next_uuid: u64,
    first_event_emitted: bool,
}

impl PerfettoTraceBuilder {
    pub fn new() -> Self {
        PerfettoTraceBuilder {
            packets: Vec::new(),
            next_uuid: 1,
            first_event_emitted: false,
        }
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
        self.packets.push(pkt);
    }

    fn add_child_track(&mut self, uuid: u64, parent_uuid: u64, name: &str) {
        let mut pkt = self.make_packet();
        let mut td = TrackDescriptor::new();
        td.uuid = Some(uuid);
        td.parent_uuid = Some(parent_uuid);
        td.static_or_dynamic_name = Some(Static_or_dynamic_name::Name(name.to_string()));
        pkt.data = Some(Data::TrackDescriptor(td));
        self.packets.push(pkt);
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
        self.packets.push(pkt);
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
        self.packets.push(pkt);
    }

    fn add_slice_end(&mut self, track_uuid: u64, ts_ns: u64) {
        let mut pkt = self.make_event_packet(ts_ns);
        let mut te = TrackEvent::new();
        te.type_ = Some(EnumOrUnknown::new(Type::TYPE_SLICE_END));
        te.track_uuid = Some(track_uuid);
        pkt.data = Some(Data::TrackEvent(te));
        self.packets.push(pkt);
    }

    fn add_counter_value(&mut self, track_uuid: u64, ts_ns: u64, value: i64) {
        let mut pkt = self.make_event_packet(ts_ns);
        let mut te = TrackEvent::new();
        te.type_ = Some(EnumOrUnknown::new(Type::TYPE_COUNTER));
        te.track_uuid = Some(track_uuid);
        te.counter_value_field = Some(Counter_value_field::CounterValue(value));
        pkt.data = Some(Data::TrackEvent(te));
        self.packets.push(pkt);
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

    // --- High-level methods ---

    pub fn add_queries(&mut self, queries: &[Query]) {
        // Group by host_name → process, then user → thread
        let mut host_uuids: HashMap<String, u64> = HashMap::new();
        // (host, user) → thread_uuid
        let mut user_uuids: HashMap<(String, String), u64> = HashMap::new();

        for q in queries {
            let host_uuid = *host_uuids.entry(q.host_name.clone()).or_insert_with(|| {
                let uuid = self.alloc_uuid();
                self.add_process_track(uuid, &q.host_name);
                uuid
            });

            let user_key = (q.host_name.clone(), q.user.clone());
            let user_uuid = *user_uuids.entry(user_key).or_insert_with(|| {
                let uuid = self.alloc_uuid();
                self.add_child_track(uuid, host_uuid, &q.user);
                uuid
            });

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

    pub fn add_otel_spans(&mut self, columns: &Columns) {
        if columns.row_count() == 0 {
            return;
        }

        // Group spans by operation_name → thread track under query's host process
        // Use a single process track for OTel spans
        let process_uuid = self.alloc_uuid();
        self.add_process_track(process_uuid, "OpenTelemetry Spans");

        let mut op_uuids: HashMap<String, u64> = HashMap::new();

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

            let start_ns = start_us.saturating_mul(1000);
            let end_ns = finish_us.saturating_mul(1000);

            let track_uuid = *op_uuids.entry(operation_name.clone()).or_insert_with(|| {
                let uuid = self.alloc_uuid();
                self.add_child_track(
                    uuid,
                    process_uuid,
                    &format!("Processor: {}", operation_name),
                );
                uuid
            });

            let annotations = vec![Self::make_annotation_str("query_id", &query_id)];

            self.add_slice_begin(track_uuid, &operation_name, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn add_trace_log_counters(&mut self, columns: &Columns) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.alloc_uuid();
        self.add_process_track(process_uuid, "ProfileEvent Counters");

        // event_name → (track_uuid, running_total)
        let mut counter_tracks: HashMap<String, (u64, i64)> = HashMap::new();

        for i in 0..columns.row_count() {
            let event: String = columns.get(i, "event").unwrap_or_default();
            let increment: i64 = columns.get(i, "increment").unwrap_or(0);
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

            let (track_uuid, running_total) =
                counter_tracks.entry(event.clone()).or_insert_with(|| {
                    let uuid = self.alloc_uuid();
                    self.add_counter_track(uuid, process_uuid, &event, Unit::UNIT_UNSPECIFIED);
                    (uuid, 0)
                });

            *running_total += increment;
            self.add_counter_value(*track_uuid, timestamp_ns, *running_total);
        }
    }

    pub fn add_query_metrics(&mut self, rows: &[QueryMetricRow]) {
        if rows.is_empty() {
            return;
        }

        let process_uuid = self.alloc_uuid();
        self.add_process_track(process_uuid, "Query Metrics");

        // metric_name → track_uuid
        let mut counter_tracks: HashMap<String, u64> = HashMap::new();

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
                let track_uuid = *counter_tracks.entry(name.to_string()).or_insert_with(|| {
                    let uuid = self.alloc_uuid();
                    self.add_counter_track(uuid, process_uuid, name, unit);
                    uuid
                });
                self.add_counter_value(track_uuid, row.timestamp_ns, value);
            }

            // ProfileEvent_* metrics
            for (name, value) in &row.profile_events {
                let track_uuid = *counter_tracks.entry(name.clone()).or_insert_with(|| {
                    let uuid = self.alloc_uuid();
                    self.add_counter_track(uuid, process_uuid, name, Unit::UNIT_UNSPECIFIED);
                    uuid
                });
                self.add_counter_value(track_uuid, row.timestamp_ns, *value as i64);
            }
        }
    }

    pub fn add_part_log(&mut self, columns: &Columns) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.alloc_uuid();
        self.add_process_track(process_uuid, "Part Log");

        // "db.table" → thread_uuid
        let mut table_uuids: HashMap<String, u64> = HashMap::new();

        for i in 0..columns.row_count() {
            let event_type: String = columns.get(i, "event_type").unwrap_or_default();
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

            let table_key = format!("{}.{}", database, table);
            let track_uuid = *table_uuids.entry(table_key.clone()).or_insert_with(|| {
                let uuid = self.alloc_uuid();
                self.add_child_track(uuid, process_uuid, &table_key);
                uuid
            });

            let start_ns = match event_time.with_timezone(&Local).timestamp_nanos_opt() {
                Some(ns) => ns as u64,
                None => {
                    log::warn!("Perfetto: part_log row {} timestamp overflow", i);
                    continue;
                }
            };
            let end_ns = start_ns + duration_ms * 1_000_000;

            let label = format!("{} {}", event_type, part_name);
            let annotations = vec![
                Self::make_annotation_str("query_id", &query_id),
                Self::make_annotation_str("part_name", &part_name),
                Self::make_annotation_int("rows", rows as i64),
                Self::make_annotation_int("size_in_bytes", size_in_bytes as i64),
            ];

            self.add_slice_begin(track_uuid, &label, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn add_query_thread_log(&mut self, columns: &Columns) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.alloc_uuid();
        self.add_process_track(process_uuid, "Query Threads");

        // thread_name → track_uuid
        let mut thread_uuids: HashMap<String, u64> = HashMap::new();

        for i in 0..columns.row_count() {
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
            let thread_name: String = columns.get(i, "thread_name").unwrap_or_default();
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

            let names: Vec<String> = columns.get(i, "ProfileEvents.Names").unwrap_or_default();
            let values: Vec<u64> = columns.get(i, "ProfileEvents.Values").unwrap_or_default();

            let track_uuid = *thread_uuids.entry(thread_name.clone()).or_insert_with(|| {
                let uuid = self.alloc_uuid();
                self.add_child_track(uuid, process_uuid, &thread_name);
                uuid
            });

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

            self.add_slice_begin(track_uuid, &query_id, start_ns, annotations);
            self.add_slice_end(track_uuid, end_ns);
        }
    }

    pub fn build(mut self) -> Vec<u8> {
        // ClockSnapshot: map sequence-scoped clock 128 → BOOTTIME (default trace clock)
        // Both at timestamp 0 with 1ns multiplier, so raw nanosecond values pass through as-is
        let mut cs = ClockSnapshot::new();
        let mut unixtime_clock = Clock::new();
        unixtime_clock.clock_id = Some(CLOCK_ID_UNIXTIME);
        unixtime_clock.timestamp = Some(0);
        unixtime_clock.unit_multiplier_ns = Some(1);
        unixtime_clock.is_incremental = Some(false);
        let mut boottime_clock = Clock::new();
        boottime_clock.clock_id = Some(6); // BUILTIN_CLOCK_BOOTTIME
        boottime_clock.timestamp = Some(0);
        boottime_clock.unit_multiplier_ns = Some(1);
        boottime_clock.is_incremental = Some(false);
        cs.clocks = vec![unixtime_clock, boottime_clock];

        let mut cs_pkt = self.make_packet();
        cs_pkt.timestamp = Some(0);
        cs_pkt.timestamp_clock_id = Some(CLOCK_ID_UNIXTIME);
        cs_pkt.data = Some(Data::ClockSnapshot(cs));

        let mut trace = Trace::new();
        trace.packet = Vec::with_capacity(self.packets.len() + 1);
        trace.packet.push(cs_pkt);
        trace.packet.extend(self.packets);
        trace.write_to_bytes().unwrap_or_default()
    }
}

pub struct PerfettoServer {
    trace_data: Arc<Mutex<Option<Arc<Vec<u8>>>>>,
    #[allow(dead_code)]
    server_thread: Option<std::thread::JoinHandle<()>>,
}

impl PerfettoServer {
    pub fn new() -> Self {
        let trace_data: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
        let trace_data_clone = trace_data.clone();

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
                    let data: Option<Arc<Vec<u8>>> = trace_data_clone.lock().unwrap().clone();
                    match data {
                        Some(bytes) => {
                            let response = tiny_http::Response::from_data((*bytes).clone())
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
            trace_data,
            server_thread: Some(server_thread),
        }
    }

    pub fn set_trace(&self, data: Vec<u8>) {
        *self.trace_data.lock().unwrap() = Some(Arc::new(data));
    }

    pub fn get_perfetto_url(&self) -> String {
        "https://ui.perfetto.dev/#!/?url=http://127.0.0.1:9001/trace".to_string()
    }
}
