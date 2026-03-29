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
use perfetto_protos::interned_data::InternedData;
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

struct Sample {
    callstack_iid: u64,
    timestamp_us: i64,
}

pub struct PerfettoTraceBuilder {
    packets: Vec<TracePacket>,
    next_uuid: u64,
    next_sequence_id: u32,
    first_event_emitted: bool,

    function_name_iids: HashMap<String, u64>,
    frame_iids: HashMap<(u64, u64), u64>,
    callstack_iids: HashMap<Vec<u64>, u64>,
    next_intern_id: u64,

    host_uuids: HashMap<String, u64>,
    query_id_to_host: HashMap<String, String>,
    // (host_name, category) → category track uuid
    host_category_uuids: HashMap<(String, &'static str), u64>,
    per_server: bool,
}

impl PerfettoTraceBuilder {
    pub fn new(per_server: bool) -> Self {
        PerfettoTraceBuilder {
            packets: Vec::new(),
            next_uuid: 1,
            next_sequence_id: SEQUENCE_ID + 1,
            first_event_emitted: false,

            function_name_iids: HashMap::new(),
            frame_iids: HashMap::new(),
            callstack_iids: HashMap::new(),
            next_intern_id: 1,

            host_uuids: HashMap::new(),
            query_id_to_host: HashMap::new(),
            host_category_uuids: HashMap::new(),
            per_server,
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

    fn unit_for_event(name: &str) -> Unit {
        if name.ends_with("Bytes") {
            Unit::UNIT_SIZE_BYTES
        } else {
            Unit::UNIT_UNSPECIFIED
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

    // --- High-level methods ---

    pub fn add_queries(&mut self, queries: &[Query]) {
        // (host, user) → thread_uuid
        let mut user_uuids: HashMap<(String, String), u64> = HashMap::new();

        for q in queries {
            let host_uuid = if let Some(&uuid) = self.host_uuids.get(&q.host_name) {
                uuid
            } else {
                let uuid = self.alloc_uuid();
                self.add_process_track(uuid, &q.host_name);
                self.host_uuids.insert(q.host_name.clone(), uuid);
                uuid
            };

            self.query_id_to_host
                .insert(q.query_id.clone(), q.host_name.clone());

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

    fn get_host_category_track(&mut self, query_id: &str, category: &'static str) -> Option<u64> {
        if !self.per_server {
            return None;
        }
        let host = self.query_id_to_host.get(query_id)?.clone();
        let host_uuid = *self.host_uuids.get(&host)?;
        let key = (host, category);
        if let Some(&uuid) = self.host_category_uuids.get(&key) {
            Some(uuid)
        } else {
            let uuid = self.alloc_uuid();
            self.add_child_track(uuid, host_uuid, category);
            self.host_category_uuids.insert(key, uuid);
            Some(uuid)
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
        // (host_uuid, operation_name) → track_uuid
        let mut server_op_uuids: HashMap<(u64, String), u64> = HashMap::new();

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

            self.add_slice_begin(track_uuid, &operation_name, start_ns, annotations.clone());
            self.add_slice_end(track_uuid, end_ns);

            if let Some(cat_uuid) = self.get_host_category_track(&query_id, "OpenTelemetry Spans") {
                let server_track = *server_op_uuids
                    .entry((cat_uuid, operation_name.clone()))
                    .or_insert_with(|| {
                        let uuid = self.alloc_uuid();
                        self.add_child_track(uuid, cat_uuid, &operation_name);
                        uuid
                    });
                self.add_slice_begin(server_track, &operation_name, start_ns, annotations);
                self.add_slice_end(server_track, end_ns);
            }
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
        // (host_uuid, event_name) → (track_uuid, running_total)
        let mut server_tracks: HashMap<(u64, String), (u64, i64)> = HashMap::new();

        for i in 0..columns.row_count() {
            let event: String = columns.get(i, "event").unwrap_or_default();
            let increment: i64 = columns.get(i, "increment").unwrap_or(0);
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
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

            let unit = Self::unit_for_event(&event);
            let (track_uuid, running_total) =
                counter_tracks.entry(event.clone()).or_insert_with(|| {
                    let uuid = self.alloc_uuid();
                    self.add_counter_track(uuid, process_uuid, &event, unit);
                    (uuid, 0)
                });

            *running_total += increment;
            self.add_counter_value(*track_uuid, timestamp_ns, *running_total);

            if let Some(cat_uuid) = self.get_host_category_track(&query_id, "ProfileEvent Counters")
            {
                let (track_uuid, running_total) = server_tracks
                    .entry((cat_uuid, event.clone()))
                    .or_insert_with(|| {
                        let uuid = self.alloc_uuid();
                        self.add_counter_track(uuid, cat_uuid, &event, unit);
                        (uuid, 0)
                    });
                *running_total += increment;
                self.add_counter_value(*track_uuid, timestamp_ns, *running_total);
            }
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
        // (host_uuid, metric_name) → track_uuid
        let mut server_tracks: HashMap<(u64, String), u64> = HashMap::new();

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

                if let Some(cat_uuid) = self.get_host_category_track(&row.query_id, "Query Metrics")
                {
                    let server_track = *server_tracks
                        .entry((cat_uuid, name.to_string()))
                        .or_insert_with(|| {
                            let uuid = self.alloc_uuid();
                            self.add_counter_track(uuid, cat_uuid, name, unit);
                            uuid
                        });
                    self.add_counter_value(server_track, row.timestamp_ns, value);
                }
            }

            // ProfileEvent_* metrics
            for (name, value) in &row.profile_events {
                let unit = Self::unit_for_event(name);
                let track_uuid = *counter_tracks.entry(name.clone()).or_insert_with(|| {
                    let uuid = self.alloc_uuid();
                    self.add_counter_track(uuid, process_uuid, name, unit);
                    uuid
                });
                self.add_counter_value(track_uuid, row.timestamp_ns, *value as i64);

                if let Some(cat_uuid) = self.get_host_category_track(&row.query_id, "Query Metrics")
                {
                    let server_track = *server_tracks
                        .entry((cat_uuid, name.clone()))
                        .or_insert_with(|| {
                            let uuid = self.alloc_uuid();
                            self.add_counter_track(uuid, cat_uuid, name, unit);
                            uuid
                        });
                    self.add_counter_value(server_track, row.timestamp_ns, *value as i64);
                }
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
        // (host_uuid, "db.table") → track_uuid
        let mut server_table_uuids: HashMap<(u64, String), u64> = HashMap::new();

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

            if let Some(cat_uuid) = self.get_host_category_track(&query_id, "Part Log") {
                let server_track = *server_table_uuids
                    .entry((cat_uuid, table_key.clone()))
                    .or_insert_with(|| {
                        let uuid = self.alloc_uuid();
                        self.add_child_track(uuid, cat_uuid, &table_key);
                        uuid
                    });
                self.add_slice_begin(server_track, &label, start_ns, annotations);
                self.add_slice_end(server_track, end_ns);
            }
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
        // (host_uuid, thread_name) → track_uuid
        let mut server_thread_uuids: HashMap<(u64, String), u64> = HashMap::new();

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

            self.add_slice_begin(track_uuid, &query_id, start_ns, annotations.clone());
            self.add_slice_end(track_uuid, end_ns);

            if let Some(cat_uuid) = self.get_host_category_track(&query_id, "Query Threads") {
                let server_track = *server_thread_uuids
                    .entry((cat_uuid, thread_name.clone()))
                    .or_insert_with(|| {
                        let uuid = self.alloc_uuid();
                        self.add_child_track(uuid, cat_uuid, &thread_name);
                        uuid
                    });
                self.add_slice_begin(server_track, &query_id, start_ns, annotations);
                self.add_slice_end(server_track, end_ns);
            }
        }
    }

    pub fn add_text_logs(&mut self, columns: &Columns) {
        if columns.row_count() == 0 {
            return;
        }

        let process_uuid = self.alloc_uuid();
        self.add_process_track(process_uuid, "Query Logs");

        // level → track_uuid
        let mut level_uuids: HashMap<String, u64> = HashMap::new();
        // (host_uuid, level) → track_uuid
        let mut server_level_uuids: HashMap<(u64, String), u64> = HashMap::new();

        for i in 0..columns.row_count() {
            let level: String = columns.get(i, "level").unwrap_or_default();
            let logger_name: String = columns.get(i, "logger_name").unwrap_or_default();
            let message: String = columns.get(i, "message").unwrap_or_default();
            let query_id: String = columns.get(i, "query_id").unwrap_or_default();
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

            let track_uuid = *level_uuids.entry(level.clone()).or_insert_with(|| {
                let uuid = self.alloc_uuid();
                self.add_child_track(uuid, process_uuid, &level);
                uuid
            });

            let label = if message.len() > 80 {
                format!("{}...", &message[..80])
            } else {
                message.clone()
            };

            let annotations = vec![
                Self::make_annotation_str("level", &level),
                Self::make_annotation_str("logger", &logger_name),
                Self::make_annotation_str("message", &message),
            ];

            self.add_instant(track_uuid, &label, timestamp_ns, annotations.clone());

            if let Some(cat_uuid) = self.get_host_category_track(&query_id, "Query Logs") {
                let server_track = *server_level_uuids
                    .entry((cat_uuid, level.clone()))
                    .or_insert_with(|| {
                        let uuid = self.alloc_uuid();
                        self.add_child_track(uuid, cat_uuid, &level);
                        uuid
                    });
                self.add_instant(server_track, &label, timestamp_ns, annotations);
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
    pub fn add_stack_traces(&mut self, columns: &Columns) {
        if columns.row_count() == 0 {
            return;
        }

        // Global: trace_type → samples
        let mut samples_by_type: HashMap<String, Vec<Sample>> = HashMap::new();
        // Per-server: (host_name, trace_type) → samples
        let mut samples_by_host_type: HashMap<(String, String), Vec<Sample>> = HashMap::new();

        // Interning accumulators for this batch
        let mut interned_strings: Vec<InternedString> = Vec::new();
        let mut interned_frames: Vec<Frame> = Vec::new();
        let mut interned_callstacks: Vec<Callstack> = Vec::new();

        let mapping_iid = self.alloc_intern_id();

        for i in 0..columns.row_count() {
            let trace_type: String = columns.get(i, "trace_type").unwrap_or_default();
            let stack: Vec<String> = columns.get(i, "stack").unwrap_or_default();

            if stack.is_empty() {
                continue;
            }

            let timestamp_us: i64 =
                match columns.get::<DateTime<Tz>, _>(i, "event_time_microseconds") {
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
                        interned_strings.push(is);
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
                    interned_frames.push(f);
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
                        interned_callstacks.push(cs);
                        iid
                    });

            samples_by_type
                .entry(trace_type.clone())
                .or_default()
                .push(Sample {
                    callstack_iid,
                    timestamp_us,
                });

            if self.per_server {
                let query_id: String = columns.get(i, "query_id").unwrap_or_default();
                if let Some(host) = self.query_id_to_host.get(&query_id) {
                    samples_by_host_type
                        .entry((host.clone(), trace_type))
                        .or_default()
                        .push(Sample {
                            callstack_iid,
                            timestamp_us,
                        });
                }
            }
        }

        // Build one dummy mapping
        let mut mapping = Mapping::new();
        mapping.iid = Some(mapping_iid);

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
        self.packets.push(desc_pkt);

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
        self.packets.push(pkt);
    }

    /// Build a ClockSnapshot mapping sequence-scoped clock 128 → BOOTTIME.
    /// Both at timestamp 0 with 1ns multiplier, so raw nanosecond values pass through as-is.
    fn make_clock_snapshot() -> ClockSnapshot {
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
        cs
    }

    pub fn build(self) -> Vec<u8> {
        // ClockSnapshot with timestamp=0 in its own clock (self-referencing).
        // The trace processor resolves this specially for ClockSnapshot packets,
        // placing it at the very start of the trace (time 0).
        let cs = Self::make_clock_snapshot();
        let mut cs_pkt = TracePacket::new();
        cs_pkt.set_trusted_packet_sequence_id(SEQUENCE_ID);
        cs_pkt.sequence_flags = Some(1 | 2);
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
