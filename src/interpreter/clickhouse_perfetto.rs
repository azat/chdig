use crate::generated::perfetto_protos::{track_event, EventName, Mapping};
use crate::generated::perfetto_protos::{Trace, TracePacket, trace_packet::OptionalTrustedPacketSequenceId};
use crate::interpreter::clickhouse::{ClickHouse, Columns};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub enum InternedDataType {
    EventName = 0,
    FunctionName = 1,
    SourceLocation = 2,
    LogMessageBody = 3,
    BuildId = 4,
    MappingPath = 5,
    SourcePath = 6,
    Frame = 7,
    Callstack = 8,
    Mapping = 9,
}

const CLOCK_ID_UNIXTIME: u32 = 128;


// Perfetto trace builder with global interned data state
pub struct PerfettoTraceBuilder {
    // Array of HashMaps for each interned data type
    maps: [Mutex<HashMap<String, u64>>; 10],
    // Array of ID counters for each type
    next_ids: [AtomicU64; 10],
}

impl PerfettoTraceBuilder {
    pub fn new() -> Self {
        Self {
            maps: [
                Mutex::new(HashMap::new()), // EventName
                Mutex::new(HashMap::new()), // FunctionName
                Mutex::new(HashMap::new()), // SourceLocation
                Mutex::new(HashMap::new()), // LogMessageBody
                Mutex::new(HashMap::new()), // BuildId
                Mutex::new(HashMap::new()), // MappingPath
                Mutex::new(HashMap::new()), // SourcePath
                Mutex::new(HashMap::new()), // Frame
                Mutex::new(HashMap::new()), // Callstack
                Mutex::new(HashMap::new()), // Mapping
            ],
            next_ids: [
                AtomicU64::new(1), AtomicU64::new(1), AtomicU64::new(1), 
                AtomicU64::new(1), AtomicU64::new(1), AtomicU64::new(1),
                AtomicU64::new(1), AtomicU64::new(1), AtomicU64::new(1),
                AtomicU64::new(1)
            ],
        }
    }
    
    // Universal helper function to get or create interned ID with double-checked locking
    pub async fn get_or_create_id(&self, data_type: InternedDataType, value: &str) -> (u64, bool) {
        let index = data_type as usize;
        
        // First check without locking
        {
            let map = self.maps[index].lock().await;
            if let Some(&id) = map.get(value) {
                return (id, false); // exists, don't add to local interned data
            }
        }
        
        // Value not found, take the lock and check again (double-checked locking)
        let mut map = self.maps[index].lock().await;
        if let Some(&id) = map.get(value) {
            (id, false) // Another thread added it while we were waiting
        } else {
            let id: u64 = self.next_ids[index].fetch_add(1, Ordering::SeqCst);
            map.insert(value.to_string(), id);
            (id, true) // new entry, add to local interned data
        }
    }
    
    pub async fn create_streaming_profile_packet(&self, clickhouse: &ClickHouse, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{
            trace_packet, StreamingProfilePacket, Callstack, Frame
        };
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let track_uuid = block.get::<u32, _>(row, "track_uuid")?;
        let timestamp_start_ns = block.get::<i64, _>(row, "timestamp_start_ns")?;
        
        // Extract actual data from ClickHouse
        let mut timestamp_delta_us = block.get::<Vec<i64>, _>(row, "timestamp_delta_us")?;
        timestamp_delta_us[0] = (timestamp_start_ns as f64 / 1000.0).round() as i64;

        let trusted_packet_sequence_id = track_uuid;
        // Extract callstack from ClickHouse data
        let callstack_data = block.get::<Vec<u64>, _>(row, "callstack")?;
        let frame_symbols = block.get::<Vec<String>, _>(row, "frame")?;
        
        let mut local_function_names = Vec::new();
        let mut local_frames = Vec::new();
        let mut local_callstacks = Vec::new();
        
        let mapping_path_key = format!("{}:{}", trusted_packet_sequence_id, "");
        let (mapping_path_id, mapping_path_is_new) = self.get_or_create_id(InternedDataType::MappingPath, &mapping_path_key).await;
        
        let build_key = format!("{}:{}", trusted_packet_sequence_id, "");
        let (build_id, build_id_is_new) = self.get_or_create_id(InternedDataType::BuildId, &build_key).await;
    
        let mapping_key = format!("{}:{}:{}", trusted_packet_sequence_id, build_id, mapping_path_id);
        let (mapping_id, mapping_is_new) = self.get_or_create_id(InternedDataType::Mapping, &mapping_key).await;
        
        // Create frames from callstack data using global function name IDs
        let mut global_frame_ids = Vec::new();
        
        for (&frame_id, symbol) in callstack_data.iter().zip(frame_symbols.iter()) {
            let symbol_key = format!("{}:{}", trusted_packet_sequence_id, symbol);
            let (function_name_id, func_is_new) = self.get_or_create_id(InternedDataType::FunctionName, &symbol_key).await;
            
            // If it's a new function name, add it to local interned data
            if func_is_new {
                local_function_names.push(crate::generated::perfetto_protos::InternedString {
                    iid: Some(function_name_id),
                    str: Some(symbol.as_bytes().to_vec()),
                });
            }
            
            // Create unique key for frame (symbol + mapping)
            let frame_key = format!("{}:{}", symbol_key, mapping_id);
            let (global_frame_id, frame_is_new) = self.get_or_create_id(InternedDataType::Frame, &frame_key).await;
            
            if frame_is_new {
                local_frames.push(Frame {
                    iid: Some(global_frame_id),
                    function_name_id: Some(function_name_id),
                    mapping_id: Some(mapping_id),
                    rel_pc: None,
                });
            }
            
            global_frame_ids.push(global_frame_id);
        }
        
        // Create callstack with global frame IDs
        let callstack_key = format!("{}:{}", trusted_packet_sequence_id, global_frame_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(","));
        let (callstack_id, callstack_is_new) = self.get_or_create_id(InternedDataType::Callstack, &callstack_key).await;
        
        if callstack_is_new {
            local_callstacks.push(Callstack {
                iid: Some(callstack_id),
                frame_ids: global_frame_ids,
            });
        }
        
        // Create streaming profile packet with global callstack ID
        let callstack_iid = vec![callstack_id; timestamp_delta_us.len()];
        let streaming_profile_packet = StreamingProfilePacket {
            callstack_iid,
            timestamp_delta_us,
            process_priority: None,
        };
        
        // Create local collections for new entries
        let mut local_mapping_paths = Vec::new();
        let mut local_mappings = Vec::new();
        let mut local_build_ids = Vec::new();
        
        // Add to local collections if they're new
        if mapping_path_is_new {
            local_mapping_paths.push(crate::generated::perfetto_protos::InternedString {
                iid: Some(mapping_path_id),
                str: Some("".into()),
            });
        }
        
        if build_id_is_new {
            local_build_ids.push(crate::generated::perfetto_protos::InternedString {
                iid: Some(build_id),
                str: Some("".into()),
            });
        }
        
        if mapping_is_new {
            local_mappings.push(crate::generated::perfetto_protos::Mapping {
                iid: Some(mapping_id),
                build_id: Some(build_id),
                path_string_ids: vec![mapping_path_id],
                exact_offset: None,
                start_offset: None,
                start: None,
                end: None,
                load_bias: None,
            });
        }
        
        // Create InternedData with only new entries
        let mut interned_data = ClickHouse::create_interned_data_base(
            Vec::new(),
            Vec::new(),
            local_callstacks, // Only new callstacks for this packet
            local_frames,     // Only new frames for this packet
            local_function_names, // Only new function names for this packet
            Vec::new(),
            Vec::new(),
        );
        
        // Add the mapping-related fields manually
        interned_data.mapping_paths = local_mapping_paths;
        interned_data.mappings = local_mappings;
        interned_data.build_ids = local_build_ids;
        
        let mut track_packet = clickhouse.create_trace_packet_base(
            machine_id,
            Some(timestamp_start_ns as u64),
            track_uuid as u32,
            trace_packet::Data::StreamingProfilePacket(streaming_profile_packet),
            Some(interned_data)
        );
        track_packet.sequence_flags = Some(3);

        Ok(track_packet)
    }
    
    pub async fn create_query_log_packet(&self, clickhouse: &ClickHouse, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{
            trace_packet, LogMessage, SourceLocation, LogMessageBody
        };
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let track_uuid = block.get::<u32, _>(row, "track_uuid")? as u64;
        let timestamp_ns = block.get::<i64, _>(row, "timestamp_ns")? as u64;
        let message = block.get::<String, _>(row, "message")?;
        let file_name = block.get::<String, _>(row, "file_name")?;
        let func_name = block.get::<String, _>(row, "func_name")?;
        let line_number = block.get::<u64, _>(row, "line_number")? as u32;
        let prio = block.get::<i32, _>(row, "prio")?;
        
        // Create unique keys for source location and log message body
        let source_location_key = format!("{}:{}:{}", file_name, func_name, line_number);
        let log_message_key = message.clone();
        
        // Use global interned data system
        let (source_location_iid, source_is_new) = self.get_or_create_id(InternedDataType::SourceLocation, &source_location_key).await;
        let (body_iid, body_is_new) = self.get_or_create_id(InternedDataType::LogMessageBody, &log_message_key).await;
        
        // Create log message
        let log_message = LogMessage {
            source_location_iid: Some(source_location_iid),
            body_iid: Some(body_iid),
            prio: Some(prio as i32),
        };
        
        // Create track event with log message
        let track_event = clickhouse.create_track_event_base(
            Some(track_uuid),
            Some(3), // TYPE_INSTANT = 3
            Some(track_event::NameField::Name("Log".to_owned())),
            None,
            None,
            Some(log_message)
        );
        
        // Create local interned data only for new entries
        let mut local_source_locations = Vec::new();
        let mut local_log_message_bodies = Vec::new();
        
        if source_is_new {
            local_source_locations.push(SourceLocation {
                iid: Some(source_location_iid),
                file_name: Some(file_name),
                function_name: Some(func_name),
                line_number: Some(line_number),
            });
        }
        
        if body_is_new {
            local_log_message_bodies.push(LogMessageBody {
                iid: Some(body_iid),
                body: Some(message),
            });
        }
        
        let interned_data = ClickHouse::create_interned_data_base(
            local_source_locations,
            local_log_message_bodies,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        
        Ok(clickhouse.create_trace_packet_base(
            machine_id,
            Some(timestamp_ns),
            1 as u32,
            trace_packet::Data::TrackEvent(track_event),
            Some(interned_data)
        ))
    }
    
    pub async fn create_processor_event_packet(&self, clickhouse: &ClickHouse, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, track_event};
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let timestamp_ns = block.get::<u64, _>(row, "timestamp_ns")?;
        let track_uuid: u64 = block.get::<u32, _>(row, "track_uuid")? as u64;
        let operation_name = block.get::<String, _>(row, "operation_name")?;
        let event_type = block.get::<String, _>(row, "type")?;
        let thread_time_absolute_us = block.get::<u64, _>(row, "thread_time_absolute_us")?;
        
        // Use global interned data system for event names
        let (name_iid, name_is_new) = self.get_or_create_id(InternedDataType::EventName, &operation_name).await;
        
        // Map string type to TrackEvent type enum
        let track_event_type = match event_type.as_str() {
            "TYPE_SLICE_BEGIN" => 1, // TYPE_SLICE_BEGIN
            "TYPE_SLICE_END" => 2,   // TYPE_SLICE_END  
            _ => 0, // TYPE_UNSPECIFIED
        };
        
        let thread_time: Option<track_event::ThreadTime> = if event_type == "TYPE_SLICE_BEGIN" {
            Some(track_event::ThreadTime::ThreadTimeAbsoluteUs(thread_time_absolute_us as i64))
        } else {
            None
        };

        let track_event = clickhouse.create_track_event_base(
            Some(track_uuid),
            Some(track_event_type),
            Some(track_event::NameField::NameIid(name_iid)),
            None,
            thread_time,
            None
        );

        // Create local interned data only for new event names
        let interned_data = if name_is_new {
            let interned_name = crate::generated::perfetto_protos::EventName {
                iid: Some(name_iid),
                name: Some(operation_name.to_string()),
            };

            Some(ClickHouse::create_interned_data_base(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![interned_name],
            ))
        } else {
            None
        };

        Ok(clickhouse.create_trace_packet_base(
            machine_id,
            Some(timestamp_ns),
            1 as u32,
            trace_packet::Data::TrackEvent(track_event),
            interned_data
        ))
    }
    
    /// Get Perfetto processor events data as protobuf packets using global interned data
    pub async fn get_perfetto_processors_events_packets(&self, clickhouse: &ClickHouse, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = clickhouse.get_perfetto_processors_events(query_ids, start, end).await?;
        let mut results = Vec::new();
        let mut seen_tracks: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for i in 0..block.row_count() {
            let track_uuid: u64 = block.get::<u32, _>(i, "track_uuid")? as u64;
            let machine_id = block.get::<u32, _>(i, "machine_id")?;
            let parent_uuid = block.get::<u32, _>(i, "parent_uuid")? as u64;
            let timestamp_ns = block.get::<u64, _>(i, "timestamp_ns")?;

            // Create track if not seen yet
            if !seen_tracks.contains(&track_uuid) {
                seen_tracks.insert(track_uuid);
                results.push(clickhouse.create_child_track_packet(machine_id, track_uuid, parent_uuid, Some(timestamp_ns))?);
            }

            results.push(self.create_processor_event_packet(clickhouse, &block, i).await?);
        }
        
        Ok(results)
    }
    
    /// Get Perfetto streaming profile packets using global interned data
    pub async fn get_perfetto_streaming_profile_packets(&self, clickhouse: &ClickHouse, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = clickhouse.get_perfetto_streaming_profile_stack(query_ids, start, end).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_streaming_profile_packet(clickhouse, &block, i).await?);
        }
        
        Ok(results)
    }
    
    /// Get Perfetto query log packets using global interned data
    pub async fn get_perfetto_query_logs_packets(&self, clickhouse: &ClickHouse, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = clickhouse.get_perfetto_query_logs(query_ids, start, end).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_query_log_packet(clickhouse, &block, i).await?);
        }
        
        Ok(results)
    }
}

// Helper functions for creating Perfetto protobuf structures
impl ClickHouse {
    /// Creates a TracePacket with common fields populated
    fn create_trace_packet_base(
        &self, 
        machine_id: u32, 
        timestamp: Option<u64>,
        trusted_packet_sequence_id: u32,
        data: crate::generated::perfetto_protos::trace_packet::Data,
        interned_data: Option<crate::generated::perfetto_protos::InternedData>
    ) -> TracePacket {
        TracePacket {
            timestamp,
            timestamp_clock_id: Some(CLOCK_ID_UNIXTIME),
            trusted_pid: None,
            interned_data,
            sequence_flags: Some(2),
            incremental_state_cleared: None,
            trace_packet_defaults: None,
            previous_packet_dropped: None,
            first_packet_on_sequence: None,
            machine_id: Some(machine_id),
            data: Some(data),
            optional_trusted_uid: None,
            optional_trusted_packet_sequence_id: Some(OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(trusted_packet_sequence_id)),
        }
    }

    /// Creates an InternedData with all fields initialized to empty and specific fields populated
    fn create_interned_data_base(
        source_locations: Vec<crate::generated::perfetto_protos::SourceLocation>,
        log_message_body: Vec<crate::generated::perfetto_protos::LogMessageBody>,
        callstacks: Vec<crate::generated::perfetto_protos::Callstack>,
        frames: Vec<crate::generated::perfetto_protos::Frame>,
        function_names: Vec<crate::generated::perfetto_protos::InternedString>,
        event_categories: Vec<crate::generated::perfetto_protos::EventCategory>,
        event_names: Vec<crate::generated::perfetto_protos::EventName>
    ) -> crate::generated::perfetto_protos::InternedData {
        use crate::generated::perfetto_protos::InternedData;
        
        InternedData {
            source_locations: source_locations,
            log_message_body: log_message_body,
            callstacks: callstacks,
            frames: frames,
            function_names: function_names,
            event_categories: event_categories,
            event_names: event_names,
            // Initialize all other fields to empty
            debug_annotation_names: Vec::new(),
            debug_annotation_value_type_names: Vec::new(),
            unsymbolized_source_locations: Vec::new(),
            histogram_names: Vec::new(),
            build_ids: Vec::new(),
            mapping_paths: Vec::new(),
            source_paths: Vec::new(),
            mappings: Vec::new(),
            vulkan_memory_keys: Vec::new(),
            graphics_contexts: Vec::new(),
            gpu_specifications: Vec::new(),
            kernel_symbols: Vec::new(),
            debug_annotation_string_values: Vec::new(),
            packet_context: Vec::new(),
            v8_js_function_name: Vec::new(),
            v8_js_function: Vec::new(),
            v8_js_script: Vec::new(),
            v8_wasm_script: Vec::new(),
            v8_isolate: Vec::new(),
            protolog_string_args: Vec::new(),
            protolog_stacktrace: Vec::new(),
            viewcapture_package_name: Vec::new(),
            viewcapture_window_name: Vec::new(),
            viewcapture_view_id: Vec::new(),
            viewcapture_class_name: Vec::new(),
            app_wakelock_info: Vec::new(),
            correlation_id_str: Vec::new(),
        }
    }

    /// Creates a TrackEvent with all fields initialized to defaults and specific fields populated
    fn create_track_event_base(
        &self,
        track_uuid: Option<u64>,
        r#type: Option<i32>,
        name_field: Option<crate::generated::perfetto_protos::track_event::NameField>,
        counter_value_field: Option<crate::generated::perfetto_protos::track_event::CounterValueField>,
        thread_time: Option<crate::generated::perfetto_protos::track_event::ThreadTime>,
        log_message: Option<crate::generated::perfetto_protos::LogMessage>,
    ) -> crate::generated::perfetto_protos::TrackEvent {
        use crate::generated::perfetto_protos::TrackEvent;
        
        TrackEvent {
            track_uuid,
            r#type,
            name_field,
            counter_value_field,
            thread_time,
            log_message,
            // Initialize all other fields to empty/None
            categories: Vec::new(),
            category_iids: Vec::new(),
            flow_ids_old: Vec::new(),
            terminating_flow_ids_old: Vec::new(),
            debug_annotations: Vec::new(),
            task_execution: None,
            cc_scheduler_state: None,
            chrome_user_event: None,
            chrome_legacy_ipc: None,
            chrome_keyed_service: None,
            chrome_histogram_sample: None,
            chrome_latency_info: None,
            chrome_frame_reporter: None,
            chrome_application_state_info: None,
            chrome_renderer_scheduler_state: None,
            chrome_window_handle_event_info: None,
            chrome_content_settings_event_info: None,
            chrome_active_processes: None,
            screenshot: None,
            chrome_message_pump: None,
            chrome_mojo_event_info: None,
            timestamp: None,
            legacy_event: None,
            extra_counter_track_uuids: Vec::new(),
            extra_counter_values: Vec::new(),
            extra_double_counter_track_uuids: Vec::new(),
            extra_double_counter_values: Vec::new(),
            correlation_id_field: None,
            flow_ids: Vec::new(),
            terminating_flow_ids: Vec::new(),
            source_location_field: None,
            thread_instruction_count: None,
        }
    }
    fn create_track_query_packet(&self, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, ProcessDescriptor, TrackDescriptor};
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let uuid = block.get::<u32, _>(row, "uuid")? as u64;
        let query_id = block.get::<String, _>(row, "query_id")?;
        let hostname = block.get::<String, _>(row, "hostname")?;
        let is_initial_query = block.get::<u8, _>(row, "is_initial_query")? != 0;
        let timestamp_ns = block.get::<i64, _>(row, "timestamp_ns")? as u64;

        let name = if is_initial_query {
            format!("Init Query: {}, host: {}", query_id, hostname)
        } else {
            format!("Query: {}, host: {}", query_id, hostname)
        };
        let pid = block.get::<u32, _>(row, "pid")? as i32;
        
        let process_descriptor = ProcessDescriptor {
            pid: Some(pid),
            process_name: Some(name.clone()),
            cmdline: Vec::new(),
            process_priority: None,
            start_timestamp_ns: None,
            chrome_process_type: None,
            legacy_sort_index: None,
            process_labels: Vec::new(),
        };
        
        let track_descriptor = TrackDescriptor {
            uuid: Some(uuid),
            parent_uuid: None,
            process: Some(process_descriptor),
            chrome_process: None,
            thread: None,
            chrome_thread: None,
            counter: None,
            disallow_merging_with_system_tracks: None,
            child_ordering: None,
            sibling_order_rank: None,
            static_or_dynamic_name: Some(crate::generated::perfetto_protos::track_descriptor::StaticOrDynamicName::Name(name)),
        };

        let mut track_packet = self.create_trace_packet_base(
            machine_id,
            Some(timestamp_ns),
            1 as u32,
            trace_packet::Data::TrackDescriptor(track_descriptor),
            None
        );
        track_packet.sequence_flags = Some(1);
        
        Ok(track_packet)
    }
    
    fn create_counter_track_packet(&self, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, track_descriptor, CounterDescriptor, TrackDescriptor};
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let uuid = block.get::<u64, _>(row, "uuid")?;
        let parent_uuid: u64 = block.get::<u32, _>(row, "parent_uuid")? as u64;
        let name = block.get::<String, _>(row, "name")?;
        let unit = block.get::<String, _>(row, "unit")?;
        let unit_multiplier = block.get::<u16, _>(row, "unit_multiplier")? as i64;
        let timestamp_ns = block.get::<i64, _>(row, "timestamp_ns")? as u64;

        // Map string type to CounterDescriptor unit_name enum
        let counter_unit_name = match unit.as_str() {
            "UNIT_TIME_NS" => 1, // UNIT_TIME_NS
            "UNIT_COUNT" => 2,   // UNIT_COUNT
            "UNIT_SIZE_BYTES" => 3,   // UNIT_SIZE_BYTES  
            _ => 0, // UNIT_UNSPECIFIED
        };

        let counter_descriptor = CounterDescriptor {
            r#type: None,
            categories: Vec::new(),
            unit: Some(counter_unit_name),
            unit_name: None,
            unit_multiplier: Some(unit_multiplier),
            is_incremental: Some(true),
        };
        
        let track_descriptor = TrackDescriptor {
            uuid: Some(uuid),
            parent_uuid: Some(parent_uuid),
            process: None,
            chrome_process: None,
            thread: None,
            chrome_thread: None,
            counter: Some(counter_descriptor),
            disallow_merging_with_system_tracks: None,
            child_ordering: None,
            sibling_order_rank: None,
            static_or_dynamic_name: Some(track_descriptor::StaticOrDynamicName::Name(name)),
        };
        
        Ok(self.create_trace_packet_base(
            machine_id,
            Some(timestamp_ns),
            1 as u32,
            trace_packet::Data::TrackDescriptor(track_descriptor),
            None
        ))
    }
    
    fn create_counter_event_packet(&self, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, track_event};
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let timestamp_ns = block.get::<u64, _>(row, "timestamp_ns")?;
        let parent_uuid = block.get::<u32, _>(row, "parent_uuid")? as u64;
        let track_uuid = block.get::<u64, _>(row, "track_uuid")?;
        let counter_value = block.get::<i64, _>(row, "counter_value")?;
        
        let track_event = self.create_track_event_base(
            Some(track_uuid),
            Some(4), // TYPE_COUNTER = 4
            None,
            Some(track_event::CounterValueField::CounterValue(counter_value)),
            None,
            None
        );
        
        Ok(self.create_trace_packet_base(
            machine_id,
            Some(timestamp_ns),
            1 as u32,
            trace_packet::Data::TrackEvent(track_event),
            None
        ))
    }
    
    fn create_child_track_packet(&self, machine_id: u32, track_uuid: u64, parent_uuid: u64, timestamp: Option<u64>) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, TrackDescriptor};
        
        let track_descriptor = TrackDescriptor {
            uuid: Some(track_uuid),
            parent_uuid: Some(parent_uuid),
            process: None,
            chrome_process: None,
            thread: None,
            chrome_thread: None,
            counter: None,
            disallow_merging_with_system_tracks: None,
            child_ordering: None,
            sibling_order_rank: None,
            static_or_dynamic_name: Some(crate::generated::perfetto_protos::track_descriptor::StaticOrDynamicName::Name("Processors".to_string())),
        };
        
        Ok(self.create_trace_packet_base(
            machine_id,
            timestamp,
            1 as u32,
            trace_packet::Data::TrackDescriptor(track_descriptor),
            None
        ))
    }
    
    fn create_streaming_alloc_free_packet(&self, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{
            trace_packet, StreamingAllocation, StreamingFree
        };
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let track_uuid = block.get::<u32, _>(row, "track_uuid")?;
        let direction = block.get::<i16, _>(row, "direction")?;
        let clock_monotonic_coarse_timestamp  = block.get::<Vec<u64>, _>(row, "clock_monotonic_coarse_timestamp")?;
        let address  = block.get::<Vec<u64>, _>(row, "address")?;
        let size = block.get::<Vec<u64>, _>(row, "size_arr")?;
        let sequence_number  = block.get::<Vec<u64>, _>(row, "sequence_number")?;

        let track_packet;
        if direction > 0 {
            let streaming_packet = StreamingAllocation {
            address: address,
            size: size,
            sample_size: vec![0],
            clock_monotonic_coarse_timestamp: clock_monotonic_coarse_timestamp  ,
            heap_id: vec![0],
            sequence_number: sequence_number,
        };
            track_packet = self.create_trace_packet_base(
            machine_id,
            Some(0),
            1 as u32,
            trace_packet::Data::StreamingAllocation(streaming_packet),
            None
        );
        
        } else {
            let streaming_packet = StreamingFree {
            address: address,
            heap_id: vec![0],
            sequence_number: sequence_number,
        };
            track_packet = self.create_trace_packet_base(
            machine_id,
            Some(0),
            1 as u32,
            trace_packet::Data::StreamingFree(streaming_packet),
            None
        );
        };

        Ok(track_packet)
    }

    fn create_track_thread_packet(&self, block: &Columns, row: usize) -> Result<Vec<TracePacket>> {
        use crate::generated::perfetto_protos::{trace_packet, ThreadDescriptor, TrackDescriptor};
        
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        let uuid = block.get::<u32, _>(row, "uuid")? as u64;
        let counter_uuid = block.get::<u32, _>(row, "counter_uuid")? as u64;
        let parent_uuid = block.get::<u32, _>(row, "parent_uuid")? as u64;
        let pid = block.get::<u32, _>(row, "pid")? as i32;
        let tid = block.get::<u64, _>(row, "tid")? as i32;
        let timestamp_ns = block.get::<i64, _>(row, "timestamp_ns")? as u64;
        let name = format!("Thread: {}", tid);
        let counter_name = format!("Thread: {}, Counters", tid);        

        let thread_descriptor = ThreadDescriptor {
            pid: Some(pid),
            tid: Some(tid),
            thread_name: Some(tid.to_string()),
            reference_timestamp_us: None,
            reference_thread_time_us: None,
            reference_thread_instruction_count: None,
            chrome_thread_type: None,
            legacy_sort_index: None,
        };
        
        let track_descriptor = TrackDescriptor {
            uuid: Some(uuid),
            parent_uuid: Some(parent_uuid),
            process: None,
            chrome_process: None,
            thread: Some(thread_descriptor),
            chrome_thread: None,
            counter: None,
            disallow_merging_with_system_tracks: None,
            child_ordering: None,
            sibling_order_rank: None,
            static_or_dynamic_name: Some(crate::generated::perfetto_protos::track_descriptor::StaticOrDynamicName::Name(name)),
        };

        let counter_track_descriptor = TrackDescriptor {
            uuid: Some(counter_uuid),
            parent_uuid: Some(uuid),
            process: None,
            chrome_process: None,
            thread: None,
            chrome_thread: None,
            counter: None,
            disallow_merging_with_system_tracks: None,
            child_ordering: None,
            sibling_order_rank: None,
            static_or_dynamic_name: Some(crate::generated::perfetto_protos::track_descriptor::StaticOrDynamicName::Name(counter_name)),
        };
        
        let track_thread_packets = vec![
            self.create_trace_packet_base(
                machine_id,
                Some(timestamp_ns),
                uuid as u32,
                trace_packet::Data::TrackDescriptor(track_descriptor),
                None
            ),
            self.create_trace_packet_base(
                machine_id,
                Some(timestamp_ns),
                uuid as u32,
                trace_packet::Data::TrackDescriptor(counter_track_descriptor),
                None
            ),
        ];
        
        Ok(track_thread_packets)
    }

    /// Execute query with profiling enabled and return the initial query ID for Perfetto tracing
    pub async fn execute_with_profiling(&self, database: &str, query: &str) -> Result<String> {
        // Generate a unique query_id for this execution
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let log_comment = format!("chdig-{}", timestamp);

        // Execute the query with profiling settings enabled
        let profiling_settings_query = format!(r#"
SET query_profiler_real_time_period_ns = 10000000,
    query_profiler_cpu_time_period_ns = 10000000,
    memory_profiler_step = 1048576,
    memory_profiler_sample_probability = 0.1,
    trace_profile_events = 1,
    opentelemetry_start_trace_probability = 1,
    opentelemetry_trace_processors = 1,
    log_processors_profiles = 1,
    log_comment = '{}'"#, log_comment);

        // Execute the query with profiling settings enabled
        // Execute the query with profiling
        self.execute_simple(&profiling_settings_query).await?;
        self.execute_query(&database, &query).await?;
        
        // Flush logs to ensure all profiling data is written
        self.execute_simple("SYSTEM FLUSH LOGS").await?;
        
        // Return the log_comment which can be used with get_perfetto_query_ids
        Ok(log_comment)
    }

    /// Execute query with profiling and return all related query IDs for Perfetto tracing
    pub async fn execute_with_profiling_and_get_query_ids(&self, database: &str, query: &str) -> Result<(Vec<String>, u64, u64)> {
        let initial_query_id = self.execute_with_profiling(database, query).await?;
        self.get_perfetto_query_ids(&initial_query_id).await
    }

    /// Get query IDs for Perfetto trace generation
    pub async fn get_perfetto_query_ids(&self, initial_query_id: &str) -> Result<(Vec<String>, u64, u64)> {
        let query_log = self.get_table_name("system", "query_log");
        let block = self
            .execute(&format!(
                "SELECT groupUniqArray(query_id) as query_ids, toUnixTimestamp(min(event_time))::UInt64 as min_time, toUnixTimestamp(max(event_time))::UInt64 as max_time FROM {} WHERE log_comment = '{}' AND query_kind = 'Select' AND event_date = today()",
                query_log, initial_query_id
            ))
            .await?;
        
        let mut query_ids = Vec::new();
        query_ids.append(&mut block.get::<Vec<String>, _>( 0, "query_ids")?);

        let min_time = block.get::<u64, _>( 0, "min_time")?;
        let max_time = block.get::<u64, _>( 0, "max_time")?;

        if query_ids.is_empty() {
            return Err(anyhow::Error::msg("no queries found"));
        }
        
        Ok((query_ids, min_time, max_time))
    }

    /// Get Perfetto track query data as protobuf packets
    pub async fn get_perfetto_track_query(&self, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "query_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(query_id) AS uuid,
    murmurHash3_32(hostname) as machine_id,
    hostname,
    if(is_initial_query, 'Init', '') as init,
    murmurHash3_32(query_id) % 4194304 AS pid
SELECT 
    machine_id,
    uuid,
    pid,
    is_initial_query,
    query_id,
    hostname,
    toUnixTimestamp64Nano(min(event_time_microseconds) - INTERVAL 100 MICROSECONDS) as timestamp_ns
FROM {}
WHERE (query_id IN {}) AND event_date = today() AND event_time BETWEEN {} AND {}
GROUP BY machine_id, query_id, is_initial_query, uuid, pid, hostname"#, table_name, ids, start, end);
        
        let block = self.execute(&query).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_track_query_packet(&block, i)?);
        }
        
        Ok(results)
    }

    /// Get Perfetto track thread data as protobuf packets
    pub async fn get_perfetto_track_thread(&self, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "query_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(query_id) AS parent_uuid,
    murmurHash3_32(tid, query_id) AS uuid,
    murmurHash3_32(tid, query_id, 'Counter') AS counter_uuid,
    murmurHash3_32(hostname) as machine_id,
    murmurHash3_32(query_id) % 4194304 AS pid,
    arrayJoin(thread_ids) AS tid
SELECT 
    machine_id,
    uuid,
    parent_uuid,
    counter_uuid,
    pid,
    tid,
    query_id,
    toUnixTimestamp64Nano(min(event_time_microseconds) - INTERVAL 100 MICROSECONDS) as timestamp_ns
FROM {}
WHERE (query_id IN {}) AND event_date = today() AND event_time BETWEEN {} AND {}
GROUP BY tid, machine_id, query_id, uuid, parent_uuid, pid"#, table_name,  ids, start, end);
        
        let block = self.execute(&query).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.append(&mut self.create_track_thread_packet(&block, i)?);
        }
        
        Ok(results)
    }

    /// Get Perfetto track counter event data as protobuf packets
    pub async fn get_perfetto_track_counter_event_packets(&self, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = self.get_perfetto_track_counter_event(query_ids, start, end).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_counter_event_packet(&block, i)?);
        }
        
        Ok(results)
    }

    /// Get Perfetto track counter data as protobuf packets  
    pub async fn get_perfetto_track_counter_packets(&self, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = self.get_perfetto_track_counter(query_ids, start, end).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_counter_track_packet(&block, i)?);
        }
        
        Ok(results)
    }


    /// Get Perfetto streaming profile stack data as protobuf packets
    pub async fn get_perfetto_streaming_alloc_free_packets(&self, query_ids: &[String], start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = self.get_perfetto_streaming_alloc_free(query_ids, start, end).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_streaming_alloc_free_packet(&block, i)?);
        }
        
        Ok(results)
    }

    /// Get Perfetto track counter data
    pub async fn get_perfetto_track_counter(&self, query_ids: &[String], start: u64, end: u64) -> Result<Columns> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "trace_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(hostname) as machine_id,
    murmurHash3_32(thread_id, query_id, 'Counter') AS parent_uuid,
    extractAll(event, 'Lock|Microseconds|Bytes|Network|IO|Read|Write|Wait|OS|Log|ThreadPool|ConcurrencyControl|Arena|Page|Fault|Selected|Execute') AS categories,
    bitShiftLeft(CAST(parent_uuid, 'UInt64'), 32) + murmurHash3_32(event) AS uuid,
    multiIf(event LIKE '%seconds%', 'UNIT_TIME_NS', event LIKE '%Bytes%', 'UNIT_SIZE_BYTES', 'UNIT_COUNT') AS unit,
    if(event LIKE '%Microseconds%', 1000, 0) AS unit_multiplier,
    event AS name,
    true AS is_incremental
SELECT 
    machine_id,
    uuid,
    parent_uuid,
    name,
    categories,
    unit,
    unit_multiplier,
    is_incremental,
    thread_id,
    query_id,
    event,
    toUnixTimestamp64Nano(min(event_time_microseconds) - INTERVAL 100 MICROSECONDS) AS timestamp_ns
FROM {}
WHERE (query_id IN {}) AND (trace_type = 'ProfileEvent') AND event_date = today() AND event_time BETWEEN {} AND {}
GROUP BY
    thread_id,
    query_id,
    event,
    machine_id,
    uuid,
    parent_uuid,
    name,
    categories,
    unit,
    unit_multiplier,
    is_incremental"#, table_name, ids, start, end);
        
        self.execute(&query).await
    }

    /// Get Perfetto track counter event data
    pub async fn get_perfetto_track_counter_event(&self, query_ids: &[String], start: u64, end: u64) -> Result<Columns> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "trace_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(hostname) as machine_id,
    murmurHash3_32(thread_id, query_id, 'Counter') AS parent_uuid,
    bitShiftLeft(CAST(parent_uuid, 'UInt64'), 32) + murmurHash3_32(event) AS track_uuid,
    'TYPE_COUNTER' AS type,
    increment AS counter_value
SELECT 
    machine_id,
    timestamp_ns,
    type,
    track_uuid,
    parent_uuid,
    counter_value,
    thread_id,
    query_id,
    event
FROM {}
WHERE (query_id IN {}) AND (trace_type = 'ProfileEvent') AND (increment != 0) AND event_date = today() AND event_time BETWEEN {} AND {}
ORDER BY timestamp_ns ASC"#, table_name, ids, start, end);
        
        self.execute(&query).await
    }

    /// Get Perfetto processors events data
    pub async fn get_perfetto_processors_events(&self, query_ids: &[String], start: u64, end: u64) -> Result<Columns> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "opentelemetry_span_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(hostname) as machine_id,
    {} * 1_000_000 AS start,
    {} * 1_000_000 AS end,
    (
        SELECT groupUniqArray((trace_id, attribute['clickhouse.query_id']))
        FROM {}
        WHERE (attribute['clickhouse.query_id']) IN {} AND start_time_us BETWEEN start AND end
    ) AS my_trace_id,
    transform(trace_id, my_trace_id.1, my_trace_id.2, '') as query_id,
    span_id as uuid,
    murmurHash3_32(operation_name) as name_iid,
    --murmurHash3_32(query_id) AS parent_uuid,
    murmurHash3_32(toUInt64OrZero(attribute['clickhouse.thread_id']), query_id) AS parent_uuid,
    murmurHash3_32(toUInt64OrZero(attribute['clickhouse.thread_id']), query_id, 'Processors') AS track_uuid,
    [start_time_us, finish_time_us] AS timestamp_arr
SELECT 
    machine_id,
    track_uuid,
    parent_uuid,
    uuid,
    name_iid,
    timestamp * 1000 AS timestamp_ns,
    operation_name,
    type,
    toUInt64(toUInt32OrZero(attribute['execution_time_ms'])) * 1000 AS thread_time_absolute_us,
    trace_id,
    query_id
FROM {}
ARRAY JOIN
    timestamp_arr AS timestamp,
    ['TYPE_SLICE_BEGIN', 'TYPE_SLICE_END'] AS type
WHERE trace_id IN my_trace_id.1 AND start_time_us BETWEEN start AND end
ORDER BY timestamp ASC
SETTINGS allow_experimental_analyzer=1"#,start, end, table_name, ids, table_name);
        
        self.execute(&query).await
    }

    /// Get Perfetto streaming profile heap data
    pub async fn get_perfetto_streaming_alloc_free(&self, query_ids: &[String], start: u64, end: u64) -> Result<Columns> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "trace_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(thread_id, query_id) AS track_uuid,
    murmurHash3_32(hostname) as machine_id,
    groupArray((timestamp_ns, ptr, abs(size)::UInt64)) as changes,
    arraySort(x-> x.1 ,changes) AS sorted
SELECT
    if(size >= 0, 1, -1) as direction, 
    machine_id,
    track_uuid,
    sorted.1 as address,
    sorted.2 as clock_monotonic_coarse_timestamp,
    sorted.3 AS size_arr,
    arrayEnumerate(address)::Array(UInt64) AS sequence_number
FROM {}
WHERE (query_id IN {}) AND (trace_type = 'MemorySample') AND event_date = today() AND event_time BETWEEN {} AND {}
GROUP BY machine_id, track_uuid, direction
SETTINGS allow_experimental_analyzer = 1"#, table_name,  ids, start, end);
        
        self.execute(&query).await
    }

    /// Get Perfetto streaming profile stack data
    pub async fn get_perfetto_streaming_profile_stack(&self, query_ids: &[String], start: u64, end: u64) -> Result<Columns> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "trace_log");
        
        let query = format!(r#"WITH
    murmurHash3_32(thread_id, query_id) AS track_uuid,
    murmurHash3_32(hostname) as machine_id,
    min(timestamp_ns::Int64) AS timestamp_start_ns,
    arrayMap(x -> round(x/1000), arrayDifference(arraySort(groupArray(timestamp_ns))))::Array(Int64) AS timestamp_delta_us,
    any(reverse(trace)) AS callstack,
    arrayMap(frame_id -> addressToSymbol(frame_id), callstack) AS frame
SELECT 
    machine_id,
    track_uuid,
    timestamp_start_ns,
    timestamp_delta_us,
    callstack,
    frame,
    thread_id,
    query_id
FROM {}
WHERE (query_id IN {}) AND (trace_type = 'Real') AND event_date = today() AND event_time BETWEEN {} AND {}
GROUP BY
    thread_id,
    query_id,
    machine_id,
    track_uuid
ORDER BY timestamp_start_ns ASC
SETTINGS allow_introspection_functions = 1, allow_experimental_analyzer = 1"#, table_name,  ids, start, end);
        
        self.execute(&query).await
    }

    /// Get Perfetto query logs data
    pub async fn get_perfetto_query_logs(&self, query_ids: &[String], start: u64, end: u64) -> Result<Columns> {
        let ids = Self::format_query_ids(query_ids);
        let table_name = self.get_table_name_no_history("system", "text_log");
        
        let query = format!(r#"WITH
    transform(level, [1, 2, 3, 4, 5, 6, 7, 8, 9], [7, 7, 6, 5, 4, 4, 3, 2, 2], 0)::Int32 AS prio,
    murmurHash3_32(thread_id, query_id) AS track_uuid,
    splitByChar(';', source_file)[1] AS file_name,
    splitByChar(';', source_file)[2] AS func_name,
    source_line AS line_number,
    murmurHash3_32(hostname) AS machine_id
SELECT 
    machine_id,
    toUnixTimestamp64Nano(event_time_microseconds) AS timestamp_ns,
    track_uuid,
    prio,
    message,
    file_name,
    func_name,
    line_number,
    thread_id,
    query_id,
    level,
    source_file,
    event_time_microseconds
FROM {}
WHERE query_id IN {} AND event_date = today() AND event_time BETWEEN {} AND {}"#, table_name, ids, start, end);
        
        self.execute(&query).await
    }

    /// Get system metrics data for SysStats packets from asynchronous_metric_log
    pub async fn get_perfetto_sys_stats_data(&self, start: u64, end: u64) -> Result<Columns> {
        let table_name = self.get_table_name_no_history("system", "asynchronous_metric_log");
        
        // TODO: Add your SQL query here to query system.asynchronous_metric_log
        let query = format!(r#"WITH
    if(metric NOT LIKE 'jemalloc', splitByChar('_', metric)[1], metric) AS metric_group,
    transform(metric_group, ['BlockReadBytes', 'BlockReadTime', 'BlockWriteBytes', 'BlockWriteTime', 'BlockDiscardBytes', 'BlockDiscardTime'], [0, 1, 2, 3, 4, 5], 10) AS disk_metric,
    if(metric NOT LIKE 'jemalloc', splitByChar('_', metric)[2], metric) AS name,
    groupArrayMapIf(map(name, (disk_metric, value)), (disk_metric < 10) AND (value > 0)) AS disk_stat_map,
    mapKeys(disk_stat_map) as disk_name,
    murmurHash3_32(hostname) as machine_id,
    mapValues(disk_stat_map) as disk_stats,
    flatten(arrayMap(x-> arrayFilter(y-> y.1 = 0, x).2, disk_stats)) as disk_stat_read_bytes,
    flatten(arrayMap(x-> arrayFilter(y-> y.1 = 1, x).2, disk_stats)) as disk_stat_read_time,
    flatten(arrayMap(x-> arrayFilter(y-> y.1 = 2, x).2, disk_stats)) as disk_stat_write_bytes,
    flatten(arrayMap(x-> arrayFilter(y-> y.1 = 3, x).2, disk_stats)) as disk_stat_write_time,
    flatten(arrayMap(x-> arrayFilter(y-> y.1 = 4, x).2, disk_stats)) as disk_stat_discard_bytes,
    flatten(arrayMap(x-> arrayFilter(y-> y.1 = 5, x).2, disk_stats)) as disk_stat_discard_time
SELECT
    machine_id,
    toUnixTimestamp(event_time) * 1000000000 AS timestamp_ns,
    groupArrayInsertAtIf(value, toUInt16OrZero(name), metric_group = 'CPUFrequencyMHz') AS cpu_freq_mhz,
    disk_name,
    disk_stat_read_bytes,
    disk_stat_read_time,
    disk_stat_write_bytes,
    disk_stat_write_time,
    disk_stat_discard_bytes,
    disk_stat_discard_time,
    anyIf(value, metric = 'OSUserTimeNormalized') as user_ns,
    anyIf(value, metric = 'OSSystemTimeNormalized') as system_mode_ns,
    anyIf(value, metric = 'OSIdleTimeNormalized') as idle_ns,
    anyIf(value, metric = 'OSIOWaitTimeNormalized') as io_wait_ns,
    anyIf(value, metric = 'OSIrqTimeNormalized') as irq_ns,
    anyIf(value, metric = 'OSSoftIrqTimeNormalized') as softirq_ns,
    anyIf(value, metric = 'OSStealTimeNormalized') as steal_ns,
    (anyIf(value, metric = 'OSMemoryTotal')/ 1024)::UInt64 as mem_total,
    (anyIf(value, metric = 'OSMemoryFreeWithoutCached')/ 1024)::UInt64  as mem_free,
    (anyIf(value, metric = 'OSMemoryAvailable')/ 1024)::UInt64 as mem_available,
    (anyIf(value, metric = 'OSMemoryBuffers')/ 1024)::UInt64 as mem_buffers,
    (anyIf(value, metric = 'OSMemoryCached')/ 1024)::UInt64 as mem_cached
FROM {}
WHERE event_date = today() AND event_time BETWEEN {} AND {}
GROUP BY machine_id, event_time
ORDER BY machine_id, event_time ASC"#, table_name, start, end);
        
        self.execute(&query).await
    }
    
    /// Create SysStats packet from system metrics data
    fn create_sys_stats_packet(&self, block: &Columns, row: usize) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, sys_stats, SysStats, MeminfoCounters};
        
        // Extract basic data
        let timestamp_ns = block.get::<u64, _>(row, "timestamp_ns")?;
        let machine_id = block.get::<u32, _>(row, "machine_id")?;
        
        // Extract memory info
        let mut meminfo = Vec::new();
        if let Ok(mem_total) = block.get::<u64, _>(row, "mem_total") {
            if mem_total > 0 {
                meminfo.push(sys_stats::MeminfoValue {
                    key: Some(MeminfoCounters::MeminfoMemTotal as i32),
                    value: Some(mem_total),
                });
            }
        }
        if let Ok(mem_free) = block.get::<u64, _>(row, "mem_free") {
            if mem_free > 0 {
                meminfo.push(sys_stats::MeminfoValue {
                    key: Some(MeminfoCounters::MeminfoMemFree as i32),
                    value: Some(mem_free),
                });
            }
        }
        if let Ok(mem_available) = block.get::<u64, _>(row, "mem_available") {
            if mem_available > 0 {
                meminfo.push(sys_stats::MeminfoValue {
                    key: Some(MeminfoCounters::MeminfoMemAvailable as i32),
                    value: Some(mem_available),
                });
            }
        }
        if let Ok(mem_buffers) = block.get::<u64, _>(row, "mem_buffers") {
            if mem_buffers > 0 {
                meminfo.push(sys_stats::MeminfoValue {
                    key: Some(MeminfoCounters::MeminfoBuffers as i32),
                    value: Some(mem_buffers),
                });
            }
        }
        if let Ok(mem_cached) = block.get::<u64, _>(row, "mem_cached") {
            if mem_cached > 0 {
                meminfo.push(sys_stats::MeminfoValue {
                    key: Some(MeminfoCounters::MeminfoCached as i32),
                    value: Some(mem_cached),
                });
            }
        }
        
        // Extract CPU stats - create single CPU entry with aggregate times
        let mut cpu_stat = Vec::new();
        let user_ns = block.get::<f64, _>(row, "user_ns").unwrap_or(0.0) as u64;
        let system_mode_ns = block.get::<f64, _>(row, "system_mode_ns").unwrap_or(0.0) as u64;
        let idle_ns = block.get::<f64, _>(row, "idle_ns").unwrap_or(0.0) as u64;
        let io_wait_ns = block.get::<f64, _>(row, "io_wait_ns").unwrap_or(0.0) as u64;
        let irq_ns = block.get::<f64, _>(row, "irq_ns").unwrap_or(0.0) as u64;
        let softirq_ns: u64 = block.get::<f64, _>(row, "softirq_ns").unwrap_or(0.0) as u64;
        let steal_ns: u64 = block.get::<f64, _>(row, "steal_ns").unwrap_or(0.0) as u64;

        if user_ns > 0 || system_mode_ns > 0 || idle_ns > 0 {
            cpu_stat.push(sys_stats::CpuTimes {
                cpu_id: Some(0), // Aggregate CPU stats
                user_ns: Some(user_ns),
                user_nice_ns: Some(0),
                system_mode_ns: Some(system_mode_ns),
                idle_ns: Some(idle_ns),
                io_wait_ns: Some(io_wait_ns),
                irq_ns: Some(irq_ns),
                softirq_ns: Some(softirq_ns),
                steal_ns: Some(steal_ns),
            });
        }
        
        // Extract CPU frequencies
        let mut cpufreq_khz = Vec::new();
        if let Ok(cpu_freq_mhz) = block.get::<Vec<f64>, _>(row, "cpu_freq_mhz") {
            cpufreq_khz = cpu_freq_mhz.iter().map(|&freq| (freq * 1000.0) as u32).collect();
        }
        
        // Extract disk stats
        let mut disk_stat = Vec::new();
        if let Ok(disk_names) = block.get::<Vec<String>, _>(row, "disk_name") {
            let read_bytes = block.get::<Vec<u64>, _>(row, "disk_stat_read_bytes").unwrap_or_default();
            let read_time = block.get::<Vec<u64>, _>(row, "disk_stat_read_time").unwrap_or_default();
            let write_bytes = block.get::<Vec<u64>, _>(row, "disk_stat_write_bytes").unwrap_or_default();
            let write_time = block.get::<Vec<u64>, _>(row, "disk_stat_write_time").unwrap_or_default();
            let discard_bytes = block.get::<Vec<u64>, _>(row, "disk_stat_discard_bytes").unwrap_or_default();
            let discard_time = block.get::<Vec<u64>, _>(row, "disk_stat_discard_time").unwrap_or_default();
            
            for (i, disk_name) in disk_names.iter().enumerate() {
                disk_stat.push(sys_stats::DiskStat {
                    device_name: Some(disk_name.clone()),
                    read_sectors: read_bytes.get(i).copied(),
                    read_time_ms: read_time.get(i).copied(),
                    write_sectors: write_bytes.get(i).copied(),
                    write_time_ms: write_time.get(i).copied(),
                    discard_sectors: discard_bytes.get(i).copied(),
                    discard_time_ms: discard_time.get(i).copied(),
                    flush_count: None,
                    flush_time_ms: None,
                });
            }
        }
        
        let sys_stats = SysStats {
            meminfo,
            vmstat: Vec::new(),
            cpu_stat,
            num_forks: None,
            num_irq_total: None,
            num_irq: Vec::new(),
            num_softirq_total: None,
            num_softirq: Vec::new(),
            collection_end_timestamp: Some(timestamp_ns),
            devfreq: Vec::new(),
            cpufreq_khz,
            buddy_info: Vec::new(),
            disk_stat,
            psi: Vec::new(),
            thermal_zone: Vec::new(),
            cpuidle_state: Vec::new(),
            gpufreq_mhz: Vec::new(),
        };
        
        Ok(self.create_trace_packet_base(
            machine_id,
            Some(timestamp_ns),
            1 as u32,
            trace_packet::Data::SysStats(sys_stats),
            None
        ))
    }
    
    /// Get SysStats packets for Perfetto trace
    pub async fn get_perfetto_sys_stats_packets(&self, start: u64, end: u64) -> Result<Vec<TracePacket>> {
        let block = self.get_perfetto_sys_stats_data(start, end).await?;
        let mut results = Vec::new();
        
        for i in 0..block.row_count() {
            results.push(self.create_sys_stats_packet(&block, i)?);
        }
        
        Ok(results)
    }

    pub fn get_perfetto_clock_snapshot(&self) -> Result<TracePacket> {
        use crate::generated::perfetto_protos::{trace_packet, ClockSnapshot};
        use crate::generated::perfetto_protos::clock_snapshot::Clock;
        
        // Create clock snapshot with realtime clock
        let clock_snapshot = ClockSnapshot {
            clocks: vec![
                Clock {
                    clock_id: Some(CLOCK_ID_UNIXTIME), // CUSTOM clock ID
                    timestamp: Some(0),
                    unit_multiplier_ns: Some(1), // Already in nanoseconds
                    is_incremental: Some(false),
                },
                Clock {
                    clock_id: Some(6), // BOOTTIME clock ID
                    timestamp: Some(0),
                    unit_multiplier_ns: Some(1), // Already in nanoseconds
                    is_incremental: Some(false),
                }
            ],
            primary_trace_clock: None, // Use BOOTTIME as primary
        };
        
        let mut trace_packet = self.create_trace_packet_base(
            3547214653, // No specific machine_id for clock snapshot
            Some(0), // Clock snapshot at time 0
            1 as u32,
            trace_packet::Data::ClockSnapshot(clock_snapshot),
            None
        );

        trace_packet.timestamp_clock_id = Some(CLOCK_ID_UNIXTIME);
        Ok(trace_packet)
    }

    pub async fn generate_perfetto_trace_pb(
        &self,
        database: &str,
        query: &str,
        _output: &str,
    ) -> Result<Vec<u8>> {
        use prost::Message;
        
        let (query_ids, start_time, end_time) = self.execute_with_profiling_and_get_query_ids(database, query).await?;
        
        // Create PerfettoTraceBuilder with global interned data management
        let trace_builder = PerfettoTraceBuilder::new();
        
        // Collect all trace packets in parallel using the single trace_builder instance
        let (
            track_query_packets,
            track_thread_packets,
            counter_packets,
            counter_event_packets,
            processor_event_packets,
            streaming_profile_packets,
            query_logs_packets,
            sys_stats_packets,
            //streaming_alloc_free_packets,
        ) = tokio::try_join!(
            self.get_perfetto_track_query(&query_ids, start_time, end_time),
            self.get_perfetto_track_thread(&query_ids, start_time, end_time),
            self.get_perfetto_track_counter_packets(&query_ids, start_time, end_time),
            self.get_perfetto_track_counter_event_packets(&query_ids, start_time, end_time),
            trace_builder.get_perfetto_processors_events_packets(self, &query_ids, start_time, end_time),
            trace_builder.get_perfetto_streaming_profile_packets(self, &query_ids, start_time, end_time),
            trace_builder.get_perfetto_query_logs_packets(self, &query_ids, start_time, end_time),
            self.get_perfetto_sys_stats_packets(start_time, end_time),
            //self.get_perfetto_streaming_alloc_free_packets(&query_ids, start_time, end_time),
        )?;
        
        let mut all_packets = Vec::new();

        
        all_packets.extend(track_query_packets);
        all_packets.extend(track_thread_packets);
        all_packets.extend(counter_packets);
        all_packets.extend(counter_event_packets);
        all_packets.extend(processor_event_packets);
        all_packets.extend(streaming_profile_packets);
        all_packets.extend(query_logs_packets);
        all_packets.extend(sys_stats_packets);
        //all_packets.extend(streaming_alloc_free_packets);

        // Sort all packets by timestamp ascending
        all_packets.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        all_packets.insert(0, self.get_perfetto_clock_snapshot()?);

        // Add clock snapshot at the beginning
        // Create the main Trace object
        let trace = Trace {
            packet: all_packets,
        };
        
        // Serialize to protobuf binary format
        let mut buf = Vec::new();
        trace.encode(&mut buf)?;      
        Ok(buf)
    }
}