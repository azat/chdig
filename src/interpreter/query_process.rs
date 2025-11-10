use chrono::{DateTime, Local};
use size::{Base, SizeFormatter, Style};
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug)]
pub struct QueryProcess {
    pub selection: bool,
    pub host_name: String,
    pub user: String,
    pub threads: usize,
    pub memory: i64,
    pub elapsed: f64,
    pub query_start_time_microseconds: DateTime<Local>,
    pub query_end_time_microseconds: DateTime<Local>,
    // Is the name good enough? Maybe simply "queries" or "shards_queries"?
    pub subqueries: u64,
    pub is_initial_query: bool,
    pub initial_query_id: String,
    pub query_id: String,
    pub normalized_query: String,
    pub original_query: String,
    pub current_database: String,

    pub profile_events: HashMap<String, u64>,
    pub settings: HashMap<String, String>,

    // Used for metric rates (like top(1) shows)
    pub prev_elapsed: Option<f64>,
    pub prev_profile_events: Option<HashMap<String, u64>>,

    // If running is true, then the metrics will be shown as per-second rate, otherwise raw data.
    // Since for system.processes we indeed the rates, while for slow queries/last queries raw
    // data.
    pub running: bool,
}
impl QueryProcess {
    // NOTE: maybe it should be corrected with moving sampling?
    pub fn cpu(&self) -> f64 {
        if !self.running {
            let ms = *self
                .profile_events
                .get("OSCPUVirtualTimeMicroseconds")
                .unwrap_or(&0);
            return (ms as f64) / 1e6 * 100.;
        }

        if let Some(prev_profile_events) = &self.prev_profile_events {
            let ms_prev = *prev_profile_events
                .get("OSCPUVirtualTimeMicroseconds")
                .unwrap_or(&0);
            let ms_now = *self
                .profile_events
                .get("OSCPUVirtualTimeMicroseconds")
                .unwrap_or(&0);
            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            if elapsed > 0. {
                // It is possible to overflow, at least because metrics for initial queries is
                // summarized, and when query on some node will be finished (non initial), then initial
                // query will have less data.
                return ms_now.saturating_sub(ms_prev) as f64 / 1e6 / elapsed * 100.;
            }
        }

        let ms = *self
            .profile_events
            .get("OSCPUVirtualTimeMicroseconds")
            .unwrap_or(&0);
        return (ms as f64) / 1e6 / self.elapsed * 100.;
    }

    pub fn io_wait(&self) -> f64 {
        if !self.running {
            let ms = *self
                .profile_events
                .get("OSIOWaitMicroseconds")
                .unwrap_or(&0);
            return (ms as f64) / 1e6 * 100.;
        }

        if let Some(prev_profile_events) = &self.prev_profile_events {
            let ms_prev = *prev_profile_events
                .get("OSIOWaitMicroseconds")
                .unwrap_or(&0);
            let ms_now = *self
                .profile_events
                .get("OSIOWaitMicroseconds")
                .unwrap_or(&0);
            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            if elapsed > 0. {
                // It is possible to overflow, at least because metrics for initial queries is
                // summarized, and when query on some node will be finished (non initial), then initial
                // query will have less data.
                return ms_now.saturating_sub(ms_prev) as f64 / 1e6 / elapsed * 100.;
            }
        }

        let ms = *self
            .profile_events
            .get("OSIOWaitMicroseconds")
            .unwrap_or(&0);
        return (ms as f64) / 1e6 / self.elapsed * 100.;
    }

    pub fn cpu_wait(&self) -> f64 {
        if !self.running {
            let ms = *self
                .profile_events
                .get("OSCPUWaitMicroseconds")
                .unwrap_or(&0);
            return (ms as f64) / 1e6 * 100.;
        }

        if let Some(prev_profile_events) = &self.prev_profile_events {
            let ms_prev = *prev_profile_events
                .get("OSCPUWaitMicroseconds")
                .unwrap_or(&0);
            let ms_now = *self
                .profile_events
                .get("OSCPUWaitMicroseconds")
                .unwrap_or(&0);
            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            if elapsed > 0. {
                // It is possible to overflow, at least because metrics for initial queries is
                // summarized, and when query on some node will be finished (non initial), then initial
                // query will have less data.
                return ms_now.saturating_sub(ms_prev) as f64 / 1e6 / elapsed * 100.;
            }
        }

        let ms = *self
            .profile_events
            .get("OSCPUWaitMicroseconds")
            .unwrap_or(&0);
        return (ms as f64) / 1e6 / self.elapsed * 100.;
    }

    pub fn net_io(&self) -> f64 {
        return self.get_per_second_rate_events_multi(&[
            "NetworkSendBytes",
            "NetworkReceiveBytes",
            "ReadBufferFromS3Bytes",
            "WriteBufferFromS3Bytes",
        ]);
    }

    pub fn disk_io(&self) -> f64 {
        return self.get_per_second_rate_events_multi(&[
            "WriteBufferFromFileDescriptorWriteBytes",
            // Note that it may differs from ReadCompressedBytes, since later takes into account
            // network.
            "ReadBufferFromFileDescriptorReadBytes",
        ]);
    }

    pub fn io(&self) -> f64 {
        return self.get_per_second_rate_events_multi(&[
            // Though sometimes it is bigger the the real uncompressed reads, so maybe it is better
            // to use CompressedReadBufferBytes instead.
            // But yes it will not take into account non-compressed reads, but this should be rare
            // (except for the cases when the MergeTree is used with CODEC NONE).
            "SelectedBytes",
            "InsertedBytes",
        ]);
    }

    fn get_profile_events_multi(&self, names: &[&'static str]) -> u64 {
        let mut result: u64 = 0;
        for &name in names {
            result += *self.profile_events.get(name).unwrap_or(&0);
        }
        return result;
    }
    fn get_prev_profile_events_multi(&self, names: &[&'static str]) -> u64 {
        let mut result: u64 = 0;
        for &name in names {
            result += *self
                .prev_profile_events
                .as_ref()
                .unwrap()
                .get(name)
                .unwrap_or(&0);
        }
        return result;
    }

    fn get_per_second_rate_events_multi(&self, events: &[&'static str]) -> f64 {
        if !self.running {
            return self.get_profile_events_multi(events) as f64;
        }

        if self.prev_profile_events.is_some() {
            let now = self.get_profile_events_multi(events);
            let prev = self.get_prev_profile_events_multi(events);
            let diff = now.saturating_sub(prev);

            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            if elapsed > 0. {
                return (diff as f64) / elapsed;
            }
        }

        let value = self.get_profile_events_multi(events);
        return value as f64 / self.elapsed;
    }
}

impl fmt::Display for QueryProcess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatter = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

        let memory_str = formatter.format(self.memory);
        let status = if self.running { "Running" } else { "Finished" };

        writeln!(f, "Query ID:         {}", self.query_id)?;
        writeln!(f, "Initial Query ID: {}", self.initial_query_id)?;
        writeln!(f, "Status:           {}", status)?;
        writeln!(f, "Is Initial:       {}", self.is_initial_query)?;
        writeln!(f, "Subqueries:       {}", self.subqueries)?;
        writeln!(f, "Host:             {}", self.host_name)?;
        writeln!(f, "User:             {}", self.user)?;
        writeln!(f, "Database:         {}", self.current_database)?;
        writeln!(f, "Threads:          {}", self.threads)?;
        writeln!(f, "Memory:           {}", memory_str)?;
        writeln!(f, "Elapsed:          {:.2}s", self.elapsed)?;
        writeln!(f, "CPU:              {:.1}%", self.cpu())?;
        writeln!(f, "IO Wait:          {:.1}%", self.io_wait())?;
        writeln!(f, "CPU Wait:         {:.1}%", self.cpu_wait())?;
        writeln!(
            f,
            "Start Time:       {}",
            self.query_start_time_microseconds
                .format("%Y-%m-%d %H:%M:%S")
        )?;
        writeln!(
            f,
            "End Time:         {}",
            self.query_end_time_microseconds.format("%Y-%m-%d %H:%M:%S")
        )?;
        writeln!(f, "Query:")?;
        write!(f, "{}", self.original_query)
    }
}
