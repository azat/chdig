use chrono::DateTime;
use chrono_tz::Tz;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct QueryProcess {
    pub host_name: String,
    pub user: String,
    pub threads: usize,
    pub memory: i64,
    pub elapsed: f64,
    pub query_start_time_microseconds: DateTime<Tz>,
    // Is the name good enough? Maybe simply "queries" or "shards_queries"?
    pub subqueries: u64,
    pub is_initial_query: bool,
    pub initial_query_id: String,
    pub query_id: String,
    pub normalized_query: String,
    pub original_query: String,
    pub current_database: String,

    pub profile_events: HashMap<String, u64>,

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
        let network_events = [
            "NetworkSendBytes",
            "NetworkReceiveBytes",
            "ReadBufferFromS3Bytes",
            "WriteBufferFromS3Bytes",
        ];

        if !self.running {
            return self.get_profile_events_multi(&network_events) as f64;
        }

        if self.prev_profile_events.is_some() {
            let net_now = self.get_profile_events_multi(&network_events);
            let net_prev = self.get_prev_profile_events_multi(&network_events);
            let net_diff = net_now.saturating_sub(net_prev);

            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            if elapsed > 0. {
                return (net_diff as f64) / elapsed;
            }
        }

        let net = self.get_profile_events_multi(&network_events);
        return net as f64 / self.elapsed;
    }

    pub fn disk_io(&self) -> f64 {
        let disk_events = [
            "WriteBufferFromFileDescriptorWriteBytes",
            "ReadBufferFromFileDescriptorReadBytes",
        ];

        if !self.running {
            return self.get_profile_events_multi(&disk_events) as f64;
        }

        if self.prev_profile_events.is_some() {
            let disk_now = self.get_profile_events_multi(&disk_events);
            let disk_prev = self.get_prev_profile_events_multi(&disk_events);
            let disk_diff = disk_now.saturating_sub(disk_prev);

            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            if elapsed > 0. {
                return (disk_diff as f64) / elapsed;
            }
        }

        let disk = self.get_profile_events_multi(&disk_events);
        return disk as f64 / self.elapsed;
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
}
