use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct QueryProcess {
    pub host_name: String,
    pub user: String,
    pub threads: usize,
    pub memory: i64,
    // NOTE: there are some issues with elapsed in system.processes [1]
    //
    //   [1]: https://github.com/ClickHouse/ClickHouse/pull/46047
    pub elapsed: f64,
    // Is the name good enough? Maybe simply "queries" or "shards_queries"?
    pub subqueries: u64,
    pub is_initial_query: bool,
    pub initial_query_id: String,
    pub query_id: String,
    pub normalized_query: String,
    pub original_query: String,

    pub profile_events: HashMap<String, u64>,

    // Used for metric rates (like top(1) shows)
    pub prev_elapsed: Option<f64>,
    pub prev_profile_events: Option<HashMap<String, u64>>,
}
impl QueryProcess {
    // NOTE: maybe it should be corrected with moving sampling?
    pub fn cpu(&self) -> f64 {
        if let Some(prev_profile_events) = &self.prev_profile_events {
            let ms_prev = *prev_profile_events
                .get("OSCPUVirtualTimeMicroseconds")
                .unwrap_or(&0);
            let ms_now = *self
                .profile_events
                .get("OSCPUVirtualTimeMicroseconds")
                .unwrap_or(&0);
            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            // It is possible to overflow, at least because metrics for initial queries is
            // summarized, and when query on some node will be finished (non initial), then initial
            // query will have less data.
            return ms_now.saturating_sub(ms_prev) as f64 / 1e6 / elapsed * 100.;
        }

        let ms = *self
            .profile_events
            .get("OSCPUVirtualTimeMicroseconds")
            .unwrap_or(&0);
        return (ms as f64) / 1e6 / self.elapsed * 100.;
    }

    pub fn net_io(&self) -> f64 {
        if let Some(prev_profile_events) = &self.prev_profile_events {
            let in_prev = *prev_profile_events.get("NetworkReceiveBytes").unwrap_or(&0);
            let in_now = *self.profile_events.get("NetworkReceiveBytes").unwrap_or(&0);

            let out_prev = *prev_profile_events.get("NetworkSendBytes").unwrap_or(&0);
            let out_now = *self.profile_events.get("NetworkSendBytes").unwrap_or(&0);

            let in_diff = in_now.saturating_sub(in_prev);
            let out_diff = out_now.saturating_sub(out_prev);

            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            return ((in_diff + out_diff) as f64) / elapsed;
        }

        let net_in = *self.profile_events.get("NetworkReceiveBytes").unwrap_or(&0);
        let net_out = *self.profile_events.get("NetworkSendBytes").unwrap_or(&0);
        return (net_in + net_out) as f64 / self.elapsed;
    }

    pub fn disk_io(&self) -> f64 {
        if let Some(prev_profile_events) = &self.prev_profile_events {
            let prev = *prev_profile_events
                .get("ReadBufferFromFileDescriptorReadBytes")
                .unwrap_or(&0);
            let now = *self
                .profile_events
                .get("ReadBufferFromFileDescriptorReadBytes")
                .unwrap_or(&0);

            let elapsed = self.elapsed - self.prev_elapsed.unwrap();
            return now.saturating_sub(prev) as f64 / elapsed;
        }

        let now = *self
            .profile_events
            .get("ReadBufferFromFileDescriptorReadBytes")
            .unwrap_or(&0);
        return now as f64 / self.elapsed;
    }
}
