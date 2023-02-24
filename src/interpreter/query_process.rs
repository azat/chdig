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
    pub has_initial_query: bool,
    pub is_initial_query: bool,
    pub initial_query_id: String,
    pub query_id: String,
    pub normalized_query: String,
    pub original_query: String,

    pub profile_events: HashMap<String, u64>,
}
impl QueryProcess {
    pub fn cpu(&self) -> f64 {
        let ms = *self
            .profile_events
            .get("OSCPUVirtualTimeMicroseconds")
            .unwrap_or(&0);
        return (ms as f64) / 1e6 / self.elapsed * 100.;
    }
    pub fn net_io(&self) -> u64 {
        let net_in = *self.profile_events.get("NetworkReceiveBytes").unwrap_or(&0);
        let net_out = *self.profile_events.get("NetworkSendBytes").unwrap_or(&0);
        return net_in + net_out;
    }
    pub fn disk_io(&self) -> u64 {
        return *self
            .profile_events
            .get("ReadBufferFromFileDescriptorReadBytes")
            .unwrap_or(&0);
    }
}
