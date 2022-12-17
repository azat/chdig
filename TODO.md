### Checklist

- LIVE VIEW for `text_log`
- Progress bar (and query estimation)
- Diff values with previous screen (like `top` does)
- Configurable columns
- New metrics (page cache usage)
- Colored queries metrics (if uses too much RAM/CPU/Disk/Net)
- Re-run query with maximum profiling and analyze the data
- Compare multiple queries (`ProfileEvents`)
- `system.trace_log` -> `system.stack_trace` (by `thread_id`)
   - implement `system.kernel_stack_trace` and support it here
   - look at how much does it spent time in locks (but care should be take and conditional variables should not be take into account)
   - various grouping
- EXPLAIN for query (and use syntect for highlight)

*See lot's of TODO/FIXME/NOTE in the code*

### Plugins checklist

- `system.events/metrics/asynchronous_metrics`
- `system.parts`
- `system.mutations`
- `system.replication_queue`
- `system.replicated_fetches`
- `system.backups`
- `system.errors`
- `system.warnings`
- `system.*_log`
  - `system.processors_profile_log JOIN system.query_log`
  - ...
- Locks introspection
- ...

### Rust checklist

- better Rust
- better error handling
- better shortcuts
- write tests
- extend documentation (Features, Motivation)
- add screencasts with [asciinema](https://asciinema.org/)
- Rewrite tfg in Rust
- panic from thread fails only that thread, it need to stop the whole program
