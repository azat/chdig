### Checklist

- Progress bar (and query estimation, requires driver support)
- Diff profile events for multiple queries
- `ProfileEvents` in a loadavg fashion (1/5/15 using `simple_moving_average` crate)
- Configurable columns
- New metrics (page cache usage, but there are some issues with this metrics in ClickHouse itself)
- Colored queries metrics (if uses too much RAM/CPU/Disk/Net)
- Graphs for summary metrics (memory, ...)
- Compare multiple queries (`ProfileEvents`)
- `system.trace_log` -> `system.stack_trace` (by `thread_id`)
   - implement `system.kernel_stack_trace` and support it here
   - look at how much does it spent time in locks (but care should be take and conditional variables should not be take into account)
   - various grouping
- Decompose query to the inner most subquery
- Async metrics with charts

*See lot's of TODO/FIXME/NOTE in the code*

### Plugins checklist

- `system.events/metrics/asynchronous_metrics`
- `system.parts`
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
- Rewrite flameshow in Rust (at least brew does not accept packages that depends on PyOxidizer)
- panic from thread fails only that thread, it need to stop the whole program
