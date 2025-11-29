### What is format of the URL accepted by `chdig`?

The simplest form is just - **`localhost`**

For a secure connections with user and password _(note: passing the password on
the command line is not safe)_, use:

```sh
chdig -u 'user:password@clickhouse-host.com/?secure=true'
```

A full list of supported connection options is available [here](https://github.com/azat-rust/clickhouse-rs/?tab=readme-ov-file#dns).

_Note: This link currently points to my fork, as some changes have not yet been accepted upstream._

### Environment variables

A safer way to pass the password is via environment variables:


```sh
export CLICKHOUSE_USER='user'
export CLICKHOUSE_PASSWORD='password'
chdig -u 'clickhouse-host.com/?secure=true'
# or specify the port explicitly
chdig -u 'clickhouse-host.com:9440/?secure=true'
```

### What is --config (`CLICKHOUSE_CONFIG`)?

This is standard config for [ClickHouse client](https://clickhouse.com/docs/interfaces/cli#configuration_files), i.e.

```yaml
user: foo
password: bar
host: play
secure: true
```

_See also some examples and possible advanced use cases [here](/tests/configs)_

### What is --connection?

`--connection` allows you to use predefined connections, that is supported by
`clickhouse-client` ([1], [2]).

Here is an example in `XML` format:

```xml
<clickhouse>
    <connections_credentials>
        <connection>
            <name>prod</name>
            <hostname>prod</hostname>
            <user>default</user>
            <password>secret</password>
            <!-- <secure>false</secure> -->
            <!-- <skip_verify>false</skip_verify> -->
            <!-- <ca_certificate></ca_certificate> -->
            <!-- <client_certificate></client_certificate> -->
            <!-- <client_private_key></client_private_key> -->
        </connection>
    </connections_credentials>
</clickhouse>
```

Or in `YAML`:

```yaml
---
connections_credentials:
  prod:
    name: prod
    hostname: prod
    user: default
    password: secret
    # secure: false
    # skip_verify: false
    # ca_certificate:
    # client_certificate:
    # client_private_key:
```

And later, instead of specifying `--url` (with password in plain-text, which is
highly not recommended), you can use `chdig --connection prod`.

  [1]: https://github.com/ClickHouse/ClickHouse/pull/45715
  [2]: https://github.com/ClickHouse/ClickHouse/pull/46480

### What is flamegraph?

It is best to start with [Brendan Gregg's site](https://www.brendangregg.com/flamegraphs.html) for a solid introduction to flamegraphs.

Below is a description of the various types of flamegraphs available in `chdig`:

- `Real` - Traces are captured at regular intervals (defined by [`query_profiler_real_time_period_ns`](https://clickhouse.com/docs/operations/settings/settings#query_profiler_real_time_period_ns)/[`global_profiler_real_time_period_ns`](https://clickhouse.com/docs/operations/server-configuration-parameters/settings#global_profiler_real_time_period_ns)) for each thread, regardless of whether the thread is actively running on the CPU
- `CPU` - Traces are captured only when a thread is actively executing on the CPU, based on the interval specified in [`query_profiler_cpu_time_period_ns`](https://clickhouse.com/docs/operations/settings/settings#query_profiler_cpu_time_period_ns)/[`global_profiler_cpu_time_period_ns`](https://clickhouse.com/docs/operations/server-configuration-parameters/settings#global_profiler_cpu_time_period_ns)
- `Memory` - Traces are captured after each [`memory_profiler_step`](https://clickhouse.com/docs/operations/settings/settings#memory_profiler_step)/[`total_memory_profiler_step`](https://clickhouse.com/docs/operations/server-configuration-parameters/settings#total_memory_profiler_step) bytes are allocated by the query or server
- `Live` - Real-time visualization of what server is doing now from [`system.stack_trace`](https://clickhouse.com/docs/operations/system-tables/stack_trace)

See also:
- [Sampling Query Profiler](https://clickhouse.com/docs/operations/optimizing-performance/sampling-query-profiler)

_Note: for `Memory` `chdig` uses `memory_profiler_step` over `memory_profiler_sample_probability`, since the later is disabled by default_

### Why I see IO wait reported as zero?

- You should ensure that ClickHouse uses one of taskstat gathering methods:
  - procfs
  - netlink

- And also for linux 5.14 you should enable `kernel.task_delayacct` sysctl as well.

### How to copy text from `chdig`

By default `chdig` is started with mouse mode enabled in terminal, you cannot
copy with this mode enabled. But, terminals provide a way to disable it
temporary by pressing some key (usually it is some combination of `Alt`,
`Shift` or/and `Ctrl`), so you can find yours press them, and copy.

---

See also [bugs list](Bugs.md)
