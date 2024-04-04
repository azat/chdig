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
```

And later, instead of specifying `--url` (with password in plaintext, which is
highly not recommended), you can use `chdig --connection prod`.

  [1]: https://github.com/ClickHouse/ClickHouse/pull/45715
  [2]: https://github.com/ClickHouse/ClickHouse/pull/46480

### What are the shortcuts supported?

#### chdig v24.3.1

##### Global shortcuts:
- **F1** - Show help
- **F2** - Views
- **F8** - Show actions
- **Ctrl+p** - Fuzzy actions
- **F** - CPU Server Flamegraph
- CPU Server Flamegraph in speedscope
- **~** - chdig debug console
- **q** - Back/Quit
- **Q** - Quit forcefully
- **Backspace** - Back
- **p** - Toggle pause
- **r** - Refresh
- **T** - Seek 10 mins backward
- **t** - Seek 10 mins forward
- **Alt+t** - Set time interval

##### Actions:
- **<Space>** - Select
- **-** - Show all queries
- **+** - Show queries on shards
- **/** - Filter
- **D** - Query details
- **P** - Query processors
- **v** - Query views
- **C** - Show CPU flamegraph
- **R** - Show Real flamegraph
- **M** - Show memory flamegraph
- **L** - Show live flamegraph
- Show CPU flamegraph in speedscope
- Show Real flamegraph in speedscope
- Show memory flamegraph in speedscope
- Show live flamegraph in speedscope
- **Alt+E** - Edit query and execute
- **s** - `EXPLAIN SYNTAX`
- **e** - `EXPLAIN PLAN`
- **E** - `EXPLAIN PIPELINE`
- **G** - `EXPLAIN PIPELINE graph=1` (open in browser)
- **I** - `EXPLAIN INDEXES`
- **K** - `KILL` query
- **l** - Show query Logs
- **(** - Increase number of queries to render to 20
- **)** - Decrease number of queries to render to 20

##### Extended navigation:
- **Home** - reset selection/follow item in table


### Why I see IO wait reported as zero?

- You should ensure that ClickHouse uses one of taskstat gathering methods:
  - procfs
  - netlink

- And also for linux 5.14 you should enable `kernel.task_delayacct` sysctl as well.
