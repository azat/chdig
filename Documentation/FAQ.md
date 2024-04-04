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

### What is format of the URL accepted by chdiag?

example for secure connection with all default connection settings & user name & password (passing the password in the command line is unsafe)

```sh
chdig -u 'tcp://user:password@clickhouse-host.com:9440/?secure=true&skip_verify=false&compression=lz4&query_timeout=600s&connection_timeout=5s'
```

safer option is to pass the password via the ENV

```sh
export CLICKHOUSE_USER='user'
export CLICKHOUSE_PASSWORD='password'
chdig -u 'tcp://clickhouse-host.com:9440/?secure=true'
```

or via the config file (see above)

### Why I see IO wait reported as zero?

- You should ensure that ClickHouse uses one of taskstat gathering methods:
  - procfs
  - netlink

- And also for linux 5.14 you should enable `kernel.task_delayacct` sysctl as well.
