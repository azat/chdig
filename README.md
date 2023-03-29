### chdig

Dig into [ClickHouse](https://github.com/ClickHouse/ClickHouse/) with TUI interface.

### Motivation

The idea is came from everyday digging into various ClickHouse issues.

ClickHouse has a approximately universe of introspection tools, and it is easy
to forget some of them. At first I came with some
[slides](https://azat.sh/presentations/2022-know-your-clickhouse/) and a
picture (to attract your attention) by analogy to what [Brendan
Gregg](https://www.brendangregg.com/linuxperf.html) did for Linux:

[![Know Your ClickHouse](https://azat.sh/presentations/2022-know-your-clickhouse/Know-Your-ClickHouse.png)](https://azat.sh/presentations/2022-know-your-clickhouse/)

*Note, the picture and the presentation had been made in the beginning of 2022,
so it may not include some new introspection tools*.

But this requires you to dig into lots of places, and even though during this
process you will learn a lot, it does not solves the problem of forgetfulness.
So I came up with this simple TUI interface that tries to make this process
simpler.

`chdig` can be used not only to debug some problems, but also just as a regular
introspection, like `top` for Linux.

### Demo

[![asciicast](https://asciinema.org/a/btIMdbWEMphHxTSVbMybJwgBG.svg)](https://asciinema.org/a/btIMdbWEMphHxTSVbMybJwgBG)

### Features

- `top` like interface (or [`csysdig`](https://github.com/draios/sysdig) to be more precise)
- [Flamegraphs](https://www.brendangregg.com/flamegraphs.html) (CPU/Real/Memory)
- Query logs
- Cluster support (`--cluster`)

### Plugins

- Query view (`system.processes`)
- Merges view (`system.merges`)
- Mutations view (`system.mutations`)
- Replicas (`system.replicas`)
- Replication queue view (`system.replication_queue`)
- Fetches (`system.replicated_fetches`)
- Backups (`system.backups`)
- Errors (`system.errors`)

And there is a huge bunch of [TODOs](TODO.md#checklist) (right now it is too
huge to include it here).

**Note, this it is in a pre-alpha stage, so everything can be changed (keyboard
shortcuts, views, color schema and of course features)**

### Requirements

- ClickHouse 23.1+

### Installation

Prerequisites:
- [`cargo`](https://doc.rust-lang.org/cargo/)
- [`nfpm`](https://github.com/goreleaser/nfpm)
- [`pyinstaller`](https://pyinstaller.org/en/stable/)

```
make packages
```

For now, only deb packages are available.

But we are using `nfpm`, so any supported package, archlinux, deb, rpm, tar,
you name it, can be supported):

### Third party libraries

- [tfg](https://github.com/4rtzel/tfg)

### Notes

Since Rust is a new language to me, the code can be far from ideal.
