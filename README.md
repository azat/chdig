### chdig

Dig into [ClickHouse](https://github.com/ClickHouse/ClickHouse/) with TUI interface.

### Installation

There are pre-built packages for the latest available version:

- [chdig standalone binary](https://github.com/azat/chdig/releases/download/latest/chdig)
- [debian](https://github.com/azat/chdig/releases/download/latest/chdig-latest_amd64.deb)
- [fedora](https://github.com/azat/chdig/releases/download/latest/chdig-latest.x86_64.rpm)
- [archlinux](https://github.com/azat/chdig/releases/download/latest/chdig-latest-x86_64.pkg.tar.zst)
- [tar.gz](https://github.com/azat/chdig/releases/download/latest/chdig-latest-x86_64.tar.gz)
- [macos x86_64](https://github.com/azat/chdig/releases/download/latest/chdig-macos-x86_64)
- [macos arm64](https://github.com/azat/chdig/releases/download/latest/chdig-macos-arm64)
- [windows](https://github.com/azat/chdig/releases/download/latest/chdig-windows.exe)

<details>

<summary>Package repositories</summary>

#### archlinux user repository (aur)

And also for archlinux there is an aur package:
- [**chdig-latest-bin**](https://aur.archlinux.org/packages/chdig-latest-bin) - binary artifact of the upstream
- [chdig-git](https://aur.archlinux.org/packages/chdig-git) - build from sources
- [chdig-bin](https://aur.archlinux.org/packages/chdig-bin) - binary of the latest stable version

*Note: `chdig-latest-bin` is recommended because it is latest available version and you don't need toolchain to compile*

#### scoop (windows)

```
scoop bucket add extras
scoop install extras/chdig
```

#### brew (macos)

Right now official formula is [not available yet](https://github.com/azat/chdig/issues/13), but you can use my tap for this:

```
brew tap azat-archive/chdig
brew install azat-archive/chdig/chdig
```

</details>

*And also see [releases](https://github.com/azat/chdig/releases) as well.*

### Demo

[![asciicast](https://github.com/azat/chdig/releases/download/v0.5.0/chdig-v0.5.0.gif)](https://asciinema.org/a/627298)

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

See also [FAQ](Documentation/FAQ.md)

### Features

- `top` like interface (or [`csysdig`](https://github.com/draios/sysdig) to be more precise)
- [Flamegraphs](https://www.brendangregg.com/flamegraphs.html) (CPU/Real/Memory)
- Cluster support (`--cluster`)

### Views

- Query view (`system.processes`)
- Slow query log (`system.query_log`)
- Last queries (`system.query_log`)
- Processors (`system.processors_profile_log`)
- Views (`system.query_views_log`)
- Query logs (`system.text_log`)
- Server logs (`system.text_log`)
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

If something does not work, like you have too old version of ClickHouse, consider upgrading.

Later some backward compatiblity will be added as well.

*Note: the oldest version that had been tested was 21.2*

### Build from sources

Prerequisites:
- [`cargo`](https://doc.rust-lang.org/cargo/)
- [`nfpm`](https://github.com/goreleaser/nfpm)

```
# will build deb/rpm/archlinux packages
make packages
```

### Third party libraries

- [flamelens](https://github.com/ys-l/flamelens)

### Third party services

- https://dreampuf.github.io/GraphvizOnline/
- https://www.speedscope.app/

### References

- [`clog.py`](https://github.com/azat/ch-env.d/blob/main/scripts/clog.py) - Highlight ClickHouse logs

### Notes

Since Rust is a new language to me, the code can be far from ideal.
