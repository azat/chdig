### chdig

Dig into [ClickHouse](https://github.com/ClickHouse/ClickHouse/) with TUI interface.

### Installation

`chdig` is also available as part of `clickhouse` - `clickhouse chdig`, but
that version may be slightly outdated.

Pre-built packages (`.deb`, `.rpm`, `archlinux`, `.tar.gz`) and standalone
binaries for `Linux` and `macOS` are available for both `x86_64` and `aarch64`
architectures.

The latest [unstable release can be found on GitHub](https://github.com/azat/chdig/releases/tag/latest).

*See also the complete list of [releases](https://github.com/azat/chdig/releases).*

<details>

<summary>Package repositories (AUR, Scoop, Homebrew)</summary>

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

```
brew install chdig
```

</details>

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
- [Flamegraphs](Documentation/FAQ.md#what-is-flamegraph) (CPU/Real/Memory/Live)
- Cluster support (`--cluster`) - aggregate data from all hosts in the cluster
- Historical support (`--history`) - includes rotated `system.*_log_*` tables
- `clickhouse-client` compatibility (including `--connection`) for options and configuration files

And there is a huge bunch of [TODOs](TODO.md#checklist) (right now it is too
huge to include it here).

**Note, this it is in a pre-alpha stage, so everything can be changed (keyboard
shortcuts, views, color schema and of course features)**

### Requirements

If something does not work, like you have too old version of ClickHouse, consider upgrading.

Later some backward compatibility will be added as well.

*Note: the oldest version that had been tested was 21.2 (at some point in time)*

### Build from sources

```
cargo build
```

### Third party libraries

- [flamelens](https://github.com/ys-l/flamelens)

### Third party services

- https://dreampuf.github.io/GraphvizOnline/
- https://www.speedscope.app/

### References

- [`clog.py`](https://github.com/azat/ch-env.d/blob/main/scripts/clog.py) - Highlight ClickHouse logs

### Notes

_Since Rust is a new language to me, the code is far from ideal now_
