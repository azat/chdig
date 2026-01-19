### Actions

`chdig` supports lots of actions, some has shortcut, others available only in
`Ctlr-P` (fuzzy search by all actions) (also there is `F8` for query actions
and `F2` for global actions, if you prefer old school).

### Shortcuts

Here is a list of available shortcuts

| Category        | Shortcut      | Description                                   |
|-----------------|---------------|-----------------------------------------------|
| Global Shortcuts| **F1**        | Show help                                     |
|                 | **F2**        | Views                                         |
|                 | **F8**        | Show actions                                  |
|                 | **Ctrl-p**    | Fuzzy actions                                 |
|                 | **F**         | CPU Server Flamegraph                         |
|                 |               | Real Server Flamegraph                        |
|                 |               | Memory Server Flamegraph                      |
|                 |               | Memory Sample Server Flamegraph               |
|                 |               | Jemalloc Sample Server Flamegraph             |
|                 |               | Events Server Flamegraph                      |
|                 |               | Live Server Flamegraph                        |
|                 |               | CPU Server Flamegraph in speedscope           |
|                 |               | Real Server Flamegraph in speedscope          |
|                 |               | Memory Server Flamegraph in speedscope        |
|                 |               | Memory Sample Server Flamegraph in speedscope |
|                 |               | Jemalloc Sample Server Flamegraph in speedscope|
|                 |               | Events Server Flamegraph in speedscope        |
|                 |               | Live Server Flamegraph in speedscope          |
| Actions         | **<Space>**   | Select                                        |
|                 | **-**         | Show all queries                              |
|                 | **+**         | Show queries on shards                        |
|                 | **/**         | Filter                                        |
|                 |               | Query details                                 |
|                 |               | Query profile events                          |
|                 | **P**         | Query processors                              |
|                 | **v**         | Query views                                   |
|                 | **C**         | Show CPU flamegraph                           |
|                 | **R**         | Show Real flamegraph                          |
|                 | **M**         | Show memory flamegraph                        |
|                 |               | Show memory sample flamegraph                 |
|                 |               | Show jemalloc sample flamegraph               |
|                 |               | Show events flamegraph                        |
|                 | **L**         | Show live flamegraph                          |
|                 |               | Show CPU flamegraph in speedscope             |
|                 |               | Show Real flamegraph in speedscope            |
|                 |               | Show memory flamegraph in speedscope          |
|                 |               | Show memory sample flamegraph in speedscope   |
|                 |               | Show jemalloc sample flamegraph in speedscope |
|                 |               | Show events flamegraph in speedscope          |
|                 |               | Show live flamegraph in speedscope            |
|                 | **Alt+E**     | Edit query and execute                        |
|                 | **S**         | Show query                                    |
|                 | **y**         | Copy query to clipboard                       |
|                 | **s**         | `EXPLAIN SYNTAX`                              |
|                 | **e**         | `EXPLAIN PLAN`                                |
|                 | **E**         | `EXPLAIN PIPELINE`                            |
|                 | **G**         | `EXPLAIN PIPELINE graph=1` (open in browser)  |
|                 | **I**         | `EXPLAIN INDEXES`                             |
|                 | **K**         | `KILL` query                                  |
|                 | **l**         | Show query logs                               |
|                 | **(**         | Increase number of queries to render to 20    |
|                 | **)**         | Decrease number of queries to render to 20    |
| Logs            | **-**         | Turn ON/OFF options:                          |
|                 |               | - `S` - toggle wrap mode                      |
|                 | **/**         | Forward search                                |
|                 | **?**         | Reverse search                                |
|                 | **s**         | Save logs to file                             |
|                 | **n**/**N**   | Move to next/previous match                   |
| Basic navigation| **j**/**k**   | Down/Up                                       |
|                 | **G**/**g**   | Move to the end/Move to the beginning         |
|                 | **PageDown**/**PageUp**| Move to the end/Move to the beginning|
|                 | **Home**      | Reset selection/follow item in table          |
| chdig controls  | **Esc**       | Back/Quit                                     |
|                 | **q**         | Back/Quit                                     |
|                 | **Q**         | Quit forcefully                               |
|                 | **Backspace** | Back                                          |
|                 | **p**         | Toggle pause                                  |
|                 | **r**         | Refresh                                       |
|                 | **T**         | Seek 10 mins backward                         |
|                 | **t**         | Seek 10 mins forward                          |
|                 | **Alt+t**     | Set time interval                             |
|                 | **~**         | chdig debug console                           |
