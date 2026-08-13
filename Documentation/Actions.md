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
|                 | **F**         | CPU flamegraph (other server flamegraphs are in the Views/**F2** menu)|
| Actions         | **<Space>**   | Select                                        |
|                 | **-**         | Show all queries                              |
|                 | **+**         | Show queries on shards                        |
|                 | **/**         | Filter                                        |
|                 |               | Query details                                 |
|                 |               | Query profile events                          |
|                 |               | Query processors                              |
|                 |               | Query views                                   |
|                 |               | Show CPU flamegraph                           |
|                 |               | Show Real flamegraph                          |
|                 |               | Show memory flamegraph                        |
|                 |               | Show memory sample flamegraph                 |
|                 |               | Show jemalloc sample flamegraph               |
|                 |               | Show events flamegraph                        |
|                 | **L**         | Show live flamegraph                          |
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
| Flamegraphs     | **S**         | Share the flamegraph (upload and open in browser)|
|                 | **r**/**R**   | Refresh (live flamegraphs)                    |
|                 | **P**         | Pause/resume updates (live flamegraphs)       |
|                 | **D**         | Toggle diff coloring (live flamegraphs)       |
|                 | **hjkl**      | Move cursor                                   |
|                 | **f**/**b**   | Scroll                                        |
|                 | **Enter**/**Esc**| Zoom/unzoom                                |
|                 | **/**         | Search                                        |
|                 | **#**         | Search like the frame under the cursor        |
|                 | **i**         | Reverse stack order                           |
|                 | **n**/**N**   | Move to next/previous search match            |
| Logs            | **-**         | Turn ON/OFF options:                          |
|                 |               | - `S` - toggle wrap mode                      |
|                 | **/**         | Forward search                                |
|                 | **?**         | Reverse search                                |
|                 | **s**         | Save logs to file                             |
|                 | **S**         | Share logs (upload and open in browser)       |
|                 | **n**/**N**   | Move to next/previous match                   |
| Basic navigation| **j**/**k**   | Down/Up                                       |
|                 | **G**/**g**   | Move to the end/Move to the beginning         |
|                 | **PageDown**/**PageUp**| Move to the end/Move to the beginning|
|                 | **Home**      | Reset selection/follow item in table          |
| Panes           | **Alt+=**     | Split pane (right)                            |
|                 | **Alt+-**     | Split pane (below)                            |
|                 | **Alt+Arrows**| Move focus between panes                      |
|                 | **Ctrl+Arrows**| Resize panes (or drag the separator)         |
|                 | **Ctrl+x**    | Zoom the focused pane (fullscreen on/off)     |
| chdig controls  | **Esc**       | Back/Close pane                               |
|                 | **q**         | Back/Close pane/Quit                          |
|                 | **Q**         | Quit forcefully                               |
|                 | **Backspace** | Back                                          |
|                 | **p**         | Toggle pause                                  |
|                 | **r**         | Refresh                                       |
|                 | **T**         | Seek 10 mins backward                         |
|                 | **t**         | Seek 10 mins forward                          |
|                 | **Alt+t**     | Set time interval                             |
|                 | **~**         | chdig debug console                           |
