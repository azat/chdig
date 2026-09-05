# Query views

Complements the [Queries section of the features tour](Features.md#queries).
Every view/action here is reachable via **Ctrl-P**; the shortcuts are listed
in [Actions.md](Actions.md).

## History views (`system.query_log`)

- **Slow queries** (`chdig slow-queries`) - queries slower than 1 second,
  ordered by duration:

  ![slow queries](images/slow_queries.png)

- **Last queries** (`chdig last-queries`) - recently finished queries:

  ![last queries](images/last_queries.png)

The time interval is controlled with `--start`/`--end` and can be moved
interactively (**t**/**T**/**Alt-t**).

## Filtering

**/** filters any query view with a `LIKE` pattern (matched against query,
user, query_id, ...); **-** shows everything again:

![filter](images/filter.png)

## Inspecting a query

**S** shows the full query text:

![show query](images/show_query.png)

*Query details* (via **Ctrl-P**) shows everything about one query:

![query details](images/query_details.png)

*Query profile events* (via **Ctrl-P**) lists the query's `ProfileEvents`;
for a running query it auto-refreshes (query and its subqueries fetched by
`query_id`), with the rate column measured over the last interval (like
`top`), and switches to the lifetime average once the query finishes.

**e**/**E**/**s**/**I** run `EXPLAIN PLAN`/`PIPELINE`/`SYNTAX`/`INDEXES` for
the selected query (**G** opens the pipeline graph in the browser):

![explain plan](images/explain_plan.png)

## Other per-query actions

- **K** - `KILL` the query
- **l** - show the query's logs (see [log filtering](Features.md#logs))
- **y** - copy the query to the clipboard
- **Alt-E** - edit the query and re-execute it
- **L** - live flamegraph of the running query; CPU/Real/Memory variants and
  *Share* (speedscope) versions via **Ctrl-P**
- *Query flamegraph diff* - select two queries with **Space** and compare
  their profiles
- *Query metric log* - the query's `system.query_metric_log` (memory and
  ProfileEvents over its lifetime, with sparklines; **Enter** charts a metric)
- *Query threads* - the query's `system.query_thread_log` (per-thread
  CPU/IO wait, rows/bytes, peak memory; **Enter** shows the thread's
  ProfileEvents)
- *Export to Perfetto* - open the query timeline in
  [ui.perfetto.dev](https://ui.perfetto.dev/)
  (see [FAQ](FAQ.md#what-is-perfetto-export))
