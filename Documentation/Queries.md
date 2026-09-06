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

**/** filters any query view; the list narrows as you type (the rows already
loaded), and the server is asked in the background for more rows matching the
filter. Free text is a `LIKE` against query, user, query_id, database, host,
log_comment, ... (`%text%` unless it has a `%`), and `field<op>value`
predicates can be combined (all must match), e.g.

```
user=default elapsed>10s mem>1G
q~insert kind=Insert host=ch1
cancelled=1 exception~Timeout
```

Fields: `user`, `initial_user`, `host`, `db`, `query_id`, `initial_query_id`,
`hash`, `query` (`q`), `log_comment`, `exception`, `elapsed` (`500ms`, `10s`,
`2m`), `mem` (`100M`, `2G`), `cpu` (percent), `thr`, `cancelled`, `initial`,
`kind`. Operators: `=`, `!=`, `~` (LIKE), `!~`, `>`, `>=`, `<`, `<=`; quote
values with spaces. **Tab**/**Shift-Tab** cycle through the completions of the
field name, or of the value from the rows on screen (users, databases, hosts,
...); the hint line above the prompt lists the candidates. **-** clears the filter and
shows everything again:

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

- **K** - `KILL` the query (or every query selected with **Space**)
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
