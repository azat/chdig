#!/usr/bin/env bash
# Capture chdig screenshots (PNG) and recordings (GIF + asciinema cast) for
# Documentation/Features.md, Queries.md and SystemViews.md.
#
# Requirements: tmux, asciinema (>= 3.0, for --capture-input), agg, imagemagick,
# a local ClickHouse server on localhost:9000 and activity from
# generate-load.sh (setup + run).
#
# Usage:
#   capture-screenshots.sh          # capture everything
#   capture-screenshots.sh <shot>…  # capture only the listed shots (function
#                                   # names below without the shot_/gif_ prefix)

set -o nounset -o pipefail

BASE_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/..
IMAGES_DIR=$BASE_DIR/images
CASTS_DIR=${CASTS_DIR:-/tmp/chdig-casts}
CHDIG=${CHDIG:-$BASE_DIR/../target/release/chdig}
CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT:-clickhouse-client}
DB=chdig_demo

TMUX=(tmux -L chdig)
SESSION=shot
COLS=160
ROWS=45
# Fast refresh so that views fill up quickly and GIFs show live updates
CHDIG_ARGS=(--chdig-config /tmp/chdig-empty.yaml --url localhost --delay-interval 1000)

mkdir -p "$IMAGES_DIR" "$CASTS_DIR"
printf '{}\n' > /tmp/chdig-empty.yaml

ch() { $CLICKHOUSE_CLIENT --database "$DB" "$@"; }

die() { echo "ERROR: $*" >&2; exit 1; }

kill_session() { "${TMUX[@]}" kill-session -t $SESSION 2>/dev/null ||:; }

# start_chdig [chdig args...] - start chdig in a fresh tmux session
start_chdig() {
    kill_session
    "${TMUX[@]}" new-session -d -s $SESSION -x $COLS -y $ROWS \
        "$CHDIG ${CHDIG_ARGS[*]} $*"
}

# start_cast <cast-name> [chdig args...] - like start_chdig, but recorded with
# asciinema (including keyboard input, so that the player shows keystrokes)
start_cast() {
    local cast=$CASTS_DIR/$1.cast
    shift
    kill_session
    rm -f "$cast"
    "${TMUX[@]}" new-session -d -s $SESSION -x $COLS -y $ROWS \
        "asciinema rec --capture-input --quiet '$cast' -c '$CHDIG ${CHDIG_ARGS[*]} $*'"
}

pane() { "${TMUX[@]}" capture-pane -p -t $SESSION 2>/dev/null; }

# wait_re <regex> [timeout-seconds] - poll the pane until regex matches
wait_re() {
    local re=$1 timeout=${2:-20} i
    for ((i = 0; i < timeout * 10; ++i)); do
        pane | grep -qE "$re" && return 0
        sleep 0.1
    done
    echo "WARNING: timed out waiting for '$re'" >&2
    return 1
}

# wait_summary - wait for the summary bar (i.e. fully initialized UI)
wait_summary() { wait_re 'Uptime: [^ ]'; }

keys() { "${TMUX[@]}" send-keys -t $SESSION "$@"; }

# snap <name> - render the current pane into Documentation/images/<name>.png
snap() {
    local name=$1
    local ansi=/tmp/chdig-shot.ansi cast=/tmp/chdig-shot.cast gif=/tmp/chdig-shot.gif
    "${TMUX[@]}" capture-pane -e -p -t $SESSION > "$ansi" || die "$name: no pane to capture"
    python3 - "$ansi" "$cast" $COLS $ROWS <<'EOF'
import json, sys
lines = open(sys.argv[1]).read().split('\n')
if lines and lines[-1] == '':
    lines.pop()
data = '\x1b[2J\x1b[H' + '\r\n'.join(lines) + '\x1b[?25l'
with open(sys.argv[2], 'w') as f:
    f.write(json.dumps({"version": 2, "width": int(sys.argv[3]), "height": int(sys.argv[4])}) + '\n')
    f.write(json.dumps([0.1, "o", data]) + '\n')
EOF
    agg -q --last-frame-duration 1 "$cast" "$gif"
    magick "$gif" -coalesce -delete 0--2 -strip "$IMAGES_DIR/$name.png"
    echo "captured $name.png"
}

# render_gif <name> - wait for the recording session to end and render the GIF
render_gif() {
    local name=$1 i
    for ((i = 0; i < 300; ++i)); do
        "${TMUX[@]}" has-session -t $SESSION 2>/dev/null || break
        sleep 0.1
    done
    kill_session
    agg -q "$CASTS_DIR/$name.cast" "$IMAGES_DIR/$name.gif"
    echo "captured $name.gif (cast: $CASTS_DIR/$name.cast)"
}

# burst_queries [n] - run a few heavy queries in the background so that the
# processes view has something to show
burst_queries() {
    local queries=(
        "SELECT url, uniqExact(user_id) AS users, avg(latency_ms) AS avg_latency FROM web_requests GROUP BY url ORDER BY users DESC FORMAT Null"
        "SELECT user_id, sum(bytes_sent) AS traffic FROM web_requests GROUP BY user_id ORDER BY traffic DESC LIMIT 50 FORMAT Null"
        "SELECT sum(sipHash64(user_id, url, latency_ms)) FROM web_requests FORMAT Null"
    )
    for q in "${queries[@]}"; do
        ch --query_profiler_real_time_period_ns=10000000 \
           --query_profiler_cpu_time_period_ns=10000000 \
           --max_threads=4 -q "$q" &>/dev/null &
    done
}

# wait_rows <regex> - wait until the view shows a data row matching regex,
# refreshing the view in between
wait_rows() {
    local re=$1 i
    for ((i = 0; i < 100; ++i)); do
        pane | grep -qE "$re" && return 0
        keys r
        sleep 0.3
    done
    echo "WARNING: no rows matching '$re'" >&2
    return 1
}

### Shots ####################################################################

shot_queries() {
    burst_queries
    start_chdig queries
    wait_summary
    wait_rows 'SELECT'
    snap queries
}

shot_help() {
    start_chdig queries
    wait_summary
    keys F1
    wait_re 'Extended navigation|Show help'
    snap help
}

shot_views_menu() {
    start_chdig queries
    wait_summary
    keys F2
    wait_re 'Query patterns'
    snap views_menu
}

shot_fuzzy_actions() {
    burst_queries
    start_chdig queries
    wait_summary
    wait_rows 'SELECT'
    keys C-p
    wait_re 'flamegraph'
    keys 'flame'
    sleep 0.5
    snap fuzzy_actions
}

shot_settings() {
    start_chdig queries
    wait_summary
    keys F3
    wait_re 'Settings'
    snap settings
}

shot_slow_queries() {
    start_chdig slow-queries
    wait_summary
    wait_rows 'SELECT|INSERT|OPTIMIZE'
    snap slow_queries
}

shot_last_queries() {
    start_chdig last-queries
    wait_summary
    wait_rows 'SELECT|INSERT'
    snap last_queries
}

shot_merges() {
    # a merge of the freshly inserted parts, big enough to be visible
    ch -q "OPTIMIZE TABLE web_requests PARTITION tuple(toYYYYMMDD(now()))" &>/dev/null &
    start_chdig merges
    wait_summary
    wait_rows 'web_requests'
    snap merges
}

shot_mutations() {
    ch --mutations_sync=0 -q "
        ALTER TABLE web_requests
        UPDATE bytes_sent = bytes_sent + 1
        WHERE sipHash64(user_id, timestamp) % 2 = 0" &>/dev/null
    start_chdig mutations
    wait_summary
    wait_rows 'web_requests'
    snap mutations
}

shot_replication_queue() {
    ch -q "SYSTEM STOP FETCHES events_r2"
    ch -q "INSERT INTO events_r1 SELECT now(), 'click', randomPrintableASCII(100) FROM numbers(1000000)"
    start_chdig replication-queue
    wait_summary
    wait_rows 'events_r2|GET_PART'
    snap replication_queue
    ch -q "SYSTEM START FETCHES events_r2"
}

shot_replicated_fetches() {
    ch -q "SYSTEM STOP FETCHES events_r2"
    ch -q "INSERT INTO events_r1 SELECT now(), 'view', randomPrintableASCII(500) FROM numbers(5000000)"
    start_chdig replicated-fetches
    wait_summary
    ch -q "SYSTEM START FETCHES events_r2"
    wait_rows 'events_r2'
    snap replicated_fetches
}

shot_replicas() {
    start_chdig replicas
    wait_summary
    wait_rows 'events_r'
    snap replicas
}

shot_tables() {
    start_chdig tables
    wait_summary
    wait_rows 'web_requests'
    snap tables
}

shot_table_parts() {
    start_chdig table-parts
    wait_summary
    wait_rows 'web_requests'
    snap table_parts
}

shot_part_log() {
    start_chdig part-log
    wait_summary
    wait_rows 'web_requests'
    snap part_log
}

shot_backups() {
    ch -q "BACKUP TABLE countries TO File('chdig-demo-backup-{uuid}') ASYNC" &>/dev/null
    start_chdig backups
    wait_summary
    wait_rows 'BACKUP'
    snap backups
}

shot_dictionaries() {
    start_chdig dictionaries
    wait_summary
    wait_rows 'countries_dict'
    snap dictionaries
}

shot_errors() {
    start_chdig errors
    wait_summary
    wait_rows 'UNKNOWN_TABLE|FUNCTION_THROW'
    snap errors
}

shot_server_logs() {
    start_chdig server-logs
    wait_summary
    wait_rows 'executeQuery|MergeTask|<[A-Z][a-z]+>'
    snap server_logs
}

shot_logs_filters() {
    # short interval to keep the number of Ctrl-f tags small
    start_chdig --start 1m server-logs
    wait_summary
    wait_rows '<Debug>|<Trace>'
    keys C-f
    wait_re 'identifier:'
    snap logs_filter_mode
    local tag
    tag=$(pane | grep -oP 'MemoryTracker\[\Kl[0-9]+' | head -1)
    if [[ -n $tag ]]; then
        keys "$tag" Enter
        sleep 1
        snap logs_filtered
    else
        echo "WARNING: no MemoryTracker logger tag visible, skipping logs_filtered" >&2
    fi
}

shot_cpu_flamegraph() {
    burst_queries
    start_chdig cpu-flamegraph
    wait_summary
    wait_re 'Flamegraph'
    wait_rows 'clickhouse|DB::'
    snap cpu_flamegraph
}

shot_query_details() {
    burst_queries
    start_chdig queries
    wait_summary
    wait_rows 'SELECT'
    keys j
    keys C-p
    wait_re 'flamegraph'
    keys 'Query details'
    sleep 0.5
    keys Enter
    wait_re 'query_id|QueryDetails'
    snap query_details
}

shot_explain_plan() {
    burst_queries
    start_chdig queries
    wait_summary
    wait_rows 'GROUP BY'
    keys j
    keys e
    wait_re 'ReadFromMergeTree|Expression'
    snap explain_plan
}

shot_show_query() {
    burst_queries
    start_chdig queries
    wait_summary
    wait_rows 'SELECT'
    keys j
    keys S
    wait_re 'FROM web_requests'
    snap show_query
}

shot_filter() {
    burst_queries
    start_chdig queries
    wait_summary
    wait_rows 'SELECT'
    keys /
    sleep 0.3
    keys '%sipHash64%'
    sleep 0.5
    snap filter
}

shot_panes() {
    cat > /tmp/chdig-layout.yaml <<'EOF'
layout:
  direction: horizontal
  panes:
  - direction: vertical
    ratio: 0.55
    panes:
    - view: queries
      ratio: 0.6
    - server_logs
  - cpu_flamegraph
  focus: queries
EOF
    burst_queries
    start_chdig --chdig-config /tmp/chdig-layout.yaml
    wait_summary
    wait_rows 'SELECT'
    wait_re 'Flamegraph'
    snap panes
}

### GIFs #####################################################################

gif_live_flamegraph() {
    burst_queries
    start_cast live_flamegraph live-flamegraph
    wait_summary
    wait_re 'Flamegraph'
    sleep 10
    burst_queries
    sleep 10
    keys q
    render_gif live_flamegraph
}

gif_query_patterns_metrics() {
    # longer interval so the heatmap has history to show
    start_cast query_patterns_metrics --start 3h query-patterns
    wait_summary
    wait_rows 'SELECT|INSERT'
    sleep 3
    # cycle through a few metrics (Space)
    for _ in 1 2 3 4; do
        keys Space; sleep 2.5
    done
    # the fuzzy picker (m) lists all of them
    keys m; sleep 4
    keys 'cpu'; sleep 1.5
    keys Enter; sleep 3
    keys q; sleep 1
    "${TMUX[@]}" has-session -t $SESSION 2>/dev/null && keys q
    render_gif query_patterns_metrics
}

gif_panes_split() {
    burst_queries
    start_cast panes_split queries
    wait_summary
    wait_rows 'SELECT'
    sleep 2
    # split right; the views menu opens for the new pane
    keys M-=
    wait_re 'Slow queries' 5; sleep 1.5
    keys j; sleep 0.5
    keys Enter; sleep 3
    # split the right pane below, put server logs there
    keys M--
    wait_re 'Slow queries' 5; sleep 1.5
    for _ in $(seq 21); do keys j; sleep 0.15; done
    sleep 1
    keys Enter; sleep 4
    # resize the panes
    for _ in 1 2 3 4 5; do keys C-Left; sleep 0.3; done
    sleep 1
    for _ in 1 2 3; do keys C-Up; sleep 0.3; done
    sleep 1.5
    # zoom the focused pane and back
    keys C-x; sleep 2.5
    keys C-x; sleep 2
    # move focus around
    keys M-Left; sleep 1.5
    keys M-Right; sleep 1.5
    # close the focused pane
    keys q; sleep 2
    keys q; sleep 1
    "${TMUX[@]}" has-session -t $SESSION 2>/dev/null && { keys q; sleep 1; }
    "${TMUX[@]}" has-session -t $SESSION 2>/dev/null && keys q
    render_gif panes_split
}

gif_overview() {
    cat > /tmp/chdig-tour.yaml <<'EOF'
views:
  server_logs:
    start: 1m
    limit: 1000
    level: warning
  cpu_flamegraph:
    start: 1m

layout:
  direction: horizontal
  panes:
  - view: queries
    ratio: 0.6
  - direction: vertical
    panes:
    - cpu_flamegraph
    - view: server_logs
      ratio: 0.4
  focus: queries
EOF
    burst_queries
    # the last --chdig-config wins, overriding the neutral one
    start_cast overview --start 10m --chdig-config /tmp/chdig-tour.yaml
    wait_summary
    # 'hjkl' = flamelens help bar, i.e. the flamegraph pane actually rendered;
    # the queries pane is too narrow for the query column, so wait for any row
    wait_re 'hjkl' 45
    wait_re 'default' 20
    sleep 5
    # filter the queries (LIKE pattern) and select the first one
    keys /; sleep 0.5
    keys '%sipHash64%'; sleep 1
    keys Enter; sleep 2
    keys j; sleep 1
    # explain it
    keys e
    if wait_re 'ReadFromMergeTree|Aggregating' 5; then
        sleep 3
        keys Escape; sleep 1
    fi
    # drop the filter
    keys -; sleep 1.5
    # zoom the flamegraph pane and back
    keys M-Right; sleep 1.5
    keys C-x; sleep 3
    keys C-x; sleep 2
    # swap this pane's view for slow queries
    keys F2; sleep 2
    keys j; sleep 0.5
    keys Enter
    wait_re 'Slow queries' 5; sleep 4
    keys q; sleep 1
    "${TMUX[@]}" has-session -t $SESSION 2>/dev/null && { keys q; sleep 1; }
    "${TMUX[@]}" has-session -t $SESSION 2>/dev/null && { keys q; sleep 1; }
    "${TMUX[@]}" has-session -t $SESSION 2>/dev/null && keys q
    render_gif overview
}

### main #####################################################################

ALL_SHOTS=(
    queries help views_menu fuzzy_actions settings
    slow_queries last_queries
    merges mutations
    replication_queue replicated_fetches replicas
    tables table_parts part_log
    backups dictionaries errors server_logs logs_filters
    cpu_flamegraph query_details explain_plan show_query filter panes
)
ALL_GIFS=(live_flamegraph overview query_patterns_metrics panes_split)

if [[ $# -gt 0 ]]; then
    for name in "$@"; do
        if declare -F "shot_$name" >/dev/null; then "shot_$name"
        elif declare -F "gif_$name" >/dev/null; then "gif_$name"
        else die "unknown shot: $name"
        fi
    done
else
    for name in "${ALL_SHOTS[@]}"; do "shot_$name"; done
    for name in "${ALL_GIFS[@]}"; do "gif_$name"; done
fi

kill_session
