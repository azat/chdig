#!/usr/bin/env bash
# Generate realistic activity on a local ClickHouse server so that chdig views
# have data to show (processes, merges, mutations, replicas, replication
# queue, errors, dictionaries, flamegraphs, ...).
#
# Usage:
#   generate-load.sh setup   # create tables/dictionary and fill initial data
#   generate-load.sh run     # spawn background activity loops (writes PID file)
#   generate-load.sh stop    # stop background loops
#   generate-load.sh clean   # drop everything created by setup

set -o errexit -o nounset -o pipefail

CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT:-clickhouse-client}
DB=chdig_demo
PID_FILE=/tmp/chdig-demo-load.pids

ch() { $CLICKHOUSE_CLIENT --database "$DB" "$@"; }

PROFILER_SETTINGS=(
    --query_profiler_real_time_period_ns=10000000
    --query_profiler_cpu_time_period_ns=10000000
    --memory_profiler_sample_probability=0.01
    --log_queries=1
)

setup() {
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE IF NOT EXISTS $DB"

    ch -q "
        CREATE TABLE IF NOT EXISTS web_requests
        (
            timestamp DateTime,
            user_id UInt64,
            url LowCardinality(String),
            status UInt16,
            latency_ms UInt32,
            bytes_sent UInt64
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (url, timestamp)"

    ch -q "
        INSERT INTO web_requests
        SELECT
            now() - INTERVAL rand() % 86400 SECOND,
            rand() % 1000000,
            ['/index.html', '/api/v1/users', '/api/v1/orders', '/static/app.js',
             '/login', '/search', '/api/v1/products', '/checkout'][1 + rand() % 8],
            [200, 200, 200, 200, 301, 404, 500][1 + rand() % 7],
            rand() % 2000,
            rand() % 1000000
        FROM numbers_mt(50000000)"

    # Two replicas of one table on the same server: inserts into r1 make r2
    # fetch parts, populating system.replication_queue and system.fetches.
    for r in r1 r2; do
        ch -q "
            CREATE TABLE IF NOT EXISTS events_$r
            (
                timestamp DateTime,
                event_type LowCardinality(String),
                payload String
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/events', '$r')
            ORDER BY timestamp"
    done

    ch -q "
        CREATE TABLE IF NOT EXISTS countries
        (
            code String,
            name String,
            population UInt64
        )
        ENGINE = MergeTree ORDER BY code"
    ch -q "
        INSERT INTO countries VALUES
        ('US', 'United States', 331000000), ('DE', 'Germany', 83000000),
        ('FR', 'France', 67000000), ('NL', 'Netherlands', 17000000),
        ('ES', 'Spain', 47000000), ('JP', 'Japan', 125000000)"
    ch -q "
        CREATE DICTIONARY IF NOT EXISTS countries_dict
        (
            code String,
            name String,
            population UInt64
        )
        PRIMARY KEY code
        SOURCE(CLICKHOUSE(TABLE 'countries' DB '$DB'))
        LIFETIME(MIN 60 MAX 120)
        LAYOUT(COMPLEX_KEY_HASHED())"
    ch -q "SELECT dictGet('$DB.countries_dict', 'name', 'US') FORMAT Null"
}

# Endless loops; each writes its PID into $PID_FILE.
loop_heavy_selects() {
    local queries=(
        "SELECT url, count() AS hits, round(quantile(0.95)(latency_ms)) AS p95, formatReadableSize(sum(bytes_sent)) AS traffic FROM web_requests WHERE timestamp > now() - INTERVAL 1 DAY GROUP BY url ORDER BY hits DESC LIMIT 10 FORMAT Null"
        "SELECT user_id, count() AS requests, countIf(status >= 500) AS errors FROM web_requests GROUP BY user_id HAVING errors > 0 ORDER BY errors DESC LIMIT 100 FORMAT Null"
        "SELECT toStartOfHour(timestamp) AS hour, status, count() FROM web_requests GROUP BY hour, status ORDER BY hour FORMAT Null"
        "SELECT sum(sipHash64(user_id, url, latency_ms)) FROM web_requests FORMAT Null"
    )
    while true; do
        for q in "${queries[@]}"; do
            ch "${PROFILER_SETTINGS[@]}" --max_threads=4 -q "$q" ||:
        done
    done
}

loop_inserts() {
    while true; do
        ch -q "
            INSERT INTO web_requests
            SELECT now(), rand() % 1000000,
                ['/index.html', '/api/v1/users', '/search'][1 + rand() % 3],
                200, rand() % 2000, rand() % 1000000
            FROM numbers(500000)" ||:
        ch -q "
            INSERT INTO events_r1
            SELECT now(), ['click', 'view', 'purchase'][1 + rand() % 3], randomPrintableASCII(100)
            FROM numbers(100000)" ||:
        sleep 2
    done
}

loop_merges_and_mutations() {
    while true; do
        ch -q "OPTIMIZE TABLE web_requests PARTITION tuple(toYYYYMMDD(now()))" ||:
        ch --mutations_sync=0 -q "
            ALTER TABLE web_requests
            UPDATE latency_ms = latency_ms + 1
            WHERE status = 500 AND timestamp > now() - INTERVAL 1 HOUR" ||:
        sleep 30
    done
}

loop_errors() {
    while true; do
        ch -q "SELECT * FROM no_such_table" 2>/dev/null ||:
        ch -q "SELECT throwIf(1, 'demo error for chdig screenshots')" 2>/dev/null ||:
        sleep 5
    done
}

run() {
    stop 2>/dev/null ||:
    : > "$PID_FILE"
    for fn in loop_heavy_selects loop_inserts loop_merges_and_mutations loop_errors; do
        $fn & echo $! >> "$PID_FILE"
    done
    echo "Load generators started (PIDs in $PID_FILE)"
}

stop() {
    if [[ -f "$PID_FILE" ]]; then
        while read -r pid; do
            pkill -P "$pid" 2>/dev/null ||:
            kill "$pid" 2>/dev/null ||:
        done < "$PID_FILE"
        rm -f "$PID_FILE"
        echo "Load generators stopped"
    fi
}

clean() {
    stop
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB"
}

case "${1:-}" in
    setup|run|stop|clean) "$1" ;;
    *) echo "Usage: $0 setup|run|stop|clean" >&2; exit 1 ;;
esac
