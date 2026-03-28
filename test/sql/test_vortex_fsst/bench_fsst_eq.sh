#!/bin/bash
# Rigorous FSST EQ benchmark
# - Disables compaction
# - Drops all caches (Linux + StarRocks page cache) before each query type
# - Creates fresh tables each run
# - Idempotent: same result every time
set -euo pipefail
export LC_ALL=en_US.UTF-8

MYSQL="mysql -h 127.0.0.1 -P 9030 -u root --batch -N"
BE_HOST="127.0.0.1"
BE_HTTP_PORT="8040"
DB="bench_fsst_eq"
NROWS=${1:-2000000}
BATCH=200000
RUNS=7

echo "=========================================="
echo "  FSST EQ Benchmark (rigorous)"
echo "  Rows: $NROWS"
echo "=========================================="

# --- Step 0: Disable compaction ---
echo ""
echo "--- Step 0: Disable compaction ---"
curl -s -XPOST "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?max_compaction_concurrency=0" || true
echo "  Compaction disabled (max_compaction_concurrency=0)"

# --- Step 1: Fresh data ---
echo ""
echo "--- Step 1: Create fresh tables + load data ---"
$MYSQL -e "DROP DATABASE IF EXISTS $DB;"
$MYSQL -e "CREATE DATABASE $DB;"

$MYSQL $DB -e "
CREATE TABLE t_fsst (k1 INT, val VARCHAR(1024))
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES('replication_num' = '1', 'enable_fsst_encoding' = 'true');
"
$MYSQL $DB -e "
CREATE TABLE t_base (k1 INT, val VARCHAR(1024))
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES('replication_num' = '1');
"

for START in $(seq 1 $BATCH $NROWS); do
    END=$((START + BATCH - 1))
    if [ $END -gt $NROWS ]; then END=$NROWS; fi
    $MYSQL $DB -e "
    INSERT INTO t_fsst
    SELECT generate_series AS k1,
        CONCAT(
            'GET /api/v2/internal/microservice/',
            CASE generate_series % 6
                WHEN 0 THEN 'user-management' WHEN 1 THEN 'order-processing'
                WHEN 2 THEN 'payment-gateway' WHEN 3 THEN 'inventory-service'
                WHEN 4 THEN 'notification-hub' WHEN 5 THEN 'analytics-engine'
            END,
            '/resources/',
            CASE generate_series % 4
                WHEN 0 THEN 'profiles' WHEN 1 THEN 'transactions'
                WHEN 2 THEN 'invoices' WHEN 3 THEN 'documents'
            END,
            ' HTTP/1.1 Host: api.production.example.com Accept: application/json Authorization: Bearer token_',
            CAST(generate_series AS VARCHAR),
            ' X-Request-Id: req-',
            CAST(generate_series AS VARCHAR),
            '-', LOWER(HEX(generate_series)),
            ' X-Correlation-Id: corr-',
            CAST(generate_series AS VARCHAR),
            '-', LOWER(HEX(generate_series * 7 + 13))
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "
done
$MYSQL $DB -e "INSERT INTO t_base SELECT * FROM t_fsst;"
echo "  Data loaded: $NROWS rows"

# Wait for writes to settle (no compaction running)
sleep 3

# --- Step 2: Verify ---
echo ""
echo "--- Step 2: Verify data ---"
CNT_F=$($MYSQL $DB -e "SELECT COUNT(*) FROM t_fsst;")
CNT_B=$($MYSQL $DB -e "SELECT COUNT(*) FROM t_base;")
echo "  FSST rows: $CNT_F, Base rows: $CNT_B"

SIZE_FSST=$($MYSQL $DB -e "
SELECT SUM(DATA_SIZE) FROM information_schema.be_tablets t
JOIN information_schema.tables_config c ON t.TABLE_ID = c.TABLE_ID
WHERE c.TABLE_NAME = 't_fsst' AND c.TABLE_SCHEMA = '$DB';
")
SIZE_BASE=$($MYSQL $DB -e "
SELECT SUM(DATA_SIZE) FROM information_schema.be_tablets t
JOIN information_schema.tables_config c ON t.TABLE_ID = c.TABLE_ID
WHERE c.TABLE_NAME = 't_base' AND c.TABLE_SCHEMA = '$DB';
")
RATIO=$(awk "BEGIN {printf \"%.3f\", $SIZE_FSST / ($SIZE_BASE + 0.001)}")
echo "  FSST size: ${SIZE_FSST}, Base size: ${SIZE_BASE}, Ratio: ${RATIO}"

AVG_LEN=$($MYSQL $DB -e "SELECT ROUND(AVG(LENGTH(val)), 1) FROM t_fsst;")
echo "  Avg string length: ${AVG_LEN}"

# Get a target value for EQ
TARGET_K1=$((NROWS / 2))
EXACT_VAL=$($MYSQL $DB -e "SELECT val FROM t_fsst WHERE k1 = ${TARGET_K1} LIMIT 1;")
EXACT_VAL_ESC=$(echo "$EXACT_VAL" | sed "s/'/''/g")
echo "  Target k1=${TARGET_K1}, string length: ${#EXACT_VAL}"

# --- Helper: drop all caches ---
drop_caches() {
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    # Disable then re-enable StarRocks page cache to flush it
    curl -s -XPOST "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?disable_storage_page_cache=true" > /dev/null 2>&1 || true
    sleep 1
    curl -s -XPOST "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?disable_storage_page_cache=false" > /dev/null 2>&1 || true
}

# --- Helper: run benchmark ---
run_bench() {
    local label="$1"
    local sql="$2"

    # Drop all caches before this query type
    drop_caches

    local times=()
    for run in $(seq 1 $RUNS); do
        local start_ns=$(date +%s%N)
        $MYSQL $DB -e "$sql" > /dev/null 2>&1
        local end_ns=$(date +%s%N)
        local elapsed=$(( (end_ns - start_ns) / 1000000 ))
        times+=($elapsed)
    done
    IFS=$'\n' sorted=($(sort -n <<<"${times[*]}")); unset IFS
    local median=${sorted[$((RUNS / 2))]}
    local cold=${times[0]}
    local hot=${sorted[0]}
    printf "  %-20s cold=%4sms  hot=%4sms  median=%4sms  [%s]\n" \
        "$label" "$cold" "$hot" "$median" "${times[*]}"
}

# --- Step 3: Run benchmarks ---
echo ""
echo "=========================================="
echo "  Benchmark Results ($NROWS rows)"
echo "=========================================="

echo ""
echo "--- EQ (high selectivity: 1/$NROWS) ---"
run_bench "FSST_EQ" "SELECT COUNT(*) FROM t_fsst WHERE val = '${EXACT_VAL_ESC}';"
run_bench "BASE_EQ" "SELECT COUNT(*) FROM t_base WHERE val = '${EXACT_VAL_ESC}';"

echo ""
echo "--- FULL SCAN ---"
run_bench "FSST_FULL" "SELECT COUNT(LENGTH(val)) FROM t_fsst;"
run_bench "BASE_FULL" "SELECT COUNT(LENGTH(val)) FROM t_base;"

echo ""
echo "--- LIKE (substring, ~16.7% selectivity) ---"
run_bench "FSST_LIKE" "SELECT COUNT(*) FROM t_fsst WHERE val LIKE '%payment-gateway%';"
run_bench "BASE_LIKE" "SELECT COUNT(*) FROM t_base WHERE val LIKE '%payment-gateway%';"

# --- Step 4: Perf profile FSST EQ (hot cache, no compaction) ---
echo ""
echo "--- Perf: FSST EQ (query thread only, hot cache) ---"
BE_PID=$(pgrep -f starrocks_be | head -1)
if [ -n "$BE_PID" ]; then
    # Warm cache
    for i in 1 2 3; do
        $MYSQL $DB -e "SELECT COUNT(*) FROM t_fsst WHERE val = '${EXACT_VAL_ESC}';" > /dev/null 2>&1
    done

    perf record -F 4999 -p "$BE_PID" -g -o /root/perf_results/bench_eq.perf.data -- sleep 30 &
    PERF_PID=$!
    sleep 0.3
    for i in $(seq 1 7); do
        $MYSQL $DB -e "SELECT COUNT(*) FROM t_fsst WHERE val = '${EXACT_VAL_ESC}';" > /dev/null 2>&1
    done
    kill $PERF_PID 2>/dev/null; wait $PERF_PID 2>/dev/null || true

    echo "  Query thread hotspots (pip_scan_com):"
    perf report -i /root/perf_results/bench_eq.perf.data --stdio --no-children -n \
        --percent-limit=0.5 --comm=pip_scan_com 2>/dev/null | grep -E "^\s+[0-9]" | head -15
fi

# --- Cleanup ---
echo ""
echo "--- Cleanup ---"
$MYSQL -e "DROP DATABASE IF EXISTS $DB;"
curl -s -XPOST "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?max_compaction_concurrency=-1" > /dev/null 2>&1 || true
echo "  Compaction re-enabled. Database dropped. Done."
