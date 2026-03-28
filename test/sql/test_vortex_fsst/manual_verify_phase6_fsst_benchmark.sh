#!/bin/bash
# Phase 6: FSST Scenario Benchmark (v2)
#
# Goal: Find the "benefit window" where FSST encoding outperforms DICT/PLAIN.
#
# Key insight: FSST is triggered only when cardinality > 70% of row_count
# (per data page), so every row must be unique. But the strings must share
# common substring patterns for FSST to compress effectively.
#
# Test design:
#   - 1M rows per pattern (large enough for stable measurement)
#   - All strings are globally unique (high cardinality → defeats DICT)
#   - Strings are long (100-200 bytes) with small variable parts
#   - Variable part (unique ID) is kept short relative to template
#
# Patterns tested:
#   1. Structured log lines (95% template, 5% variable)
#   2. JSON-like records (90% schema, 10% variable)
#   3. URL with long common prefix (80% shared, 20% variable)
#   4. Random hex strings (negative case: no repeating patterns)
#
# Prerequisites:
#   - StarRocks cluster running (FE at 127.0.0.1:9030)
#   - mysql client available
#
# Usage: bash test/sql/test_vortex_fsst/manual_verify_phase6_fsst_benchmark.sh

set -euo pipefail

MYSQL="mysql -h 127.0.0.1 -P 9030 -u root --batch -N"
DB="test_phase6_fsst_v2"
MYSQL_DB="$MYSQL $DB"
NROWS=1000000
PASS=0
FAIL=0

check() {
    local desc="$1"
    local expected="$2"
    local actual="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "  PASS: $desc (expected=$expected, got=$actual)"
        ((PASS++)) || true
    else
        echo "  FAIL: $desc (expected=$expected, got=$actual)"
        ((FAIL++)) || true
    fi
}

echo "=== Phase 6: FSST Scenario Benchmark (v2) ==="
echo "  Rows per pattern: ${NROWS}"
echo ""

# --- Setup ---
echo "--- Setup ---"
$MYSQL -e "CREATE DATABASE IF NOT EXISTS $DB;"

for TBL in t_log_fsst t_log_base t_json_fsst t_json_base t_url_fsst t_url_base t_rand_fsst t_rand_base; do
    $MYSQL_DB -e "DROP TABLE IF EXISTS $TBL;" 2>/dev/null || true
done

# Create table pairs (FSST + baseline)
for NAME in t_log t_json t_url t_rand; do
    echo "  Creating ${NAME}_fsst and ${NAME}_base..."
    $MYSQL_DB -e "
    CREATE TABLE ${NAME}_fsst (
        k1 INT,
        val VARCHAR(1024)
    )
    DUPLICATE KEY(k1)
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES('replication_num' = '1', 'enable_fsst_encoding' = 'true');
    "
    $MYSQL_DB -e "
    CREATE TABLE ${NAME}_base (
        k1 INT,
        val VARCHAR(1024)
    )
    DUPLICATE KEY(k1)
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES('replication_num' = '1');
    "
done

# --- Data Loading ---
echo ""
echo "--- Data Loading ---"

BATCH=100000
for START in $(seq 1 $BATCH $NROWS); do
    END=$((START + BATCH - 1))
    if [ $END -gt $NROWS ]; then END=$NROWS; fi

    # Pattern 1: Structured log lines
    # Template: "2026-03-24T{HH}:{MM}:{SS}.{ms} {LEVEL} [{thread}] com.example.service.{Class} - {Method} /api/v2/{resource}/{id} completed status={code} latency={n}ms trace_id={hex}"
    # ~95% of bytes are from template, ~5% variable (the id and trace_id)
    # Every row is unique (generate_series in id + trace_id)
    $MYSQL_DB -e "
    INSERT INTO t_log_fsst
    SELECT generate_series AS k1,
        CONCAT(
            '2026-03-24T',
            LPAD(CAST(generate_series % 24 AS VARCHAR), 2, '0'), ':',
            LPAD(CAST(generate_series % 60 AS VARCHAR), 2, '0'), ':',
            LPAD(CAST(generate_series % 60 AS VARCHAR), 2, '0'), '.',
            LPAD(CAST(generate_series % 1000 AS VARCHAR), 3, '0'), ' ',
            CASE generate_series % 4
                WHEN 0 THEN 'INFO '
                WHEN 1 THEN 'WARN '
                WHEN 2 THEN 'DEBUG'
                WHEN 3 THEN 'ERROR'
            END, ' [',
            CASE generate_series % 8
                WHEN 0 THEN 'http-nio-8080-exec-'
                WHEN 1 THEN 'http-nio-8080-exec-'
                WHEN 2 THEN 'async-task-worker-'
                WHEN 3 THEN 'scheduled-pool-'
                WHEN 4 THEN 'kafka-consumer-'
                WHEN 5 THEN 'grpc-server-worker-'
                WHEN 6 THEN 'http-nio-8080-exec-'
                WHEN 7 THEN 'event-loop-thread-'
            END, CAST(generate_series % 64 AS VARCHAR),
            '] com.example.service.',
            CASE generate_series % 6
                WHEN 0 THEN 'UserController'
                WHEN 1 THEN 'OrderService'
                WHEN 2 THEN 'PaymentGateway'
                WHEN 3 THEN 'NotificationHandler'
                WHEN 4 THEN 'InventoryManager'
                WHEN 5 THEN 'AuthenticationFilter'
            END,
            ' - ',
            CASE generate_series % 4
                WHEN 0 THEN 'handleRequest GET'
                WHEN 1 THEN 'processOrder POST'
                WHEN 2 THEN 'validatePayment POST'
                WHEN 3 THEN 'sendNotification POST'
            END,
            ' /api/v2/',
            CASE generate_series % 5
                WHEN 0 THEN 'users'
                WHEN 1 THEN 'orders'
                WHEN 2 THEN 'payments'
                WHEN 3 THEN 'products'
                WHEN 4 THEN 'notifications'
            END, '/',
            CAST(generate_series AS VARCHAR),
            ' completed status=',
            CASE generate_series % 10
                WHEN 0 THEN '200'
                WHEN 1 THEN '200'
                WHEN 2 THEN '200'
                WHEN 3 THEN '200'
                WHEN 4 THEN '201'
                WHEN 5 THEN '200'
                WHEN 6 THEN '200'
                WHEN 7 THEN '404'
                WHEN 8 THEN '500'
                WHEN 9 THEN '200'
            END,
            ' latency=', CAST(generate_series % 999 AS VARCHAR), 'ms',
            ' trace_id=', LOWER(HEX(generate_series))
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    # Pattern 2: JSON-like records
    # Template: {"user_id":N,"action":"X","timestamp":"T","metadata":{"ip":"A.B.C.D","user_agent":"Mozilla/5.0 ... Chrome/1XX","session":"sess_N","request_path":"/dashboard/analytics/overview"}}
    # Every row unique via user_id + session
    $MYSQL_DB -e "
    INSERT INTO t_json_fsst
    SELECT generate_series AS k1,
        CONCAT(
            '{\"user_id\":', CAST(generate_series AS VARCHAR),
            ',\"action\":\"',
            CASE generate_series % 5
                WHEN 0 THEN 'page_view'
                WHEN 1 THEN 'button_click'
                WHEN 2 THEN 'form_submit'
                WHEN 3 THEN 'search_query'
                WHEN 4 THEN 'file_download'
            END,
            '\",\"timestamp\":\"2026-03-24T',
            LPAD(CAST(generate_series % 24 AS VARCHAR), 2, '0'), ':',
            LPAD(CAST(generate_series % 60 AS VARCHAR), 2, '0'), ':',
            LPAD(CAST(generate_series % 60 AS VARCHAR), 2, '0'),
            '\",\"metadata\":{\"ip\":\"10.',
            CAST(generate_series % 256 AS VARCHAR), '.',
            CAST((generate_series / 256) % 256 AS VARCHAR), '.',
            CAST((generate_series / 65536) % 256 AS VARCHAR),
            '\",\"user_agent\":\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/',
            CAST(100 + generate_series % 30 AS VARCHAR),
            '.0.0.0 Safari/537.36\",\"session\":\"sess_',
            CAST(generate_series AS VARCHAR),
            '\",\"request_path\":\"/dashboard/',
            CASE generate_series % 4
                WHEN 0 THEN 'analytics/overview'
                WHEN 1 THEN 'settings/profile'
                WHEN 2 THEN 'reports/monthly'
                WHEN 3 THEN 'admin/users/list'
            END,
            '\"}}'
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    # Pattern 3: URLs with long shared prefix
    # Template: https://api.production.example.com/v3/internal/microservice/{svc}/resources/{type}/{id}?auth_token=bearer_{token}&format=json&include=metadata,relations&page_size=100
    $MYSQL_DB -e "
    INSERT INTO t_url_fsst
    SELECT generate_series AS k1,
        CONCAT(
            'https://api.production.example.com/v3/internal/microservice/',
            CASE generate_series % 6
                WHEN 0 THEN 'user-management'
                WHEN 1 THEN 'order-processing'
                WHEN 2 THEN 'payment-gateway'
                WHEN 3 THEN 'inventory-service'
                WHEN 4 THEN 'notification-hub'
                WHEN 5 THEN 'analytics-engine'
            END,
            '/resources/',
            CASE generate_series % 4
                WHEN 0 THEN 'profiles'
                WHEN 1 THEN 'transactions'
                WHEN 2 THEN 'invoices'
                WHEN 3 THEN 'documents'
            END, '/',
            CAST(generate_series AS VARCHAR),
            '?auth_token=bearer_',
            LOWER(HEX(generate_series % 65536)),
            '&format=json&include=metadata,relations&page_size=100&offset=',
            CAST((generate_series % 100) * 100 AS VARCHAR)
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    # Pattern 4: Random hex strings (negative case — no shared substrings)
    # Each string is essentially random: HEX(generate_series) padded to ~40 chars
    $MYSQL_DB -e "
    INSERT INTO t_rand_fsst
    SELECT generate_series AS k1,
        CONCAT(
            LOWER(HEX(generate_series * 7 + 13)),
            LOWER(HEX(generate_series * 31 + 97)),
            LOWER(HEX(generate_series * 127 + 251)),
            LOWER(HEX(generate_series * 511 + 1021)),
            LOWER(HEX(generate_series * 2047 + 4093)),
            LOWER(HEX(generate_series * 8191 + 16381))
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    echo "  Inserted rows ${START}-${END} (all 4 patterns)"
done

# Copy to baseline tables
echo ""
echo "--- Copying to baseline tables ---"
for NAME in t_log t_json t_url t_rand; do
    $MYSQL_DB -e "INSERT INTO ${NAME}_base SELECT * FROM ${NAME}_fsst;"
    echo "  Copied ${NAME}"
done

# --- Test 1: Row Count ---
echo ""
echo "--- Test 1: Row Count Verification ---"
for PATTERN in log json url rand; do
    CNT_FSST=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_${PATTERN}_fsst;")
    CNT_BASE=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_${PATTERN}_base;")
    check "${PATTERN} FSST row count" "$NROWS" "$CNT_FSST"
    check "${PATTERN} baseline row count" "$NROWS" "$CNT_BASE"
done

# --- Test 2: Data Correctness ---
echo ""
echo "--- Test 2: Data Correctness (FSST == Baseline) ---"
for PATTERN in log json url rand; do
    # Spot check: SUM of lengths must match
    LEN_FSST=$($MYSQL_DB -e "SELECT SUM(LENGTH(val)) FROM t_${PATTERN}_fsst;")
    LEN_BASE=$($MYSQL_DB -e "SELECT SUM(LENGTH(val)) FROM t_${PATTERN}_base;")
    check "${PATTERN} SUM(LENGTH) match" "$LEN_BASE" "$LEN_FSST"
done

# --- Test 3: Query Correctness ---
echo ""
echo "--- Test 3: Query Correctness ---"

# Log: exact match on a pattern
CNT_FSST=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_log_fsst WHERE val LIKE '%ERROR%';")
CNT_BASE=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_log_base WHERE val LIKE '%ERROR%';")
check "log LIKE ERROR count" "$CNT_BASE" "$CNT_FSST"

# JSON: filter on action
CNT_FSST=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_json_fsst WHERE val LIKE '%page_view%';")
CNT_BASE=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_json_base WHERE val LIKE '%page_view%';")
check "json LIKE page_view count" "$CNT_BASE" "$CNT_FSST"

# URL: filter on service name
CNT_FSST=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_url_fsst WHERE val LIKE '%payment-gateway%';")
CNT_BASE=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_url_base WHERE val LIKE '%payment-gateway%';")
check "url LIKE payment-gateway count" "$CNT_BASE" "$CNT_FSST"

# --- Test 4: DATA_SIZE Comparison ---
echo ""
echo "--- Test 4: DATA_SIZE Comparison ---"
sleep 3

echo ""
echo "  Avg string lengths:"
for PATTERN in log json url rand; do
    AVG_LEN=$($MYSQL_DB -e "SELECT ROUND(AVG(LENGTH(val)), 1) FROM t_${PATTERN}_fsst;")
    echo "    ${PATTERN}: ${AVG_LEN} bytes"
done

echo ""
printf "  %-8s %14s %14s %8s %s\n" "Pattern" "FSST(bytes)" "Base(bytes)" "Ratio" "Verdict"
printf "  %-8s %14s %14s %8s %s\n" "-------" "-----------" "-----------" "-----" "-------"

for PATTERN in log json url rand; do
    SIZE_FSST=$($MYSQL_DB -e "
    SELECT SUM(DATA_SIZE) FROM information_schema.be_tablets t
    JOIN information_schema.tables_config c ON t.TABLE_ID = c.TABLE_ID
    WHERE c.TABLE_NAME = 't_${PATTERN}_fsst' AND c.TABLE_SCHEMA = '$DB';
    ")
    SIZE_BASE=$($MYSQL_DB -e "
    SELECT SUM(DATA_SIZE) FROM information_schema.be_tablets t
    JOIN information_schema.tables_config c ON t.TABLE_ID = c.TABLE_ID
    WHERE c.TABLE_NAME = 't_${PATTERN}_base' AND c.TABLE_SCHEMA = '$DB';
    ")
    RATIO=$(awk "BEGIN {printf \"%.3f\", $SIZE_FSST / ($SIZE_BASE + 0.001)}")
    if awk "BEGIN {exit !($RATIO < 0.95)}"; then
        VERDICT="FSST wins"
    elif awk "BEGIN {exit !($RATIO > 1.05)}"; then
        VERDICT="Baseline wins"
    else
        VERDICT="~same"
    fi
    printf "  %-8s %14s %14s %7sx %s\n" "$PATTERN" "$SIZE_FSST" "$SIZE_BASE" "$RATIO" "$VERDICT"
done

# --- Test 5: Query Performance (3 runs, take median) ---
echo ""
echo "--- Test 5: Full Scan Performance (wall time, 3 runs) ---"
for PATTERN in log json url; do
    echo "  ${PATTERN}:"
    for SUFFIX in fsst base; do
        TIMES=()
        for RUN in 1 2 3; do
            MS=$($MYSQL_DB -e "
            SET enable_profile = false;
            SELECT COUNT(*) FROM t_${PATTERN}_${SUFFIX} WHERE val LIKE '%status=200%' OR val LIKE '%page_view%' OR val LIKE '%payment%';
            " 2>/dev/null | tail -1)
            # Use query time from client perspective
            START_MS=$(date +%s%N)
            $MYSQL_DB -e "SELECT COUNT(*) FROM t_${PATTERN}_${SUFFIX} WHERE val LIKE '%status=200%' OR val LIKE '%page_view%' OR val LIKE '%payment%';" > /dev/null 2>&1
            END_MS=$(date +%s%N)
            ELAPSED=$(( (END_MS - START_MS) / 1000000 ))
            TIMES+=($ELAPSED)
        done
        # Sort and take median
        IFS=$'\n' SORTED=($(sort -n <<<"${TIMES[*]}")); unset IFS
        MEDIAN=${SORTED[1]}
        echo "    ${SUFFIX}: ${TIMES[0]}ms / ${TIMES[1]}ms / ${TIMES[2]}ms (median=${MEDIAN}ms)"
    done
done

# --- Cleanup ---
echo ""
echo "--- Cleanup ---"
for TBL in t_log_fsst t_log_base t_json_fsst t_json_base t_url_fsst t_url_base t_rand_fsst t_rand_base; do
    $MYSQL_DB -e "DROP TABLE IF EXISTS $TBL;" 2>/dev/null || true
done
$MYSQL -e "DROP DATABASE IF EXISTS $DB;" 2>/dev/null || true
echo "  Cleaned up."

# --- Summary ---
echo ""
echo "=== Phase 6 FSST Benchmark v2 Summary ==="
echo "  $PASS passed, $FAIL failed out of $((PASS + FAIL)) tests"
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
