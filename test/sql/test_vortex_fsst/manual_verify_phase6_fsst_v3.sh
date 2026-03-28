#!/bin/bash
# Phase 6 v3: FSST Layered Benchmark
#
# Improvements over v2:
#   A1: Verify encoding actually triggered (DATA_SIZE divergence)
#   A2: Separate EQ/NE vs LIKE benchmarks (FSST advantage vs disadvantage windows)
#
# Design:
#   - 500K rows per pattern (enough for stable measurement, faster than 1M)
#   - All strings globally unique (defeats DICT)
#   - 4 patterns: log, json, url, rand
#   - 3 query types: EQ (exact match), NE (exclude one value), LIKE (substring)

set -euo pipefail

MYSQL="mysql -h 127.0.0.1 -P 9030 -u root --batch -N"
DB="test_fsst_v3"
MYSQL_DB="$MYSQL $DB"
NROWS=500000
BATCH=100000

echo "=== Phase 6 v3: FSST Layered Benchmark ==="
echo "  Rows per pattern: ${NROWS}"
echo ""

# --- Setup ---
echo "--- Setup ---"
$MYSQL -e "CREATE DATABASE IF NOT EXISTS $DB;"

for TBL in t_log_fsst t_log_base t_json_fsst t_json_base t_url_fsst t_url_base t_rand_fsst t_rand_base; do
    $MYSQL_DB -e "DROP TABLE IF EXISTS $TBL;" 2>/dev/null || true
done

for NAME in t_log t_json t_url t_rand; do
    $MYSQL_DB -e "
    CREATE TABLE ${NAME}_fsst (k1 INT, val VARCHAR(1024))
    DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES('replication_num' = '1', 'enable_fsst_encoding' = 'true');
    "
    $MYSQL_DB -e "
    CREATE TABLE ${NAME}_base (k1 INT, val VARCHAR(1024))
    DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES('replication_num' = '1');
    "
done
echo "  Tables created."

# --- Data Loading ---
echo ""
echo "--- Data Loading ---"

for START in $(seq 1 $BATCH $NROWS); do
    END=$((START + BATCH - 1))
    if [ $END -gt $NROWS ]; then END=$NROWS; fi

    # Pattern 1: Structured log lines (~184B, moderate template ratio)
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
                WHEN 0 THEN 'INFO ' WHEN 1 THEN 'WARN ' WHEN 2 THEN 'DEBUG' WHEN 3 THEN 'ERROR'
            END, ' [',
            CASE generate_series % 4
                WHEN 0 THEN 'http-nio-8080-exec-'
                WHEN 1 THEN 'async-task-worker-'
                WHEN 2 THEN 'scheduled-pool-'
                WHEN 3 THEN 'kafka-consumer-'
            END, CAST(generate_series % 64 AS VARCHAR),
            '] com.example.service.UserController - handleRequest GET /api/v2/users/',
            CAST(generate_series AS VARCHAR),
            ' completed status=200 latency=', CAST(generate_series % 999 AS VARCHAR), 'ms',
            ' trace_id=', LOWER(HEX(generate_series))
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    # Pattern 2: JSON-like records (~328B, high template ratio)
    $MYSQL_DB -e "
    INSERT INTO t_json_fsst
    SELECT generate_series AS k1,
        CONCAT(
            '{\"user_id\":', CAST(generate_series AS VARCHAR),
            ',\"action\":\"',
            CASE generate_series % 5
                WHEN 0 THEN 'page_view' WHEN 1 THEN 'button_click' WHEN 2 THEN 'form_submit'
                WHEN 3 THEN 'search_query' WHEN 4 THEN 'file_download'
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
                WHEN 0 THEN 'analytics/overview' WHEN 1 THEN 'settings/profile'
                WHEN 2 THEN 'reports/monthly' WHEN 3 THEN 'admin/users/list'
            END,
            '\"}}'
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    # Pattern 3: URLs with long shared prefix (~191B)
    $MYSQL_DB -e "
    INSERT INTO t_url_fsst
    SELECT generate_series AS k1,
        CONCAT(
            'https://api.production.example.com/v3/internal/microservice/',
            CASE generate_series % 6
                WHEN 0 THEN 'user-management' WHEN 1 THEN 'order-processing'
                WHEN 2 THEN 'payment-gateway' WHEN 3 THEN 'inventory-service'
                WHEN 4 THEN 'notification-hub' WHEN 5 THEN 'analytics-engine'
            END,
            '/resources/',
            CASE generate_series % 4
                WHEN 0 THEN 'profiles' WHEN 1 THEN 'transactions'
                WHEN 2 THEN 'invoices' WHEN 3 THEN 'documents'
            END, '/',
            CAST(generate_series AS VARCHAR),
            '?auth_token=bearer_',
            LOWER(HEX(generate_series % 65536)),
            '&format=json&include=metadata,relations&page_size=100&offset=',
            CAST((generate_series % 100) * 100 AS VARCHAR)
        ) AS val
    FROM TABLE(generate_series(${START}, ${END}));
    "

    # Pattern 4: Random hex strings (~43B, negative case)
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

    echo "  Inserted rows ${START}-${END}"
done

# Copy to baseline tables
echo ""
echo "--- Copying to baseline tables ---"
for NAME in t_log t_json t_url t_rand; do
    $MYSQL_DB -e "INSERT INTO ${NAME}_base SELECT * FROM ${NAME}_fsst;"
    echo "  Copied ${NAME}"
done

# --- A1: Encoding Hit Verification ---
echo ""
echo "=== A1: Encoding Hit Verification ==="
echo "  (If FSST_SIZE == BASE_SIZE, FSST encoding was NOT triggered)"
echo ""
sleep 3

printf "  %-8s %14s %14s %8s %s\n" "Pattern" "FSST(bytes)" "Base(bytes)" "Ratio" "Encoding"
printf "  %-8s %14s %14s %8s %s\n" "-------" "-----------" "-----------" "-----" "--------"

declare -A ENCODING_HIT
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

    # If ratio is within 2% of 1.0, FSST likely not triggered
    if awk "BEGIN {exit !($RATIO > 0.98 && $RATIO < 1.02)}"; then
        ENC="PLAIN (FSST NOT triggered)"
        ENCODING_HIT[$PATTERN]="no"
    else
        ENC="FSST ACTIVE"
        ENCODING_HIT[$PATTERN]="yes"
    fi
    printf "  %-8s %14s %14s %7sx %s\n" "$PATTERN" "$SIZE_FSST" "$SIZE_BASE" "$RATIO" "$ENC"
done

echo ""
echo "  Avg string lengths:"
for PATTERN in log json url rand; do
    AVG_LEN=$($MYSQL_DB -e "SELECT ROUND(AVG(LENGTH(val)), 1) FROM t_${PATTERN}_fsst;")
    echo "    ${PATTERN}: ${AVG_LEN} bytes"
done

# --- A2: Layered Query Benchmark ---
echo ""
echo "=== A2: Layered Query Benchmark ==="
echo ""

# Helper: run query 3 times, print median
run_bench() {
    local desc="$1"
    local query="$2"
    local times=()
    for run in 1 2 3; do
        local start_ns=$(date +%s%N)
        $MYSQL_DB -e "$query" > /dev/null 2>&1
        local end_ns=$(date +%s%N)
        local elapsed=$(( (end_ns - start_ns) / 1000000 ))
        times+=($elapsed)
    done
    IFS=$'\n' sorted=($(sort -n <<<"${times[*]}")); unset IFS
    local median=${sorted[1]}
    printf "    %-40s %4sms / %4sms / %4sms  (median=%4sms)\n" "$desc" "${times[0]}" "${times[1]}" "${times[2]}" "$median"
}

# Only benchmark patterns where FSST is actually active
for PATTERN in log json url rand; do
    if [[ "${ENCODING_HIT[$PATTERN]}" == "no" ]]; then
        echo "  [$PATTERN] SKIPPED (FSST not triggered, both tables use PLAIN)"
        echo ""
        continue
    fi

    echo "  [$PATTERN] FSST active — benchmarking:"

    # Pick a known exact value for EQ/NE tests
    EXACT_VAL=$($MYSQL_DB -e "SELECT val FROM t_${PATTERN}_fsst WHERE k1 = 12345 LIMIT 1;")
    # Escape single quotes for SQL
    EXACT_VAL_ESC=$(echo "$EXACT_VAL" | sed "s/'/''/g")

    # --- EQ (FSST advantage: encoded-space comparison) ---
    echo "    [EQ] WHERE val = '<exact_value>':"
    run_bench "fsst" "SELECT COUNT(*) FROM t_${PATTERN}_fsst WHERE val = '${EXACT_VAL_ESC}';"
    run_bench "base" "SELECT COUNT(*) FROM t_${PATTERN}_base WHERE val = '${EXACT_VAL_ESC}';"

    # --- NE (FSST advantage: encoded-space comparison) ---
    echo "    [NE] WHERE val != '<exact_value>':"
    run_bench "fsst" "SELECT COUNT(*) FROM t_${PATTERN}_fsst WHERE val != '${EXACT_VAL_ESC}';"
    run_bench "base" "SELECT COUNT(*) FROM t_${PATTERN}_base WHERE val != '${EXACT_VAL_ESC}';"

    # --- LIKE (FSST disadvantage: must decode) ---
    # Use a pattern-specific substring
    case $PATTERN in
        log)  LIKE_PAT="status=200" ;;
        json) LIKE_PAT="page_view" ;;
        url)  LIKE_PAT="payment-gateway" ;;
        rand) LIKE_PAT="ff" ;;
    esac
    echo "    [LIKE] WHERE val LIKE '%${LIKE_PAT}%':"
    run_bench "fsst" "SELECT COUNT(*) FROM t_${PATTERN}_fsst WHERE val LIKE '%${LIKE_PAT}%';"
    run_bench "base" "SELECT COUNT(*) FROM t_${PATTERN}_base WHERE val LIKE '%${LIKE_PAT}%';"

    # --- Full scan (no predicate) ---
    echo "    [FULL SCAN] SELECT COUNT(LENGTH(val)):"
    run_bench "fsst" "SELECT COUNT(LENGTH(val)) FROM t_${PATTERN}_fsst;"
    run_bench "base" "SELECT COUNT(LENGTH(val)) FROM t_${PATTERN}_base;"

    echo ""
done

# --- Data Correctness Spot Check ---
echo "--- Data Correctness ---"
PASS=0
FAIL=0
check() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "  PASS: $desc"
        ((PASS++)) || true
    else
        echo "  FAIL: $desc (expected=$expected, got=$actual)"
        ((FAIL++)) || true
    fi
}

for PATTERN in log json url rand; do
    CNT=$($MYSQL_DB -e "SELECT COUNT(*) FROM t_${PATTERN}_fsst;")
    check "${PATTERN} row count" "$NROWS" "$CNT"
    LEN_F=$($MYSQL_DB -e "SELECT SUM(LENGTH(val)) FROM t_${PATTERN}_fsst;")
    LEN_B=$($MYSQL_DB -e "SELECT SUM(LENGTH(val)) FROM t_${PATTERN}_base;")
    check "${PATTERN} SUM(LENGTH) match" "$LEN_B" "$LEN_F"
done

# --- Cleanup ---
echo ""
echo "--- Cleanup ---"
for TBL in t_log_fsst t_log_base t_json_fsst t_json_base t_url_fsst t_url_base t_rand_fsst t_rand_base; do
    $MYSQL_DB -e "DROP TABLE IF EXISTS $TBL;" 2>/dev/null || true
done
$MYSQL -e "DROP DATABASE IF EXISTS $DB;" 2>/dev/null || true
echo "  Cleaned up."

echo ""
echo "=== Summary ==="
echo "  Correctness: $PASS passed, $FAIL failed out of $((PASS + FAIL)) tests"
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
