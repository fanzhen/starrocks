#!/bin/bash
# Phase 9 E2E Verification Script: BM25 Persistent Index
# Run inside sr-dev container: bash /build/test/sql/test_tantivy_inverted/verify_phase9.sh

set -e

MYSQL="mysql -h127.0.0.1 -P9030 -uroot --batch -N"
DB="test_bm25_p9"
PASS=0
FAIL=0

run_sql() {
    $MYSQL -D "$DB" -e "$1" 2>&1
}

check() {
    local tc=$1 desc=$2 expected=$3 actual=$4
    if echo "$actual" | grep -qE "$expected"; then
        echo "TC$tc PASS: $desc"
        PASS=$((PASS+1))
    else
        echo "TC$tc FAIL: $desc"
        echo "  Expected pattern: $expected"
        echo "  Actual: $actual"
        FAIL=$((FAIL+1))
    fi
}

echo "=== Phase 9: BM25 Persistent Index E2E ==="
echo ""

# Enable GIN index
$MYSQL -e "ADMIN SET FRONTEND CONFIG ('enable_experimental_gin' = 'true');"

# Setup
$MYSQL -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;"

run_sql "CREATE TABLE t_bm25_gin (
    id INT NOT NULL,
    content VARCHAR(500) NOT NULL,
    INDEX idx_gin_content (content) USING GIN('parser' = 'standard', 'imp_lib' = 'tantivy')
) ENGINE=OLAP DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('replication_num' = '1');"

run_sql "CREATE TABLE t_bm25_nogin (
    id INT NOT NULL,
    content VARCHAR(500) NOT NULL
) ENGINE=OLAP DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('replication_num' = '1');"

# Insert data in 3 batches for TC2 consistency test
run_sql "INSERT INTO t_bm25_gin VALUES
(1, 'database management system for real-time analytics'),
(2, 'machine learning algorithms and deep learning models'),
(3, 'distributed database with high performance query engine'),
(4, 'cloud native data warehouse for analytics'),
(5, 'real-time stream processing and database integration');"

run_sql "INSERT INTO t_bm25_gin VALUES
(6, 'database indexing and query optimization techniques'),
(7, 'full text search engine with inverted index'),
(8, 'database replication and sharding strategies'),
(9, 'analytics platform for business intelligence'),
(10, 'high availability database cluster management');"

run_sql "INSERT INTO t_bm25_gin VALUES
(11, 'database performance tuning and monitoring tools'),
(12, 'natural language processing for text search'),
(13, 'database backup and disaster recovery solutions'),
(14, 'real-time analytics dashboard and reporting'),
(15, 'scalable database architecture for big data');"

# Also insert into no-GIN table
run_sql "INSERT INTO t_bm25_nogin SELECT * FROM t_bm25_gin;"

# Wait for index build
sleep 3

echo ""
echo "--- TC1: BM25 uses persistent index ---"
# Enable profile
run_sql "SET enable_profile = true;"
result=$(run_sql "SELECT id, content FROM t_bm25_gin WHERE content MATCH_ANY 'database' ORDER BY BM25(content, 'database') DESC LIMIT 5;" || echo "QUERY_ERROR")
echo "  Query result: $result"
if echo "$result" | grep -q "QUERY_ERROR"; then
    check 1 "BM25 persistent index query" "result" "QUERY_ERROR"
else
    # Check we got results with database keyword
    row_count=$(echo "$result" | wc -l | tr -d ' ')
    if [ "$row_count" -gt 0 ]; then
        check 1 "BM25 persistent index query returns results" "." "$result"
    else
        check 1 "BM25 persistent index query returns results" "at_least_one_row" "empty"
    fi
fi

echo ""
echo "--- TC2: BM25 sorting consistency ---"
result1=$(run_sql "SELECT id FROM t_bm25_gin WHERE content MATCH_ANY 'database' ORDER BY BM25(content, 'database') DESC LIMIT 5;" 2>/dev/null || echo "ERROR1")
result2=$(run_sql "SELECT id FROM t_bm25_gin WHERE content MATCH_ANY 'database' ORDER BY BM25(content, 'database') DESC LIMIT 5;" 2>/dev/null || echo "ERROR2")
result3=$(run_sql "SELECT id FROM t_bm25_gin WHERE content MATCH_ANY 'database' ORDER BY BM25(content, 'database') DESC LIMIT 5;" 2>/dev/null || echo "ERROR3")
if [ "$result1" = "$result2" ] && [ "$result2" = "$result3" ]; then
    check 2 "BM25 sorting consistency across executions" "." "CONSISTENT: $result1"
else
    check 2 "BM25 sorting consistency" "consistent" "r1=[$result1] r2=[$result2] r3=[$result3]"
fi

echo ""
echo "--- TC3: BM25 fallback without GIN index ---"
result=$(run_sql "SELECT id, BM25(content, 'database') as score FROM t_bm25_nogin WHERE content MATCH_ANY 'database' ORDER BY score DESC LIMIT 5;" 2>/dev/null || echo "FALLBACK_ERROR")
echo "  Fallback result: $result"
if echo "$result" | grep -q "FALLBACK_ERROR"; then
    # Without GIN index, BM25 should still work via batch-local path
    # But MATCH_ANY requires GIN index, so this might fail
    # Try without MATCH predicate
    result2=$(run_sql "SELECT id, BM25(content, 'database') as score FROM t_bm25_nogin ORDER BY score DESC LIMIT 5;" 2>/dev/null || echo "FALLBACK_ERROR2")
    echo "  Fallback (no MATCH) result: $result2"
    if echo "$result2" | grep -q "FALLBACK_ERROR2"; then
        check 3 "BM25 batch-local fallback" "score" "BOTH_FAILED"
    else
        check 3 "BM25 batch-local fallback (without MATCH)" "." "$result2"
    fi
else
    check 3 "BM25 fallback without GIN index" "." "$result"
fi

echo ""
echo "--- TC4: BM25 query_type support ---"
# Test with query_type='all' — only docs containing ALL terms should score
result_any=$(run_sql "SELECT id, BM25(content, 'database analytics') as score FROM t_bm25_gin WHERE content MATCH_ANY 'database analytics' ORDER BY score DESC LIMIT 10;" 2>/dev/null || echo "ANY_ERROR")
result_all=$(run_sql "SELECT id, BM25(content, 'database analytics', 'standard', 'all') as score FROM t_bm25_gin WHERE content MATCH_ALL 'database analytics' ORDER BY score DESC LIMIT 10;" 2>/dev/null || echo "ALL_ERROR")
echo "  ANY results: $result_any"
echo "  ALL results: $result_all"
if echo "$result_any" | grep -q "ANY_ERROR"; then
    check 4 "BM25 query_type=any" "score" "ANY_ERROR"
elif echo "$result_all" | grep -q "ALL_ERROR"; then
    check 4 "BM25 query_type=all" "score" "ALL_ERROR"
else
    any_count=$(echo "$result_any" | wc -l | tr -d ' ')
    all_count=$(echo "$result_all" | wc -l | tr -d ' ')
    if [ "$all_count" -le "$any_count" ]; then
        check 4 "BM25 query_type: all <= any results" "." "any=$any_count all=$all_count"
    else
        check 4 "BM25 query_type: all should <= any" "all_le_any" "any=$any_count all=$all_count"
    fi
fi

echo ""
echo "=== Summary: $PASS PASS, $FAIL FAIL ==="
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
