#!/bin/bash
# Phase 3 E2E Verification: MATCH_PHRASE / MATCH_PHRASE_PREFIX / MATCH_REGEXP + tantivy imp_lib
# Goal: verify FE syntax → Thrift opcode → BE dispatch → tantivy query pipeline is transparent
# Run inside Docker container with mysql access

set -euo pipefail

MYSQL="mysql -h127.0.0.1 -P9030 -uroot --batch -N"
PASS=0
FAIL=0
TOTAL=0

run_tc() {
    local name="$1"
    local sql="$2"
    local expect="$3"
    TOTAL=$((TOTAL + 1))
    local result
    result=$($MYSQL -e "$sql" 2>&1) || true
    if echo "$result" | grep -qF "$expect"; then
        echo "  [PASS] TC${TOTAL}: $name"
        PASS=$((PASS + 1))
    else
        echo "  [FAIL] TC${TOTAL}: $name"
        echo "    Expected to contain: $expect"
        echo "    Got:      $result"
        FAIL=$((FAIL + 1))
    fi
}

run_tc_no_error() {
    local name="$1"
    local sql="$2"
    TOTAL=$((TOTAL + 1))
    local result
    result=$($MYSQL -e "$sql" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ] && ! echo "$result" | grep -qi "error"; then
        echo "  [PASS] TC${TOTAL}: $name"
        PASS=$((PASS + 1))
    else
        echo "  [FAIL] TC${TOTAL}: $name"
        echo "    Got error: $result"
        FAIL=$((FAIL + 1))
    fi
}

run_tc_error() {
    local name="$1"
    local sql="$2"
    local expect_error="$3"
    TOTAL=$((TOTAL + 1))
    local result
    result=$($MYSQL -e "$sql" 2>&1) || true
    if echo "$result" | grep -qiF "$expect_error"; then
        echo "  [PASS] TC${TOTAL}: $name"
        PASS=$((PASS + 1))
    else
        echo "  [FAIL] TC${TOTAL}: $name"
        echo "    Expected error containing: $expect_error"
        echo "    Got: $result"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Phase 3 E2E Verification ==="
echo "Goal: FE syntax + Thrift + BE dispatch pipeline is transparent"
echo ""

# --- Setup ---
echo "--- Setup ---"
$MYSQL -e "ADMIN SET FRONTEND CONFIG ('enable_experimental_gin' = 'true');"
echo "  GIN enabled"

$MYSQL -e "CREATE DATABASE IF NOT EXISTS test_tantivy_phase3;"
echo "  Database created"

$MYSQL -e "
DROP TABLE IF EXISTS test_tantivy_phase3.t_articles;
CREATE TABLE test_tantivy_phase3.t_articles (
    id INT NOT NULL,
    title VARCHAR(200) NOT NULL,
    body VARCHAR(2000) NOT NULL,
    tag VARCHAR(50) NOT NULL,
    INDEX idx_body (body) USING GIN('imp_lib' = 'tantivy', 'parser' = 'standard'),
    INDEX idx_tag (tag) USING GIN('imp_lib' = 'tantivy')
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1');
"
echo "  Table created with tantivy GIN indexes"

$MYSQL -e "
INSERT INTO test_tantivy_phase3.t_articles VALUES
(1, 'StarRocks Overview', 'StarRocks is a next-gen sub-second MPP OLAP database for full analytics scenarios', 'database'),
(2, 'Tantivy Search', 'Tantivy is a full-text search engine library inspired by Apache Lucene', 'search'),
(3, 'Database Internals', 'Understanding database storage engines and query processing internals', 'database'),
(4, 'Search Engine Design', 'How to design and build a modern search engine from scratch', 'search'),
(5, 'StarRocks Lakehouse', 'StarRocks provides native lakehouse analytics with iceberg and hudi support', 'lakehouse'),
(6, 'Apache Lucene Guide', 'Apache Lucene is a high-performance text search engine library written in Java', 'search'),
(7, 'Data Warehouse', 'Building a modern data warehouse with columnar storage and vectorized execution', 'database'),
(8, 'Full Text Search', 'Implementing full text search with inverted indexes and BM25 scoring', 'search'),
(9, 'Query Optimization', 'Advanced query optimization techniques including cost-based and rule-based approaches', 'database');
"
echo "  Data inserted (9 rows)"
sleep 3

echo ""
echo "--- Part A: Syntax parsing + EXPLAIN (FE pipeline) ---"

# TC1: MATCH_PHRASE parses and generates correct EXPLAIN
run_tc "MATCH_PHRASE in EXPLAIN" \
    "EXPLAIN SELECT id FROM test_tantivy_phase3.t_articles WHERE body MATCH_PHRASE 'full text search';" \
    "MATCH_PHRASE"

# TC2: MATCH_PHRASE_PREFIX parses and generates correct EXPLAIN
run_tc "MATCH_PHRASE_PREFIX in EXPLAIN" \
    "EXPLAIN SELECT id FROM test_tantivy_phase3.t_articles WHERE body MATCH_PHRASE_PREFIX 'search eng';" \
    "MATCH_PHRASE_PREFIX"

# TC3: MATCH_REGEXP parses and generates correct EXPLAIN
run_tc "MATCH_REGEXP in EXPLAIN" \
    "EXPLAIN SELECT id FROM test_tantivy_phase3.t_articles WHERE body MATCH_REGEXP '.*lucene.*';" \
    "MATCH_REGEXP"

# TC4: NOT MATCH_PHRASE parses correctly
run_tc "NOT MATCH_PHRASE in EXPLAIN" \
    "EXPLAIN SELECT id FROM test_tantivy_phase3.t_articles WHERE NOT body MATCH_PHRASE 'search engine';" \
    "NOT (3: body MATCH_PHRASE"

echo ""
echo "--- Part B: Query execution (full pipeline FE→Thrift→BE→tantivy) ---"

# TC5: MATCH_PHRASE executes without error and returns correct count
run_tc "MATCH_PHRASE returns correct count" \
    "SELECT COUNT(*) FROM test_tantivy_phase3.t_articles WHERE body MATCH_PHRASE 'search engine';" \
    "3"

# TC6: MATCH_PHRASE_PREFIX executes without error
run_tc_no_error "MATCH_PHRASE_PREFIX executes" \
    "SELECT COUNT(*) FROM test_tantivy_phase3.t_articles WHERE body MATCH_PHRASE_PREFIX 'star';"

# TC7: MATCH_REGEXP executes without error
run_tc_no_error "MATCH_REGEXP executes" \
    "SELECT COUNT(*) FROM test_tantivy_phase3.t_articles WHERE body MATCH_REGEXP 'database';"

# TC8: MATCH_ANY regression still works with correct results
run_tc "MATCH_ANY regression" \
    "SELECT COUNT(*) FROM test_tantivy_phase3.t_articles WHERE body MATCH_ANY 'database';" \
    "2"

# TC9: tantivy imp_lib rejected in CREATE TABLE without enable_experimental_gin
# (verify IndexAnalyzer allows tantivy)
run_tc_no_error "tantivy imp_lib accepted" \
    "SHOW INDEX FROM test_tantivy_phase3.t_articles;"

echo ""
echo "=== Results: ${PASS}/${TOTAL} passed, ${FAIL} failed ==="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
