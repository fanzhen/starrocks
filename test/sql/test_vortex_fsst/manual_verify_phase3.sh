#!/usr/bin/env bash
# Phase 3 FSST Encoding E2E Verification Script
#
# Usage:
#   bash manual_verify_phase3.sh                    # Default 127.0.0.1:9030
#   bash manual_verify_phase3.sh -h 43.99.41.30     # Specify server
#
# Prerequisites: StarRocks FE+BE deployed with FSST encoding support

set -euo pipefail

SR_HOST="${SR_HOST:-127.0.0.1}"
SR_PORT="${SR_PORT:-9030}"
SR_USER="${SR_USER:-root}"
DB="test_phase3_fsst"

while [[ $# -gt 0 ]]; do
    case $1 in
        -h) SR_HOST="$2"; shift 2 ;;
        -P) SR_PORT="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

MYSQL="mysql -h $SR_HOST -P $SR_PORT -u $SR_USER --batch -N"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'
PASS=0; FAIL=0

ok()   { echo -e "${GREEN}  PASS: $1${NC}"; PASS=$((PASS + 1)); }
fail() { echo -e "${RED}  FAIL: $1${NC}"; FAIL=$((FAIL + 1)); }
step() { echo -e "\n${BLUE}>>> $1${NC}"; }

# ── Setup ─────────────────────────────────────────────
step "Setup: Create test database and tables"
$MYSQL -e "CREATE DATABASE IF NOT EXISTS $DB;"
$MYSQL "$DB" -e "DROP TABLE IF EXISTS t_fsst; DROP TABLE IF EXISTS t_nofsst;"

$MYSQL "$DB" -e "
CREATE TABLE t_fsst (k1 INT, url VARCHAR(256))
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES('replication_num' = '1', 'enable_fsst_encoding' = 'true');
"

$MYSQL "$DB" -e "
CREATE TABLE t_nofsst (k1 INT, url VARCHAR(256))
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES('replication_num' = '1');
"

step "Setup: Insert 10000 rows of URL-like data"
$MYSQL "$DB" -e "
INSERT INTO t_fsst
SELECT x, CONCAT('http://example.com/api/v', CAST(x%100 AS VARCHAR), '/user/', CAST(x AS VARCHAR))
FROM TABLE(generate_series(1, 10000)) AS g(x);
"
$MYSQL "$DB" -e "
INSERT INTO t_nofsst
SELECT x, CONCAT('http://example.com/api/v', CAST(x%100 AS VARCHAR), '/user/', CAST(x AS VARCHAR))
FROM TABLE(generate_series(1, 10000)) AS g(x);
"

# ── Test 1: FSST ON/OFF query results match (EQ) ─────
step "Test 1: FSST ON/OFF equality query results match"
CNT_FSST=$($MYSQL "$DB" -e "SELECT COUNT(*) FROM t_fsst WHERE url = 'http://example.com/api/v50/user/50';")
CNT_NOFSST=$($MYSQL "$DB" -e "SELECT COUNT(*) FROM t_nofsst WHERE url = 'http://example.com/api/v50/user/50';")

if [[ "$CNT_FSST" == "$CNT_NOFSST" ]]; then
    ok "FSST=$CNT_FSST, NoFSST=$CNT_NOFSST (match)"
else
    fail "FSST=$CNT_FSST, NoFSST=$CNT_NOFSST (mismatch!)"
fi

# ── Test 2: FSST table CompressedEncodingFilterRows > 0 ──
step "Test 2: FSST table - profile CompressedEncodingFilterRows > 0"
# Run query and get query_id in the SAME session (enable_profile + query + last_query_id)
QID=$($MYSQL "$DB" -e "SET enable_profile = true; SELECT COUNT(*) FROM t_fsst WHERE url = 'http://example.com/api/v50/user/50'; SELECT last_query_id();" | tail -1)
sleep 2

CEROWS=$(curl -s -u root: "http://$SR_HOST:8030/api/profile?query_id=$QID" \
  | grep -o 'CompressedEncodingFilterRows: [0-9]*' | head -1 | awk '{print $2}' || echo "0")

if [[ -z "$CEROWS" ]]; then CEROWS=0; fi
if [[ "$CEROWS" -gt 0 ]]; then
    ok "CompressedEncodingFilterRows = $CEROWS (> 0)"
else
    fail "CompressedEncodingFilterRows = $CEROWS (expected > 0, QID=$QID)"
fi

# ── Test 3: Non-FSST table CompressedEncodingFilterRows = 0 (regression) ──
step "Test 3: Non-FSST table - CompressedEncodingFilterRows = 0 (regression)"
QID2=$($MYSQL "$DB" -e "SET enable_profile = true; SELECT COUNT(*) FROM t_nofsst WHERE url = 'http://example.com/api/v50/user/50'; SELECT last_query_id();" | tail -1)
sleep 2

CEROWS2=$(curl -s -u root: "http://$SR_HOST:8030/api/profile?query_id=$QID2" \
  | grep -o 'CompressedEncodingFilterRows: [0-9]*' | head -1 | awk '{print $2}' || echo "0")

if [[ -z "$CEROWS2" ]]; then CEROWS2=0; fi
if [[ "$CEROWS2" -eq 0 ]]; then
    ok "Non-FSST CompressedEncodingFilterRows = $CEROWS2 (expected 0)"
else
    fail "Non-FSST CompressedEncodingFilterRows = $CEROWS2 (expected 0)"
fi

# ── Test 4: LIKE predicate correctness (fallback to decode path) ──
step "Test 4: LIKE predicate correctness (fallback path)"
CNT_LIKE_FSST=$($MYSQL "$DB" -e "SELECT COUNT(*) FROM t_fsst WHERE url LIKE '%/api/v50/%';")
CNT_LIKE_NOFSST=$($MYSQL "$DB" -e "SELECT COUNT(*) FROM t_nofsst WHERE url LIKE '%/api/v50/%';")

if [[ "$CNT_LIKE_FSST" == "$CNT_LIKE_NOFSST" ]]; then
    ok "LIKE: FSST=$CNT_LIKE_FSST, NoFSST=$CNT_LIKE_NOFSST (match)"
else
    fail "LIKE: FSST=$CNT_LIKE_FSST, NoFSST=$CNT_LIKE_NOFSST (mismatch!)"
fi

# ── Test 5: IN predicate correctness ──
step "Test 5: IN predicate correctness"
CNT_IN_FSST=$($MYSQL "$DB" -e "
SELECT COUNT(*) FROM t_fsst WHERE url IN (
  'http://example.com/api/v50/user/50',
  'http://example.com/api/v1/user/1',
  'http://example.com/api/v99/user/99'
);")
CNT_IN_NOFSST=$($MYSQL "$DB" -e "
SELECT COUNT(*) FROM t_nofsst WHERE url IN (
  'http://example.com/api/v50/user/50',
  'http://example.com/api/v1/user/1',
  'http://example.com/api/v99/user/99'
);")

if [[ "$CNT_IN_FSST" == "$CNT_IN_NOFSST" ]]; then
    ok "IN: FSST=$CNT_IN_FSST, NoFSST=$CNT_IN_NOFSST (match)"
else
    fail "IN: FSST=$CNT_IN_FSST, NoFSST=$CNT_IN_NOFSST (mismatch!)"
fi

# ── Cleanup ──
step "Cleanup"
$MYSQL -e "DROP DATABASE IF EXISTS $DB;"

# ── Summary ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [[ $FAIL -eq 0 ]]; then
    echo -e "${GREEN}  Phase 3 FSST 验证: $PASS/$((PASS + FAIL)) 全部通过!${NC}"
else
    echo -e "${RED}  Phase 3 FSST 验证: $PASS passed, $FAIL failed${NC}"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
exit $FAIL
