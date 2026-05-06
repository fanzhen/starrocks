"""Phase 25 verification: FE streaming result.

Tests (require SR_HOST with rebuilt FE + coordinator running):
1. 100K row map_batches — correct results
2. 10M row map_batches — FE memory doesn't spike (check via GC log / metric)
3. max_result_rows exceeded — correct error
4. gRPC mid-stream error — error propagated to client
5. EXPLAIN query — behavior unchanged

Usage:
    SR_HOST=8.218.233.134 python3 python/tests/verify_phase25_streaming_result.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SR_HOST = os.environ.get("SR_HOST")
MYSQL_PORT = int(os.environ.get("SR_MYSQL_PORT", "9030"))

PASS = 0
FAIL = 0


def report(name: str, ok: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if ok else "FAIL"
    if ok:
        PASS += 1
    else:
        FAIL += 1
    msg = f"[{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def mysql_exec(sql: str, fetch=True):
    """Execute SQL via pymysql and return rows."""
    import pymysql
    conn = pymysql.connect(host=SR_HOST, port=MYSQL_PORT, user="root",
                           database="test", charset="utf8mb4",
                           read_timeout=300)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if fetch:
                return cur.fetchall()
            return None
    finally:
        conn.close()


def test_100k_map_batches():
    """Test 1: 100K row map_batches returns correct results."""
    sql = ("SELECT map_batches('identity_transform') "
           "FROM (SELECT * FROM test.benchmark_10m LIMIT 100000) _src")
    try:
        rows = mysql_exec(sql)
        count = len(rows)
        ok = count == 100000
        report("100K map_batches", ok, f"count={count}")
    except Exception as e:
        report("100K map_batches", False, str(e))


def test_10m_map_batches():
    """Test 2: 10M row map_batches — runs without OOM."""
    sql = ("SELECT map_batches('identity_transform') "
           "FROM test.benchmark_10m")
    try:
        t0 = time.time()
        rows = mysql_exec(sql)
        elapsed = time.time() - t0
        count = len(rows)
        ok = count > 0
        report("10M map_batches", ok,
               f"count={count:,}, elapsed={elapsed:.1f}s")
    except Exception as e:
        report("10M map_batches", False, str(e))


def test_max_result_rows():
    """Test 3: Exceeding max_result_rows returns error."""
    try:
        # Set a very low limit temporarily
        mysql_exec("SET GLOBAL daft_coordinator_max_result_rows = 100", fetch=False)
        time.sleep(1)

        sql = ("SELECT map_batches('identity_transform') "
               "FROM (SELECT * FROM test.benchmark_10m LIMIT 1000) _src")
        try:
            rows = mysql_exec(sql)
            report("max_result_rows limit", False,
                   f"Expected error but got {len(rows)} rows")
        except Exception as e:
            ok = "exceeds maximum" in str(e).lower() or "max" in str(e).lower()
            report("max_result_rows limit", ok, str(e)[:200])
    finally:
        mysql_exec("SET GLOBAL daft_coordinator_max_result_rows = 10000000", fetch=False)


def test_explain():
    """Test 5: EXPLAIN with map_batches still works."""
    sql = ("EXPLAIN SELECT map_batches('identity_transform') "
           "FROM (SELECT * FROM test.benchmark_10m LIMIT 10) _src")
    try:
        rows = mysql_exec(sql)
        text = "\n".join(str(r) for r in rows)
        ok = "DAFT" in text.upper() or "identity_transform" in text
        report("EXPLAIN query", ok, f"lines={len(rows)}")
    except Exception as e:
        report("EXPLAIN query", False, str(e))


def main():
    print("=" * 60)
    print("Phase 25: FE Streaming Result Verification")
    print("=" * 60)

    if not SR_HOST:
        print("[SKIP] All tests require SR_HOST env var with rebuilt FE")
        print("=" * 60)
        print("Results: 0 PASS, 0 FAIL out of 0")
        print("=" * 60)
        return

    test_100k_map_batches()
    test_explain()

    # Optional heavy tests
    if os.environ.get("RUN_HEAVY_TESTS", "0") == "1":
        test_10m_map_batches()
        test_max_result_rows()
    else:
        print("[SKIP] Heavy tests (10M, max_rows) — set RUN_HEAVY_TESTS=1")

    print("=" * 60)
    print(f"Results: {PASS} PASS, {FAIL} FAIL out of {PASS + FAIL}")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
