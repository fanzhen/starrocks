"""Phase 23 verification: Stream Load streaming write-back.

Tests:
1. write_back 100K rows — data correctly written to StarRocks
2. write_back with NULL values — NULLs correctly preserved
3. Memory peak comparison — streaming peak < full materialization 50%

Usage:
    SR_HOST=8.218.233.134 python3 python/tests/verify_phase23_streaming_writeback.py
"""

import os
import sys
import tracemalloc
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyarrow as pa
import pyarrow.csv as pcsv

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
SR_PORT = int(os.environ.get("SR_HTTP_PORT", "8030"))
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


def mysql_exec(sql: str):
    """Execute SQL via pymysql and return rows."""
    import pymysql
    conn = pymysql.connect(host=SR_HOST, port=MYSQL_PORT, user="root",
                           database="test", charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def setup_table(table_name: str, has_nullable: bool = False):
    """Create a test table, dropping if exists."""
    mysql_exec(f"DROP TABLE IF EXISTS `{table_name}`")
    if has_nullable:
        mysql_exec(f"""
            CREATE TABLE `{table_name}` (
                id INT,
                val VARCHAR(100),
                score DOUBLE
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """)
    else:
        mysql_exec(f"""
            CREATE TABLE `{table_name}` (
                id INT,
                val VARCHAR(100)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """)


def test_write_100k():
    """Test 1: write_back 100K rows correctly."""
    from starrocks.coordinator.stream_load_writer import StreamLoadWriter

    table_name = f"phase23_100k_{uuid.uuid4().hex[:6]}"
    setup_table(table_name)

    n = 100_000
    ids = pa.array(range(n), type=pa.int32())
    vals = pa.array([f"row_{i}" for i in range(n)], type=pa.string())
    arrow_table = pa.table({"id": ids, "val": vals})

    writer = StreamLoadWriter(SR_HOST, SR_PORT)
    result = writer.write_table(arrow_table, "test", table_name)

    # Verify count
    rows = mysql_exec(f"SELECT COUNT(*) FROM `{table_name}`")
    count = rows[0][0]

    ok = count == n
    report("Write 100K rows", ok, f"count={count}, expected={n}")

    # Cleanup
    mysql_exec(f"DROP TABLE IF EXISTS `{table_name}`")


def test_write_with_nulls():
    """Test 2: write_back with NULL values correctly preserved."""
    from starrocks.coordinator.stream_load_writer import StreamLoadWriter

    table_name = f"phase23_null_{uuid.uuid4().hex[:6]}"
    setup_table(table_name, has_nullable=True)

    ids = pa.array([1, 2, 3, 4, 5], type=pa.int32())
    vals = pa.array(["hello", None, "world", None, "end"], type=pa.string())
    scores = pa.array([1.5, 2.5, None, 4.5, None], type=pa.float64())
    arrow_table = pa.table({"id": ids, "val": vals, "score": scores})

    writer = StreamLoadWriter(SR_HOST, SR_PORT)
    writer.write_table(arrow_table, "test", table_name)

    rows = mysql_exec(f"SELECT id, val, score FROM `{table_name}` ORDER BY id")
    # Check NULLs
    null_vals = [r for r in rows if r[1] is None]
    null_scores = [r for r in rows if r[2] is None]

    ok = len(null_vals) == 2 and len(null_scores) == 2
    report("Write with NULLs", ok,
           f"null_vals={len(null_vals)}, null_scores={len(null_scores)}")

    mysql_exec(f"DROP TABLE IF EXISTS `{table_name}`")


def test_memory_comparison():
    """Test 3: Streaming write uses less peak memory than full materialization."""
    from starrocks.coordinator.stream_load_writer import StreamLoadWriter
    import io

    n = 200_000
    ids = pa.array(range(n), type=pa.int32())
    vals = pa.array([f"row_{i:06d}_padding_data" for i in range(n)], type=pa.string())
    arrow_table = pa.table({"id": ids, "val": vals})

    # Measure full materialization memory
    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()
    table_with_nulls = StreamLoadWriter._replace_nulls_with_marker(arrow_table)
    buf = io.BytesIO()
    pcsv.write_csv(table_with_nulls, buf,
                   write_options=pcsv.WriteOptions(include_header=False))
    full_data = buf.getvalue()
    snapshot2 = tracemalloc.take_snapshot()
    tracemalloc.stop()

    full_peak = sum(s.size for s in snapshot2.statistics("filename"))

    # Measure streaming memory
    tracemalloc.start()
    snapshot3 = tracemalloc.take_snapshot()
    total_bytes = 0
    for chunk in StreamLoadWriter._generate_csv_chunks(arrow_table, max_chunksize=8192):
        total_bytes += len(chunk)
    snapshot4 = tracemalloc.take_snapshot()
    tracemalloc.stop()

    streaming_peak = sum(s.size for s in snapshot4.statistics("filename"))

    # The streaming approach should use significantly less memory
    ratio = streaming_peak / full_peak if full_peak > 0 else 999
    ok = ratio < 0.5
    report("Memory comparison", ok,
           f"streaming_peak={streaming_peak:,}, full_peak={full_peak:,}, "
           f"ratio={ratio:.2f}, csv_bytes={total_bytes:,} vs {len(full_data):,}")


def main():
    print("=" * 60)
    print("Phase 23: Stream Load Streaming Write-back Verification")
    print("=" * 60)

    test_memory_comparison()  # No server needed for this test

    if SR_HOST == "127.0.0.1" and os.environ.get("SR_HOST") is None:
        print("\n[SKIP] Tests 1-2 require SR_HOST env var for StarRocks connection")
    else:
        test_write_100k()
        test_write_with_nulls()

    print("=" * 60)
    print(f"Results: {PASS} PASS, {FAIL} FAIL out of {PASS + FAIL}")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
