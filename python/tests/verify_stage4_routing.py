#!/usr/bin/env python3
"""Stage 4 E2E verification: auto-routing + multimodal types.

Requires:
  - StarRocks at SR_HOST:9030 with Arrow Flight at SR_HOST:9408
  - pip install daft pyarrow adbc_driver_flightsql adbc_driver_manager

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_stage4_routing.py
"""

from __future__ import annotations

import os
import sys
import traceback

# -- config ------------------------------------------------------------------

SR_HOST = os.getenv("SR_HOST", "127.0.0.1")
SR_PORT = int(os.getenv("SR_PORT", "9030"))
FLIGHT_PORT = int(os.getenv("SR_FLIGHT_PORT", "9408"))
DB = "test_stage4_routing"

results: list[tuple[str, bool, str]] = []


def check(name: str, func):
    """Run a check and record pass/fail."""
    try:
        msg = func()
        results.append((name, True, msg or "OK"))
        print(f"  PASS  {name}: {msg or 'OK'}")
    except Exception as e:
        results.append((name, False, str(e)))
        print(f"  FAIL  {name}: {e}")
        traceback.print_exc()


# -- setup -------------------------------------------------------------------

def setup():
    from starrocks import Session
    s = Session(host=SR_HOST, port=SR_PORT, arrow_flight_port=FLIGHT_PORT)
    s.execute(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    s.execute(f"USE `{DB}`")
    s.execute("DROP TABLE IF EXISTS routing_test")
    s.execute("""
        CREATE TABLE routing_test (
            id BIGINT,
            val DOUBLE,
            name VARCHAR(100)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """)
    s.execute("""
        INSERT INTO routing_test VALUES
        (1, 10.5, 'alice'),
        (2, 20.0, 'bob'),
        (3, 30.5, 'charlie'),
        (4, 40.0, 'diana'),
        (5, 50.5, 'eve')
    """)

    s.execute("DROP TABLE IF EXISTS routing_target")
    s.execute("""
        CREATE TABLE routing_target (
            id BIGINT,
            val DOUBLE,
            name VARCHAR(100)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """)
    return s


# -- tests -------------------------------------------------------------------

def test_1_filter_map_to_pandas(s):
    """df.filter(c>1).map_batches(func).to_pandas() auto-routes correctly."""
    from starrocks import col
    df = s.table("routing_test")
    # filter in SQL, then double val in Python
    def double_val(daft_df):
        import daft
        return daft_df.with_column("val", daft.col("val") * 2)

    result = df.filter(col("id") > 1).map_batches(double_val).to_pandas()
    assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
    # id=2, val should be 40.0
    row2 = result[result["id"] == 2].iloc[0]
    assert abs(row2["val"] - 40.0) < 0.01, f"Expected val=40.0, got {row2['val']}"
    return f"4 rows, val doubled correctly"


def test_2_chained_map_batches(s):
    """df.map_batches(f1).map_batches(f2).to_pandas() chains UDFs."""
    df = s.table("routing_test")

    def add_ten(daft_df):
        import daft
        return daft_df.with_column("val", daft.col("val") + 10)

    def multiply_two(daft_df):
        import daft
        return daft_df.with_column("val", daft.col("val") * 2)

    result = df.map_batches(add_ten).map_batches(multiply_two).to_pandas()
    assert len(result) == 5
    # id=1: (10.5 + 10) * 2 = 41.0
    row1 = result[result["id"] == 1].iloc[0]
    assert abs(row1["val"] - 41.0) < 0.01, f"Expected 41.0, got {row1['val']}"
    return f"5 rows, chained UDF correct (id=1 val={row1['val']})"


def test_3_pure_sql_regression(s):
    """df.filter().select().to_pandas() without map_batches uses pure SQL."""
    from starrocks import col
    df = s.table("routing_test")
    result = df.filter(col("id") > 3).select("id", "name").to_pandas()
    assert len(result) == 2
    names = set(result["name"].tolist())
    assert names == {"diana", "eve"}, f"Expected diana/eve, got {names}"
    return f"2 rows, pure SQL correct"


def test_4_map_then_filter_select(s):
    """df.map_batches(func).filter(c>0).select('a','b') — post-UDF ops on Daft."""
    from starrocks import col
    df = s.table("routing_test")

    def negate_val(daft_df):
        import daft
        return daft_df.with_column("val", daft.col("val") * -1)

    result = (
        df.map_batches(negate_val)
          .filter(col("val") < -20)
          .select("id", "val")
          .to_pandas()
    )
    # val < -20 means original val > 20: ids 3,4,5
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    ids = sorted(result["id"].tolist())
    assert ids == [3, 4, 5], f"Expected [3,4,5], got {ids}"
    return f"3 rows after filter on Daft"


def test_5_map_to_starrocks(s):
    """df.map_batches(func).to_starrocks('target') writes back."""
    df = s.table("routing_test")

    def upper_name(daft_df):
        return daft_df.with_column("name", daft_df["name"].str.upper())

    count = df.map_batches(upper_name).to_starrocks("routing_target", mode="overwrite")
    assert count == 5, f"Expected 5 rows written, got {count}"
    # Verify written data
    verify = s.table("routing_target").to_pandas()
    names = set(verify["name"].tolist())
    assert "ALICE" in names, f"Expected 'ALICE' in {names}"
    return f"{count} rows written, uppercase names verified"


def test_6_udf_error_handling(s):
    """UDF exception should produce a friendly error, not raw Daft/Ray stack."""
    from starrocks import col
    from starrocks.exceptions import QueryError
    df = s.table("routing_test")

    def bad_udf(daft_df):
        raise ValueError("intentional test error")

    try:
        df.map_batches(bad_udf).to_pandas()
        return "FAIL: no exception raised"
    except QueryError as e:
        assert "UDF execution failed" in str(e)
        assert "intentional test error" in str(e)
        return f"QueryError raised correctly: {e}"
    except Exception as e:
        return f"FAIL: wrong exception type {type(e).__name__}: {e}"


def test_7_multimodal_types():
    """Multimodal type annotations: Image, Embedding, Tensor."""
    from starrocks import Image, Embedding, Tensor, col

    img = col("url").cast(Image())
    assert img._multimodal_type == Image()

    emb = col("vec").cast(Embedding(dim=384))
    assert emb._multimodal_type.dim == 384

    t = col("data").cast(Tensor(shape=(3, 224, 224)))
    assert t._multimodal_type.shape == (3, 224, 224)

    return "Image/Embedding/Tensor annotations work"


def test_8_count_with_map_batches(s):
    """count() on a pipeline with map_batches should work."""
    from starrocks import col
    df = s.table("routing_test")

    def identity(daft_df):
        return daft_df

    count = df.filter(col("id") > 2).map_batches(identity).count()
    assert count == 3, f"Expected 3, got {count}"
    return f"count={count}"


def test_9_first_with_map_batches(s):
    """first() on a pipeline with map_batches should return one row."""
    df = s.table("routing_test")

    def identity(daft_df):
        return daft_df

    row = df.map_batches(identity).first()
    assert row is not None
    assert "id" in row
    return f"first row: id={row['id']}"


# -- main --------------------------------------------------------------------

def main():
    print(f"\n{'='*60}")
    print(f"Stage 4 E2E Verification: Auto-Routing + Multimodal Types")
    print(f"StarRocks: {SR_HOST}:{SR_PORT}, Flight: {SR_HOST}:{FLIGHT_PORT}")
    print(f"{'='*60}\n")

    s = setup()
    print(f"Database '{DB}' set up with routing_test (5 rows)\n")

    check("1. filter→map_batches→to_pandas", lambda: test_1_filter_map_to_pandas(s))
    check("2. chained map_batches", lambda: test_2_chained_map_batches(s))
    check("3. pure SQL regression (no map_batches)", lambda: test_3_pure_sql_regression(s))
    check("4. map_batches→filter→select on Daft", lambda: test_4_map_then_filter_select(s))
    check("5. map_batches→to_starrocks write-back", lambda: test_5_map_to_starrocks(s))
    check("6. UDF error handling", lambda: test_6_udf_error_handling(s))
    check("7. multimodal type annotations", lambda: test_7_multimodal_types)
    check("8. count() with map_batches", lambda: test_8_count_with_map_batches(s))
    check("9. first() with map_batches", lambda: test_9_first_with_map_batches(s))

    # -- summary
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print(f"\n{'='*60}")
    print(f"Results: {passed}/{total} PASSED")
    if passed == total:
        print("Stage 4 E2E: ALL PASSED")
    else:
        print("Stage 4 E2E: SOME FAILED")
        for name, ok, msg in results:
            if not ok:
                print(f"  FAILED: {name} — {msg}")
    print(f"{'='*60}\n")

    s.execute(f"DROP DATABASE IF EXISTS `{DB}` FORCE")
    s.close()
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
