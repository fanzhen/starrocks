#!/usr/bin/env python3.11
"""E2E verification for Stage 3: Arrow Flight SQL zero-copy data channel.

Usage:
    SR_HOST=47.239.57.232 python3.11 tests/verify_stage3_arrow_flight.py

Prerequisites:
    - StarRocks running on SR_HOST:9030 (MySQL) and SR_HOST:9408 (Arrow Flight)
    - pip install adbc_driver_flightsql adbc_driver_manager pyarrow getdaft pymysql pandas
"""

from __future__ import annotations

import os
import sys
import time
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SR_HOST = os.getenv("SR_HOST", "127.0.0.1")
SR_PORT = int(os.getenv("SR_PORT", "9030"))
ARROW_FLIGHT_PORT = int(os.getenv("ARROW_FLIGHT_PORT", "9408"))
DB_NAME = "test_arrow_flight_stage3"

results: list[tuple[str, bool, str]] = []


def record(name: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    results.append((name, passed, detail))
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    import pyarrow as pa

    print(f"\n=== Stage 3: Arrow Flight SQL E2E Verification ===")
    print(f"StarRocks MySQL: {SR_HOST}:{SR_PORT}")
    print(f"StarRocks Arrow Flight: {SR_HOST}:{ARROW_FLIGHT_PORT}\n")

    # -- TC1: Arrow Flight SQL direct connection --------------------------------
    print("--- TC1: Arrow Flight SQL direct connection ---")
    try:
        from starrocks.connection.arrow_flight import ArrowFlightConnection
        af_conn = ArrowFlightConnection(
            host=SR_HOST, port=ARROW_FLIGHT_PORT, user="root", password=""
        )
        table = af_conn.execute_to_arrow("SELECT 1 AS n, 3.14 AS val, 'hello' AS msg")
        record("TC1a: ArrowFlightConnection", True, f"cols={table.column_names}")
        record("TC1b: returns pyarrow.Table", isinstance(table, pa.Table), str(type(table)))
        record("TC1c: correct data", table.to_pydict()["n"] == [1], str(table.to_pydict()))
        af_conn.close()
    except Exception as e:
        record("TC1: ArrowFlightConnection", False, f"{e}\n{traceback.format_exc()}")

    # -- TC2: Session with arrow_flight_port -----------------------------------
    print("\n--- TC2: Session with arrow_flight_port ---")
    try:
        from starrocks import Session
        session = Session(
            host=SR_HOST, port=SR_PORT, user="root",
            arrow_flight_port=ARROW_FLIGHT_PORT,
        )
        record("TC2a: Session created", session.arrow_connection is not None,
               "arrow_connection is set")

        # Setup test database and table
        session.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`")
        session.execute(f"USE `{DB_NAME}`")
        for t in ["flight_source", "flight_enriched", "flight_roundtrip"]:
            session.execute(f"DROP TABLE IF EXISTS `{t}`")
        session.execute("""
            CREATE TABLE flight_source (
                id INT,
                name VARCHAR(100),
                score DOUBLE,
                category VARCHAR(50)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)
        session.execute("""
            INSERT INTO flight_source VALUES
            (1, 'Alice', 95.5, 'A'),
            (2, 'Bob', 82.3, 'B'),
            (3, 'Charlie', 91.0, 'A'),
            (4, 'Diana', 78.9, 'C'),
            (5, 'Eve', 88.7, 'B'),
            (6, 'Frank', 93.2, 'A'),
            (7, 'Grace', 85.1, 'C'),
            (8, 'Hank', 79.4, 'B'),
            (9, 'Iris', 96.8, 'A'),
            (10, 'Jack', 72.5, 'C')
        """)
        record("TC2b: test data setup", True, "10 rows inserted")
    except Exception as e:
        record("TC2: Session setup", False, f"{e}\n{traceback.format_exc()}")
        return 1

    # -- TC3: to_arrow() zero-copy ---------------------------------------------
    print("\n--- TC3: df.to_arrow() via Arrow Flight ---")
    try:
        df = session.table("flight_source")
        arrow_table = df.to_arrow()
        record("TC3a: to_arrow type", isinstance(arrow_table, pa.Table),
               str(type(arrow_table)))
        record("TC3b: to_arrow row count", arrow_table.num_rows == 10,
               f"expected=10, got={arrow_table.num_rows}")
        record("TC3c: to_arrow columns",
               set(arrow_table.column_names) == {"id", "name", "score", "category"},
               f"got={arrow_table.column_names}")

        # Verify data integrity
        pdf = arrow_table.to_pandas()
        ids = sorted(pdf["id"].tolist())
        record("TC3d: to_arrow data integrity", ids == list(range(1, 11)),
               f"ids={ids}")
    except Exception as e:
        record("TC3: to_arrow", False, f"{e}\n{traceback.format_exc()}")

    # -- TC4: to_pandas() via Arrow Flight -------------------------------------
    print("\n--- TC4: df.to_pandas() via Arrow Flight ---")
    try:
        df = session.table("flight_source").filter(
            __import__("starrocks").col("score") > 90.0
        )
        pdf = df.to_pandas()
        record("TC4a: to_pandas with filter",
               len(pdf) == 4,  # Alice(95.5), Charlie(91.0), Frank(93.2), Iris(96.8)
               f"expected=4 rows with score>90, got={len(pdf)}")
        record("TC4b: to_pandas correct names",
               set(pdf["name"].tolist()) == {"Alice", "Charlie", "Frank", "Iris"},
               f"got={pdf['name'].tolist()}")
    except Exception as e:
        record("TC4: to_pandas via Flight", False, f"{e}\n{traceback.format_exc()}")

    # -- TC5: to_daft() zero-copy Arrow path -----------------------------------
    print("\n--- TC5: df.to_daft() via Arrow Flight (zero-copy) ---")
    try:
        import daft
        df = session.table("flight_source")
        daft_df = df.to_daft()
        count = daft_df.count_rows()
        cols = daft_df.column_names
        record("TC5a: to_daft row count", count == 10,
               f"expected=10, got={count}")
        record("TC5b: to_daft columns",
               set(cols) == {"id", "name", "score", "category"},
               f"got={cols}")
    except ImportError:
        record("TC5: to_daft", False, "daft not installed")
    except Exception as e:
        record("TC5: to_daft", False, f"{e}\n{traceback.format_exc()}")

    # -- TC6: Full pipeline: Arrow Flight → Daft transform → write back --------
    print("\n--- TC6: Arrow Flight → Daft → StarRocks roundtrip ---")
    try:
        import daft

        session.execute("""
            CREATE TABLE flight_enriched (
                id INT,
                name VARCHAR(100),
                score DOUBLE,
                category VARCHAR(50),
                name_len INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)

        # Arrow Flight → Daft → transform → write back
        daft_df = session.table("flight_source").to_daft()
        result_df = daft_df.with_column("name_len", daft_df["name"].length())
        n = session.write_daft(result_df, "flight_enriched")
        record("TC6a: write_daft row count", n == 10, f"wrote={n}")

        # Verify written data
        rows = session.execute(
            "SELECT id, name, name_len FROM flight_enriched ORDER BY id"
        )
        correct = all(row["name_len"] == len(row["name"]) for row in rows)
        record("TC6b: name_len correct", correct,
               f"sample: name='{rows[0]['name']}', len={rows[0]['name_len']}")
    except Exception as e:
        record("TC6: Arrow→Daft→SR pipeline", False, f"{e}\n{traceback.format_exc()}")

    # -- TC7: map_batches with Arrow Flight ------------------------------------
    print("\n--- TC7: map_batches via Arrow Flight ---")
    try:
        import daft

        session.execute("""
            CREATE TABLE flight_roundtrip (
                id INT,
                name VARCHAR(100),
                score DOUBLE,
                category VARCHAR(50),
                score_doubled DOUBLE
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)

        def double_score(daft_df):
            return daft_df.with_column("score_doubled", daft_df["score"] * 2)

        result = session.table("flight_source").map_batches(double_score)
        result.to_starrocks("flight_roundtrip")

        rows = session.execute(
            "SELECT id, score, score_doubled FROM flight_roundtrip ORDER BY id"
        )
        correct = all(
            abs(row["score_doubled"] - row["score"] * 2) < 1e-9
            for row in rows
        )
        record("TC7a: map_batches score_doubled", correct,
               f"sample: score={rows[0]['score']}, doubled={rows[0]['score_doubled']}")

        # Verify aggregation
        agg = session.execute(
            "SELECT SUM(score_doubled) AS total FROM flight_roundtrip"
        )
        expected_sum = sum(r["score"] * 2 for r in session.execute(
            "SELECT score FROM flight_source"
        ))
        got_sum = float(agg[0]["total"])
        record("TC7b: SUM(score_doubled)", abs(got_sum - expected_sum) < 1e-6,
               f"expected={expected_sum}, got={got_sum}")
    except Exception as e:
        record("TC7: map_batches via Flight", False, f"{e}\n{traceback.format_exc()}")

    # -- TC8: Data consistency: Arrow Flight vs MySQL --------------------------
    print("\n--- TC8: Arrow Flight vs MySQL consistency ---")
    try:
        # Fetch same data via both paths
        from starrocks import Session as S
        mysql_session = S(host=SR_HOST, port=SR_PORT, user="root")
        mysql_session.execute(f"USE `{DB_NAME}`")

        sql = "SELECT id, name, score, category FROM flight_source ORDER BY id"
        arrow_pdf = session.table("flight_source").order_by(
            __import__("starrocks").col("id").asc()
        ).to_pandas()
        mysql_pdf = mysql_session.table("flight_source").order_by(
            __import__("starrocks").col("id").asc()
        ).to_pandas()

        # Compare row counts
        record("TC8a: row count match",
               len(arrow_pdf) == len(mysql_pdf),
               f"arrow={len(arrow_pdf)}, mysql={len(mysql_pdf)}")

        # Compare values
        match = True
        for i in range(len(arrow_pdf)):
            ar = arrow_pdf.iloc[i]
            mr = mysql_pdf.iloc[i]
            if (int(ar["id"]) != int(mr["id"]) or
                    str(ar["name"]) != str(mr["name"]) or
                    abs(float(ar["score"]) - float(mr["score"])) > 1e-9):
                match = False
                break
        record("TC8b: data values match", match, "Arrow Flight == MySQL")
        mysql_session.close()
    except Exception as e:
        record("TC8: consistency check", False, f"{e}\n{traceback.format_exc()}")

    # -- Summary ---------------------------------------------------------------
    print(f"\n{'='*60}")
    passed = sum(1 for _, p, _ in results if p)
    total = len(results)
    print(f"Results: {passed}/{total} PASSED")

    if passed < total:
        print("\nFailed tests:")
        for name, p, detail in results:
            if not p:
                print(f"  FAIL: {name} — {detail}")

    # Cleanup
    session.execute(f"DROP DATABASE IF EXISTS `{DB_NAME}` FORCE")
    session.close()

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
