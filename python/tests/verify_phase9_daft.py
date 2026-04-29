#!/usr/bin/env python3.11
"""E2E verification for Phase 8+9: Daft on Ray POC integration.

Usage:
    SR_HOST=47.239.57.232 python3.11 tests/verify_phase9_daft.py

Prerequisites:
    - StarRocks running on SR_HOST:9030
    - pip install "ray[default]" getdaft pymysql pandas
"""

from __future__ import annotations

import os
import sys
import traceback

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SR_HOST = os.getenv("SR_HOST", "127.0.0.1")
SR_PORT = int(os.getenv("SR_PORT", "9030"))
DB_NAME = "test_daft_poc"

results: list[tuple[str, bool, str]] = []


def record(name: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    results.append((name, passed, detail))
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    from starrocks import Session, col

    print(f"\n=== Phase 8+9 Daft POC E2E Verification ===")
    print(f"StarRocks: {SR_HOST}:{SR_PORT}\n")

    # -- Setup ----------------------------------------------------------------
    session = Session(host=SR_HOST, port=SR_PORT, user="root")
    session.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`")
    session.execute(f"USE `{DB_NAME}`")

    # Clean up from previous runs
    for t in ["daft_source", "daft_enriched", "daft_enriched2", "daft_roundtrip"]:
        session.execute(f"DROP TABLE IF EXISTS `{t}`")

    # Create source table (avoid reserved keyword 'text' → use 'txt')
    session.execute("""
        CREATE TABLE daft_source (
            id INT,
            txt VARCHAR(100),
            val DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """)
    session.execute("""
        INSERT INTO daft_source VALUES
        (1, 'hello world', 3.14),
        (2, 'foo bar', 2.71),
        (3, 'starrocks daft', 1.41),
        (4, 'ray cluster', 0.57),
        (5, 'test data', 9.99)
    """)

    # -- TC1: Ray + Daft import -----------------------------------------------
    print("\n--- TC1: Ray + Daft import ---")
    try:
        import daft
        record("TC1: import daft", True, f"v{daft.__version__}")
    except ImportError as e:
        record("TC1: import daft", False, str(e))
        print("\nFATAL: daft not installed. Run: pip install getdaft")
        return 1

    # -- TC2: to_daft() -------------------------------------------------------
    print("\n--- TC2: df.to_daft() ---")
    try:
        df = session.table("daft_source")
        daft_df = df.to_daft()
        count = daft_df.count_rows()
        cols = daft_df.column_names
        record("TC2: to_daft row count", count == 5, f"expected=5, got={count}")
        record("TC2: to_daft columns", set(cols) == {"id", "txt", "val"},
               f"got={cols}")
    except Exception as e:
        record("TC2: to_daft", False, f"{e}\n{traceback.format_exc()}")

    # -- TC3: write_daft() ----------------------------------------------------
    print("\n--- TC3: session.write_daft() ---")
    try:
        session.execute("""
            CREATE TABLE daft_enriched (
                id INT,
                txt VARCHAR(100),
                val DOUBLE,
                txt_len INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)

        # Add a txt_len column via Daft
        daft_df2 = df.to_daft()
        result_df = daft_df2.with_column(
            "txt_len",
            daft_df2["txt"].length()
        )
        n = session.write_daft(result_df, "daft_enriched")
        record("TC3: write_daft row count", n == 5, f"wrote={n}")

        # Verify in StarRocks
        rows = session.execute("SELECT id, txt, txt_len FROM daft_enriched ORDER BY id")
        correct = all(row["txt_len"] == len(row["txt"]) for row in rows)
        record("TC3: txt_len values correct", correct,
               f"sample: txt='{rows[0]['txt']}', len={rows[0]['txt_len']}")
    except Exception as e:
        record("TC3: write_daft", False, f"{e}\n{traceback.format_exc()}")

    # -- TC4: roundtrip consistency -------------------------------------------
    print("\n--- TC4: roundtrip StarRocks → Daft → StarRocks ---")
    try:
        session.execute("""
            CREATE TABLE daft_roundtrip (
                id INT,
                txt VARCHAR(100),
                val DOUBLE
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)

        daft_rt = session.table("daft_source").to_daft()
        session.write_daft(daft_rt, "daft_roundtrip")

        orig = session.execute("SELECT * FROM daft_source ORDER BY id")
        roundtrip = session.execute("SELECT * FROM daft_roundtrip ORDER BY id")

        match = True
        for o, r in zip(orig, roundtrip):
            if o["id"] != r["id"] or o["txt"] != r["txt"] or abs(o["val"] - r["val"]) > 1e-9:
                match = False
                break
        record("TC4: roundtrip data match", match and len(orig) == len(roundtrip),
               f"rows: orig={len(orig)}, roundtrip={len(roundtrip)}")
    except Exception as e:
        record("TC4: roundtrip", False, f"{e}\n{traceback.format_exc()}")

    # -- TC5: map_batches with a transform function ---------------------------
    print("\n--- TC5: df.map_batches(func) ---")
    try:
        session.execute("""
            CREATE TABLE daft_enriched2 (
                id INT,
                txt VARCHAR(100),
                val DOUBLE,
                val_doubled DOUBLE
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)

        def double_val(daft_df):
            return daft_df.with_column("val_doubled", daft_df["val"] * 2)

        result = session.table("daft_source").map_batches(double_val)
        result.to_starrocks("daft_enriched2")

        rows = session.execute("SELECT id, val, val_doubled FROM daft_enriched2 ORDER BY id")
        correct = all(abs(row["val_doubled"] - row["val"] * 2) < 1e-9 for row in rows)
        record("TC5: map_batches val_doubled", correct,
               f"sample: val={rows[0]['val']}, doubled={rows[0]['val_doubled']}")
    except Exception as e:
        record("TC5: map_batches", False, f"{e}\n{traceback.format_exc()}")

    # -- TC6: aggregation after write-back ------------------------------------
    print("\n--- TC6: StarRocks aggregation on written data ---")
    try:
        rows = session.execute("SELECT SUM(txt_len) AS total_len FROM daft_enriched")
        total = rows[0]["total_len"]
        expected_len = sum(len(r["txt"]) for r in session.execute("SELECT txt FROM daft_source"))
        record("TC6: SUM(txt_len) correct", total == expected_len,
               f"expected={expected_len}, got={total}")
    except Exception as e:
        record("TC6: aggregation", False, f"{e}\n{traceback.format_exc()}")

    # -- Summary --------------------------------------------------------------
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
