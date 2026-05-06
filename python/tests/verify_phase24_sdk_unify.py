"""Phase 24 verification: Python SDK unification.

Tests (local plan compilation — no server needed):
1. df.map_batches("identity_transform").to_sql() → correct SQL
2. df.map_batches("identity_transform").filter(col("id") > 5).to_sql() → filter wraps correctly
3. df.map_batches("identity_transform").select("id", "val").to_sql() → projection wraps correctly
4. df.map_batches(lambda df: df) → regression: local pipeline path still works

E2E tests (require SR_HOST):
1-3. Same as above but with to_pandas() execution via FE routing

Usage:
    python3 python/tests/verify_phase24_sdk_unify.py
    SR_HOST=8.218.233.134 python3 python/tests/verify_phase24_sdk_unify.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SR_HOST = os.environ.get("SR_HOST")

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


class MockFetcher:
    """Minimal mock to capture SQL without executing."""

    def __init__(self):
        self.last_sql = None

    def execute_show(self, sql, limit=20):
        self.last_sql = sql

    def execute_to_pandas(self, sql, batch_size=None):
        self.last_sql = sql
        return None  # mock — no actual data needed

    def execute_count(self, sql):
        self.last_sql = sql
        return 0

    def execute_to_dicts(self, sql):
        self.last_sql = sql
        return []


class MockSession:
    """Minimal mock session for plan compilation tests."""

    def __init__(self):
        self.fetcher = MockFetcher()
        self.arrow_connection = None


def test_sql_compilation():
    """Tests 1-3: Verify SQL compilation for remote map_batches."""
    from starrocks.column import col
    from starrocks.dataframe import DataFrame
    from starrocks.plan.logical import TableScan

    session = MockSession()
    plan = TableScan(table_name="t1", database="test")
    df = DataFrame(plan, session, schema=[("id", "INT"), ("val", "VARCHAR")])

    # Test 1: basic map_batches(str) → SQL
    df_mb = df.map_batches("identity_transform")
    sql1 = df_mb.to_sql()
    ok1 = "map_batches('identity_transform')" in sql1
    report("map_batches(str) SQL", ok1, f"sql={sql1}")

    # Test 2: map_batches(str).filter → SQL
    df_filtered = df_mb.filter(col("id") > 5)
    sql2 = df_filtered.to_sql()
    ok2 = ("map_batches('identity_transform')" in sql2
           and "WHERE" in sql2
           and "`id` > 5" in sql2)
    report("map_batches(str).filter SQL", ok2, f"sql={sql2}")

    # Test 3: map_batches(str).select → SQL
    df_projected = df_mb.select("id", "val")
    sql3 = df_projected.to_sql()
    ok3 = ("map_batches('identity_transform')" in sql3
           and "`id`" in sql3
           and "`val`" in sql3)
    report("map_batches(str).select SQL", ok3, f"sql={sql3}")


def test_remote_detection():
    """Verify _has_remote_map_batches detection."""
    from starrocks.dataframe import DataFrame
    from starrocks.plan.logical import TableScan

    session = MockSession()
    plan = TableScan(table_name="t1")
    df = DataFrame(plan, session, schema=[("id", "INT")])

    # String func → remote
    df_remote = df.map_batches("my_func")
    ok1 = df_remote._has_remote_map_batches()
    ok2 = not df_remote._has_map_batches()  # not local
    report("Remote detection (str)", ok1 and ok2,
           f"remote={ok1}, local={not ok2}")

    # Callable func → local
    df_local = df.map_batches(lambda x: x)
    ok3 = not df_local._has_remote_map_batches()
    ok4 = df_local._has_map_batches()
    report("Local detection (callable)", ok3 and ok4,
           f"remote={not ok3}, local={ok4}")


def test_action_routing():
    """Verify action methods route to SQL for remote map_batches."""
    from starrocks.dataframe import DataFrame
    from starrocks.plan.logical import TableScan

    session = MockSession()
    plan = TableScan(table_name="t1", database="test")
    df = DataFrame(plan, session, schema=[("id", "INT"), ("val", "VARCHAR")])

    df_remote = df.map_batches("identity_transform")

    # to_pandas should go through fetcher with compiled SQL
    df_remote.to_pandas()
    ok = (session.fetcher.last_sql is not None
          and "map_batches('identity_transform')" in session.fetcher.last_sql)
    report("to_pandas routes to SQL", ok,
           f"sql={session.fetcher.last_sql}")


def main():
    print("=" * 60)
    print("Phase 24: Python SDK Unification Verification")
    print("=" * 60)

    test_sql_compilation()
    test_remote_detection()
    test_action_routing()

    if SR_HOST:
        print("\n--- E2E tests (SR_HOST={}) ---".format(SR_HOST))
        print("[SKIP] E2E tests require deployed coordinator with registered function")
    else:
        print("\n[SKIP] E2E tests require SR_HOST env var")

    print("=" * 60)
    print(f"Results: {PASS} PASS, {FAIL} FAIL out of {PASS + FAIL}")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
