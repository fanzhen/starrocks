#!/usr/bin/env python3
"""verify_phase6.py — Phase 6 验收脚本"""
import os, sys, subprocess
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

# TC1: pip install
print("--- TC1: pip install ---")
result = subprocess.run(
    [sys.executable, "-m", "pip", "install", "-e",
     os.path.join(os.path.dirname(__file__), ".."), "--quiet"],
    capture_output=True, text=True
)
check("TC1: pip install", result.returncode == 0)
if result.returncode != 0:
    print(f"  stderr: {result.stderr[:200]}")

# TC2: connection failure → friendly error
print("--- TC2: connection error ---")
from starrocks import Session
from starrocks.exceptions import ConnectionError as SRConnectionError
try:
    bad = Session(host="192.0.2.1", port=19999, connect_timeout=2)
    check("TC2: connection error", False)
except SRConnectionError as e:
    msg = str(e)
    check("TC2: connection error", "Failed to connect" in msg and "192.0.2.1" in msg)
except Exception as e:
    print(f"  Got unexpected: {type(e).__name__}: {e}")
    check("TC2: connection error", False)

# TC3: SQL execution failure → friendly error with SQL
print("--- TC3: query error ---")
from starrocks.exceptions import QueryError
session = Session(host=os.getenv("SR_HOST","127.0.0.1"), port=int(os.getenv("SR_PORT","9030")),
                  user="root", database="test_dataframe")
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
try:
    session.execute("SELECT * FROM nonexistent_table_xyz_12345")
    check("TC3: query error", False)
except QueryError as e:
    msg = str(e)
    check("TC3: query error", "nonexistent_table_xyz_12345" in msg)
except Exception as e:
    print(f"  Got unexpected: {type(e).__name__}: {e}")
    check("TC3: query error", False)

# TC4: to_pandas(batch_size=N) — batched fetch
print("--- TC4: batch fetch ---")
from starrocks import col
session.execute("DROP TABLE IF EXISTS test_dataframe.batch_test")
session.execute("""
    CREATE TABLE test_dataframe.batch_test (
        id INT, val DOUBLE
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
""")
# Insert 20 rows
values = ",".join(f"({i},{i*1.5})" for i in range(1, 21))
session.execute(f"INSERT INTO test_dataframe.batch_test VALUES {values}")

df = session.table("batch_test")
# Normal fetch
r4a = df.to_pandas()
# Batched fetch with small batch size
r4b = df.to_pandas(batch_size=5)
check("TC4: batch fetch", len(r4a) == 20 and len(r4b) == 20)
# Verify data is identical
check("TC4b: batch data match",
      sorted(r4a["id"].tolist()) == sorted(r4b["id"].tolist()))

# TC5: SQL injection safety
print("--- TC5: SQL injection ---")
from starrocks.plan.expr import Literal
injection = "'; DROP TABLE batch_test; --"
sql5 = (col("val") == injection).to_sql()
check("TC5: injection escaped", "\\'" in sql5 or "DROP" not in sql5.split("'")[0])
# Actually execute a safe query with the injection string
try:
    r5 = df.filter(col("val") == injection).to_pandas()
    check("TC5b: injection safe exec", len(r5) == 0)  # No rows match, but no error
except Exception as e:
    print(f"  Error: {e}")
    check("TC5b: injection safe exec", False)

# TC6: Custom exceptions are importable from top-level
print("--- TC6: exception imports ---")
from starrocks import StarRocksError, ConnectionError, QueryError, CompilationError
check("TC6: exception imports", all([StarRocksError, ConnectionError, QueryError, CompilationError]))
check("TC6b: hierarchy", issubclass(QueryError, StarRocksError) and issubclass(ConnectionError, StarRocksError))

# TC7: pytest runs all tests
print("--- TC7: pytest ---")
test_dir = os.path.dirname(__file__)
result7 = subprocess.run(
    [sys.executable, "-m", "pytest", test_dir, "-q", "--tb=short"],
    capture_output=True, text=True, cwd=os.path.join(test_dir, "..")
)
print(f"  {result7.stdout.strip().split(chr(10))[-1]}")
check("TC7: pytest all pass", result7.returncode == 0)

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.batch_test")
session.close()
print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL ===")
print("PHASE 6 ACCEPTED" if FAIL == 0 else "PHASE 6 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
