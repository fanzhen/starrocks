#!/usr/bin/env python3
"""verify_phase2.py — Phase 2 验收脚本"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks import Session, col, func

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

session = Session(host=os.getenv("SR_HOST","127.0.0.1"), port=int(os.getenv("SR_PORT","9030")),
                  user="root", database="test_dataframe")

# Setup
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
session.execute("DROP TABLE IF EXISTS test_dataframe.sales")
session.execute("""
    CREATE TABLE test_dataframe.sales (
        id INT, city VARCHAR(50), revenue DOUBLE, user_id INT
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
""")
session.execute("""INSERT INTO test_dataframe.sales VALUES
    (1,'beijing',100.0,1),(2,'beijing',200.0,2),(3,'shanghai',150.0,1),
    (4,'shanghai',250.0,3),(5,'beijing',300.0,1)""")

df = session.table("sales")

# TC1: group_by + sum
r1 = df.group_by("city").agg(func.sum("revenue").alias("total")).to_pandas()
check("TC1: group_by sum", len(r1) == 2)

# TC2: multiple agg
r2 = df.group_by("city").agg(func.count("*").alias("cnt"), func.avg("revenue").alias("avg_rev")).to_pandas()
check("TC2: multi agg", "cnt" in r2.columns and "avg_rev" in r2.columns)

# TC3: order_by desc
r3 = df.order_by(col("revenue").desc()).to_pandas()
check("TC3: order_by desc", r3.iloc[0]["revenue"] == 300.0)

# TC4: limit
r4 = df.limit(2).to_pandas()
check("TC4: limit", len(r4) == 2)

# TC5: distinct
r5 = df.select("city").distinct().to_pandas()
check("TC5: distinct", len(r5) == 2)

# TC6: count_distinct
sql6 = df.group_by("city").agg(func.count_distinct("user_id").alias("uv")).to_sql()
check("TC6: count_distinct SQL", "DISTINCT" in sql6.upper())

# TC7: full chain E2E
r7 = (df
    .filter(col("revenue") > 100)
    .group_by("city")
    .agg(func.sum("revenue").alias("total"))
    .order_by(col("total").desc())
    .limit(10)
    .to_pandas())
check("TC7: full chain", len(r7) > 0 and r7.iloc[0]["total"] > r7.iloc[-1]["total"])

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.sales")
session.close()
print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL ===")
print("PHASE 2 ACCEPTED" if FAIL == 0 else "PHASE 2 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
