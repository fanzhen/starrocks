#!/usr/bin/env python3
"""verify_phase5.py — Phase 5 验收脚本

TC9 (全文检索 E2E) and TC10 (外表 catalog E2E) are optional —
they require specific StarRocks infrastructure (GIN index, external catalog).
Set SR_SKIP_FTS=1 or SR_SKIP_CATALOG=1 to skip them.
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks import Session, col, Window
from starrocks import functions as func

PASS = FAIL = SKIP = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

def skip(name, reason):
    global SKIP
    print(f"SKIP: {name} ({reason})")
    SKIP += 1

session = Session(host=os.getenv("SR_HOST","127.0.0.1"), port=int(os.getenv("SR_PORT","9030")),
                  user="root", database="test_dataframe")

# -- SQL generation tests (no tables needed) ----------------------------------

# TC1: match_phrase SQL
sql1 = col("content").match_phrase("hello world").to_sql()
check("TC1: match_phrase SQL", sql1 == "`content` MATCH_PHRASE 'hello world'")

# TC2: match_all SQL
sql2 = col("content").match_all("a b").to_sql()
check("TC2: match_all SQL", sql2 == "`content` MATCH_ALL 'a b'")

# TC3: match_any SQL
sql3 = col("content").match_any("a b").to_sql()
check("TC3: match_any SQL", sql3 == "`content` MATCH_ANY 'a b'")

# TC4: with_bm25 SQL — need a table for schema
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
session.execute("DROP TABLE IF EXISTS test_dataframe.docs")
session.execute("""
    CREATE TABLE test_dataframe.docs (
        id INT, content VARCHAR(500)
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
""")
session.execute("""INSERT INTO test_dataframe.docs VALUES
    (1,'starrocks is a fast database'),
    (2,'full text search with tantivy'),
    (3,'starrocks supports bitmap and hll')""")

docs = session.table("docs")
sql4 = docs.with_bm25("content", "query").to_sql()
check("TC4: with_bm25 SQL", "BM25" in sql4 and "AS `score`" in sql4)

# TC5: bitmap_union SQL
sql5 = func.bitmap_union(col("bm")).to_sql()
check("TC5: bitmap_union SQL", sql5 == "BITMAP_UNION(`bm`)")

# TC6: approx_count_distinct SQL
sql6 = func.approx_count_distinct("uid").to_sql()
check("TC6: approx_count_distinct SQL", sql6 == "APPROX_COUNT_DISTINCT(`uid`)")

# TC7: array_agg SQL
sql7 = func.array_agg(col("v")).to_sql()
check("TC7: array_agg SQL", sql7 == "ARRAY_AGG(`v`)")

# TC8: catalog table SQL
from starrocks.plan.logical import TableScan
from starrocks.compiler.sql_compiler import SQLCompiler
compiler = SQLCompiler()
scan = TableScan("orders", database="mydb", catalog="hive_catalog")
sql8 = compiler.compile(scan)
check("TC8: catalog SQL", sql8 == "SELECT * FROM `hive_catalog`.`mydb`.`orders`")

# TC8b: session.catalog().table() API
cat = session.catalog("hive_catalog")
# Just test the plan generation (no actual catalog needed)
from starrocks.plan.logical import TableScan as TS
df_cat = cat.table("mydb.orders")
plan_sql = df_cat.to_sql()
check("TC8b: catalog table SQL", "`hive_catalog`.`mydb`.`orders`" in plan_sql)

# TC9: full-text search E2E (optional)
if os.getenv("SR_SKIP_FTS", "1") == "0":
    # This requires a table with tantivy GIN index
    try:
        r9 = docs.filter(col("content").match_phrase("starrocks")).to_pandas()
        check("TC9: FTS E2E", len(r9) >= 1)
    except Exception as e:
        check(f"TC9: FTS E2E (error: {e})", False)
else:
    skip("TC9: FTS E2E", "SR_SKIP_FTS=1 (no GIN index)")

# TC10: external catalog E2E (optional)
if os.getenv("SR_SKIP_CATALOG", "1") == "0":
    try:
        catalog_name = os.getenv("SR_CATALOG", "hive_catalog")
        ext_table = os.getenv("SR_EXT_TABLE", "default.test_table")
        df10 = session.catalog(catalog_name).table(ext_table)
        r10 = df10.limit(5).to_pandas()
        check("TC10: catalog E2E", len(r10) >= 0)
    except Exception as e:
        check(f"TC10: catalog E2E (error: {e})", False)
else:
    skip("TC10: catalog E2E", "SR_SKIP_CATALOG=1 (no external catalog)")

# TC11: approx_count_distinct E2E
r11 = session.sql("SELECT APPROX_COUNT_DISTINCT(id) AS cnt FROM test_dataframe.docs").to_pandas()
check("TC11: approx_count_distinct E2E", r11.iloc[0]["cnt"] == 3)

# TC12: array_agg E2E
r12 = session.sql("SELECT ARRAY_AGG(id) AS ids FROM test_dataframe.docs").to_pandas()
check("TC12: array_agg E2E", len(r12) == 1)

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.docs")
session.close()
print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL, {SKIP} SKIP ===")
print("PHASE 5 ACCEPTED" if FAIL == 0 else "PHASE 5 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
