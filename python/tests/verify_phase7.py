#!/usr/bin/env python3
"""verify_phase7.py — Phase 7 验收脚本: 向量函数集成"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

from starrocks import Session, col
from starrocks import functions as func

session = Session(host=os.getenv("SR_HOST","127.0.0.1"), port=int(os.getenv("SR_PORT","9030")),
                  user="root", database="test_dataframe")
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")

# Setup: create table with ARRAY<FLOAT> columns
session.execute("DROP TABLE IF EXISTS test_dataframe.vec_test")
session.execute("""
    CREATE TABLE test_dataframe.vec_test (
        id INT,
        grp VARCHAR(10),
        emb1 ARRAY<FLOAT>,
        emb2 ARRAY<FLOAT>
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
""")
session.execute("""
    INSERT INTO test_dataframe.vec_test VALUES
    (1, 'a', [1.0, 0.0, 0.0], [1.0, 0.0, 0.0]),
    (2, 'a', [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]),
    (3, 'b', [1.0, 1.0, 0.0], [1.0, 0.0, 0.0]),
    (4, 'b', [0.5, 0.5, 0.5], [0.5, 0.5, 0.5])
""")

# TC1: raw SQL cosine_similarity
print("--- TC1: raw SQL cosine_similarity ---")
rows = session.fetch_all("SELECT id, cosine_similarity(emb1, emb2) AS sim FROM test_dataframe.vec_test ORDER BY id")
sims = {r[0]: r[1] for r in rows}
check("TC1: identical vectors sim=1.0", abs(sims[1] - 1.0) < 1e-5)
check("TC1: orthogonal vectors sim=0.0", abs(sims[2] - 0.0) < 1e-5)
check("TC1: same vectors sim=1.0", abs(sims[4] - 1.0) < 1e-5)

# TC2: DataFrame API cosine_similarity
print("--- TC2: DataFrame cosine_similarity ---")
df = session.table("vec_test")
r2 = (df.select(
    col("id"),
    func.cosine_similarity(col("emb1"), col("emb2")).alias("sim")
).order_by("id").to_pandas())
check("TC2: DataFrame sim row count", len(r2) == 4)
check("TC2: identical=1.0", abs(r2.iloc[0]["sim"] - 1.0) < 1e-5)
check("TC2: orthogonal=0.0", abs(r2.iloc[1]["sim"] - 0.0) < 1e-5)

# TC3: DataFrame API l2_distance
print("--- TC3: DataFrame l2_distance ---")
r3 = (df.select(
    col("id"),
    func.l2_distance(col("emb1"), col("emb2")).alias("dist")
).order_by("id").to_pandas())
check("TC3: identical dist=0.0", abs(r3.iloc[0]["dist"] - 0.0) < 1e-5)
check("TC3: orthogonal dist>0", r3.iloc[1]["dist"] > 0.5)

# TC4: cosine_similarity_norm
print("--- TC4: cosine_similarity_norm ---")
r4 = (df.select(
    col("id"),
    func.cosine_similarity_norm(col("emb1"), col("emb2")).alias("sim_norm")
).order_by("id").to_pandas())
check("TC4: norm identical=1.0", abs(r4.iloc[0]["sim_norm"] - 1.0) < 1e-5)

# TC5: aggregation with vector function
print("--- TC5: group_by + avg(cosine_similarity) ---")
r5 = (df.select(
    col("grp"),
    func.cosine_similarity(col("emb1"), col("emb2")).alias("sim")
).group_by("grp").agg(func.avg("sim").alias("avg_sim")).order_by("grp").to_pandas())
check("TC5: group count", len(r5) == 2)
# group 'a': (1.0 + 0.0) / 2 = 0.5
check("TC5: group a avg=0.5", abs(r5.iloc[0]["avg_sim"] - 0.5) < 1e-5)

# TC6: filter by vector similarity
print("--- TC6: filter by similarity ---")
r6 = (df.select(
    col("id"),
    func.cosine_similarity(col("emb1"), col("emb2")).alias("sim")
).filter(func.cosine_similarity(col("emb1"), col("emb2")) > 0.5).to_pandas())
check("TC6: filter sim>0.5", len(r6) == 2)  # id=1 (sim=1.0), id=4 (sim=1.0)

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.vec_test")
session.close()
print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL ===")
print("PHASE 7 ACCEPTED" if FAIL == 0 else "PHASE 7 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
