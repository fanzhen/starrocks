#!/usr/bin/env python3
"""verify_phase4.py — Phase 4 验收脚本"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks import Session, col, Window
from starrocks import functions as func

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

session = Session(host=os.getenv("SR_HOST","127.0.0.1"), port=int(os.getenv("SR_PORT","9030")),
                  user="root", database="test_dataframe")

# Setup
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
session.execute("DROP TABLE IF EXISTS test_dataframe.employees")
session.execute("""
    CREATE TABLE test_dataframe.employees (
        id INT, name VARCHAR(50), dept VARCHAR(50),
        salary DOUBLE, hire_date DATE
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
""")
session.execute("""INSERT INTO test_dataframe.employees VALUES
    (1,'alice','eng',8000.0,'2020-01-15'),
    (2,'bob','eng',9000.0,'2019-06-01'),
    (3,'charlie','sales',7000.0,'2021-03-10'),
    (4,'diana','sales',7500.0,'2020-11-20'),
    (5,'eve','eng',9500.0,'2018-09-05')""")

emp = session.table("employees")

# TC1: func.year()
sql1 = emp.select(func.year(col("hire_date")).alias("yr")).to_sql()
check("TC1: func.year()", "YEAR" in sql1)
r1 = emp.select(func.year(col("hire_date")).alias("yr")).to_pandas()
years = sorted(r1["yr"].tolist())
check("TC1b: year values", years == [2018, 2019, 2020, 2020, 2021])

# TC2: func.date_trunc()
sql2 = emp.select(func.date_trunc("month", col("hire_date")).alias("m")).to_sql()
check("TC2: func.date_trunc()", "DATE_TRUNC" in sql2)

# TC3: func.concat()
sql3 = func.concat(col("name"), func.lit(" - "), col("dept")).to_sql()
check("TC3: func.concat()", sql3 == "CONCAT(`name`, ' - ', `dept`)")

# TC4: func.when().otherwise()
sql4 = func.when(col("salary") > 8000, "high").otherwise("normal").to_sql()
check("TC4: case when", sql4 == "CASE WHEN `salary` > 8000 THEN 'high' ELSE 'normal' END")
r4 = emp.select(
    col("name"),
    func.when(col("salary") > 8000, "high").otherwise("normal").alias("level")
).to_pandas()
high_count = len(r4[r4["level"] == "high"])
check("TC4b: case when E2E", high_count == 2)  # bob=9000, eve=9500

# TC5: func.coalesce()
sql5 = func.coalesce(col("name"), func.lit("unknown")).to_sql()
check("TC5: coalesce", sql5 == "COALESCE(`name`, 'unknown')")

# TC6: window function — row_number
w = Window.partition_by("dept").order_by(col("salary").desc())
sql6 = emp.select(
    col("name"), col("dept"), col("salary"),
    func.row_number().over(w).alias("rn")
).to_sql()
check("TC6: window SQL", "ROW_NUMBER()" in sql6 and "OVER" in sql6 and "PARTITION BY" in sql6)
r6 = emp.select(
    col("name"), col("dept"), col("salary"),
    func.row_number().over(w).alias("rn")
).to_pandas()
# In eng dept: eve(9500)=1, bob(9000)=2, alice(8000)=3
eng = r6[r6["dept"] == "eng"].sort_values("rn")
check("TC6b: window ranking", list(eng["name"]) == ["eve", "bob", "alice"])

# TC7: with_column
r7 = emp.with_column("bonus", col("salary") * 0.1).to_pandas()
check("TC7: with_column", "bonus" in r7.columns and abs(r7[r7["name"]=="alice"]["bonus"].iloc[0] - 800.0) < 0.01)

# TC8: drop
r8 = emp.drop("hire_date").to_pandas()
check("TC8: drop", "hire_date" not in r8.columns and "name" in r8.columns)

# TC9: rename
r9 = emp.rename({"name": "employee_name"}).to_pandas()
check("TC9: rename", "employee_name" in r9.columns and "name" not in r9.columns)

# TC10: date function + aggregate E2E
r10 = (emp
    .select(func.year(col("hire_date")).alias("yr"), col("salary"))
    .group_by("yr")
    .agg(func.sum("salary").alias("total"))
    .order_by(col("yr").asc())
    .to_pandas())
check("TC10: date + agg E2E", len(r10) == 4)  # 2018, 2019, 2020, 2021

# TC11: window ranking E2E — dense_rank
w2 = Window.partition_by("dept").order_by(col("salary").desc())
r11 = emp.select(
    col("name"), col("dept"),
    func.dense_rank().over(w2).alias("dr")
).to_pandas()
check("TC11: dense_rank", len(r11) == 5)

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.employees")
session.close()
print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL ===")
print("PHASE 4 ACCEPTED" if FAIL == 0 else "PHASE 4 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
