#!/usr/bin/env python3
"""verify_phase3.py — Phase 3 验收脚本"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks import Session, col

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

session = Session(host=os.getenv("SR_HOST","127.0.0.1"), port=int(os.getenv("SR_PORT","9030")),
                  user="root", database="test_dataframe")

# Setup
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
session.execute("DROP TABLE IF EXISTS test_dataframe.orders")
session.execute("DROP TABLE IF EXISTS test_dataframe.customers")
session.execute("DROP TABLE IF EXISTS test_dataframe.products")
session.execute("""
    CREATE TABLE test_dataframe.orders (
        order_id INT, cust_id INT, product_id INT, amount DOUBLE
    ) DISTRIBUTED BY HASH(order_id) BUCKETS 1
""")
session.execute("""
    CREATE TABLE test_dataframe.customers (
        cust_id INT, name VARCHAR(50), city VARCHAR(50)
    ) DISTRIBUTED BY HASH(cust_id) BUCKETS 1
""")
session.execute("""
    CREATE TABLE test_dataframe.products (
        product_id INT, product_name VARCHAR(50)
    ) DISTRIBUTED BY HASH(product_id) BUCKETS 1
""")
session.execute("""INSERT INTO test_dataframe.orders VALUES
    (1,1,10,100.0),(2,2,20,200.0),(3,1,20,150.0),(4,3,10,300.0)""")
session.execute("""INSERT INTO test_dataframe.customers VALUES
    (1,'alice','beijing'),(2,'bob','shanghai'),(3,'charlie','beijing')""")
session.execute("""INSERT INTO test_dataframe.products VALUES
    (10,'widget'),(20,'gadget')""")

orders = session.table("orders")
customers = session.table("customers")
products = session.table("products")

# TC1: inner join on column name
r1 = orders.join(customers, on="cust_id").to_pandas()
check("TC1: inner join on='cust_id'", len(r1) == 4)

# TC2: left join
r2 = orders.join(customers, on="cust_id", how="left").to_pandas()
check("TC2: left join", len(r2) == 4)

# TC3: expression join
r3 = orders.alias("o").join(
    customers.alias("c"),
    on=(col("o.cust_id") == col("c.cust_id"))
).to_pandas()
check("TC3: expr join", len(r3) == 4)

# TC4: column disambiguation — both tables have cust_id
sql4 = orders.join(customers, on="cust_id").to_sql()
check("TC4: column disambig", "ON" in sql4 and "cust_id" in sql4)

# TC5: session.sql()
df5 = session.sql("SELECT * FROM orders WHERE amount > 100")
r5 = df5.to_pandas()
check("TC5: session.sql()", len(r5) == 3)
# Continue chaining
r5b = df5.filter(col("amount") > 200).to_pandas()
check("TC5b: sql() + filter chain", len(r5b) == 1 and r5b.iloc[0]["amount"] == 300.0)

# TC6: union
r6 = (session.sql("SELECT cust_id, name FROM customers WHERE city='beijing'")
      .union(session.sql("SELECT cust_id, name FROM customers WHERE city='shanghai'"))
      .to_pandas())
check("TC6: union", len(r6) == 3)

# TC7: union distinct
r7_sql = (session.sql("SELECT city FROM customers")
          .union_distinct(session.sql("SELECT city FROM customers"))
          .to_sql())
check("TC7: union distinct SQL", "UNION DISTINCT" in r7_sql)

# TC8: three-table join E2E
r8 = (orders
      .join(customers, on="cust_id")
      .join(products, on="product_id")
      .to_pandas())
check("TC8: three-table join", len(r8) == 4 and "product_name" in r8.columns)

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.orders")
session.execute("DROP TABLE IF EXISTS test_dataframe.customers")
session.execute("DROP TABLE IF EXISTS test_dataframe.products")
session.close()
print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL ===")
print("PHASE 3 ACCEPTED" if FAIL == 0 else "PHASE 3 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
