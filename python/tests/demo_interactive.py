#!/usr/bin/env python3
"""
StarRocks DataFrame API 交互式体验脚本

用法:
  # 在远程服务器上
  cd /root/starrocks/python
  python3.11 tests/demo_interactive.py

  # 或本地通过 SSH tunnel (先 ssh -L 9030:127.0.0.1:9030 root@47.239.57.232)
  SR_HOST=127.0.0.1 python3.11 tests/demo_interactive.py
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks import Session, col
from starrocks import functions as func
from starrocks.dataframe import Window

# ── 连接 ──────────────────────────────────────────────────
host = os.getenv("SR_HOST", "127.0.0.1")
session = Session(host=host, port=9030, user="root")
session.execute("CREATE DATABASE IF NOT EXISTS demo")
session.execute("USE demo")
print(f"Connected to StarRocks @ {host}:9030\n")

# ── 建表 & 灌数据 ─────────────────────────────────────────
session.execute("DROP TABLE IF EXISTS demo.orders")
session.execute("DROP TABLE IF EXISTS demo.products")

session.execute("""
CREATE TABLE demo.orders (
    order_id INT,
    product_id INT,
    customer VARCHAR(20),
    city VARCHAR(20),
    amount DOUBLE,
    order_date DATE
) DISTRIBUTED BY HASH(order_id) BUCKETS 1
""")

session.execute("""
CREATE TABLE demo.products (
    product_id INT,
    name VARCHAR(30),
    category VARCHAR(20),
    embedding ARRAY<FLOAT>
) DISTRIBUTED BY HASH(product_id) BUCKETS 1
""")

session.execute("""
INSERT INTO demo.orders VALUES
(1, 101, 'Alice',   'Beijing',   199.0,  '2025-01-15'),
(2, 102, 'Bob',     'Shanghai',  450.0,  '2025-01-20'),
(3, 101, 'Charlie', 'Beijing',   199.0,  '2025-02-01'),
(4, 103, 'Alice',   'Beijing',   89.5,   '2025-02-14'),
(5, 102, 'Diana',   'Shenzhen',  450.0,  '2025-03-01'),
(6, 101, 'Bob',     'Shanghai',  199.0,  '2025-03-10'),
(7, 103, 'Eve',     'Hangzhou',  89.5,   '2025-03-15'),
(8, 104, 'Alice',   'Beijing',   1299.0, '2025-04-01'),
(9, 102, 'Frank',   'Shenzhen',  450.0,  '2025-04-05'),
(10,104, 'Charlie', 'Beijing',   1299.0, '2025-04-10')
""")

session.execute("""
INSERT INTO demo.products VALUES
(101, 'Wireless Mouse',    'Electronics', [0.1, 0.9, 0.2]),
(102, 'Mechanical Keyboard','Electronics', [0.15, 0.85, 0.25]),
(103, 'USB Cable',          'Accessories', [0.8, 0.1, 0.3]),
(104, 'Monitor 27inch',     'Electronics', [0.12, 0.88, 0.18])
""")

print("=" * 60)
print("  StarRocks DataFrame API Demo")
print("=" * 60)

# ── 1. 基础: 读表 + 查看 ─────────────────────────────────
orders = session.table("orders")
products = session.table("products")

print("\n── 1. 读表 ──")
print("orders.columns:", orders.columns)
orders.show(5)

# ── 2. Filter + Select ───────────────────────────────────
print("\n── 2. 北京订单 (filter + select) ──")
beijing = orders.filter(col("city") == "Beijing").select("order_id", "customer", "amount")
print("SQL:", beijing.to_sql())
beijing.show()

# ── 3. 聚合 ──────────────────────────────────────────────
print("\n── 3. 各城市销售额 (group_by + agg) ──")
city_sales = (
    orders
    .group_by("city")
    .agg(
        func.sum("amount").alias("total"),
        func.count("*").alias("orders"),
        func.avg("amount").alias("avg_amount"),
    )
    .order_by(col("total").desc())
)
print("SQL:", city_sales.to_sql())
city_sales.show()

# ── 4. Join ──────────────────────────────────────────────
print("\n── 4. 订单 + 产品名 (join) ──")
joined = (
    orders
    .join(products, col("orders.product_id") == col("products.product_id"))
    .select("order_id", "customer", "name", "category", "amount")
    .order_by(col("amount").desc())
    .limit(5)
)
print("SQL:", joined.to_sql())
joined.show()

# ── 5. 窗口函数 ──────────────────────────────────────────
print("\n── 5. 客户消费排名 (window function) ──")
ranked = (
    orders
    .select(
        col("customer"),
        col("amount"),
        col("city"),
        func.row_number().over(
            Window.partition_by("city").order_by(col("amount").desc())
        ).alias("rank_in_city"),
    )
    .order_by(col("city"), col("rank_in_city"))
)
print("SQL:", ranked.to_sql())
ranked.show()

# ── 6. 日期函数 + CASE WHEN ──────────────────────────────
print("\n── 6. 按月统计 + 金额分档 (date_trunc + case when) ──")
monthly = (
    orders
    .select(
        func.date_trunc("month", col("order_date")).alias("month"),
        col("amount"),
        func.when(col("amount") >= 1000, "high")
            .when(col("amount") >= 200, "medium")
            .otherwise("low").alias("tier"),
    )
    .group_by("month", "tier")
    .agg(func.count("*").alias("cnt"))
    .order_by(col("month"), col("tier"))
)
print("SQL:", monthly.to_sql())
monthly.show()

# ── 7. 向量相似度 (Phase 7 新功能) ────────────────────────
print("\n── 7. 产品向量相似度 (cosine_similarity) ──")
# 查询向量: 找和 [0.1, 0.9, 0.2] 最相似的产品
from starrocks.plan.expr import Literal, FunctionCall
query_vec = col("embedding")  # 自己和自己比较先看看
sim_df = (
    products
    .select(
        col("name"),
        func.cosine_similarity(col("embedding"), col("embedding")).alias("self_sim"),
    )
)
print("SQL:", sim_df.to_sql())
sim_df.show()

# 产品间两两相似度: 用 cross join (通过 SQL)
print("\n── 7b. 产品间相似度 (cross join via session.sql) ──")
cross_sim = session.sql("""
    SELECT a.name AS product_a, b.name AS product_b,
           ROUND(cosine_similarity(a.embedding, b.embedding), 4) AS similarity
    FROM demo.products a, demo.products b
    WHERE a.product_id < b.product_id
    ORDER BY similarity DESC
""")
print("SQL:", cross_sim.to_sql())
cross_sim.show()

# ── 8. to_pandas ─────────────────────────────────────────
print("\n── 8. to_pandas() ──")
pdf = city_sales.to_pandas()
print(pdf)
print(f"\nType: {type(pdf).__name__}, Shape: {pdf.shape}")

# ── 9. 复杂链式查询 ──────────────────────────────────────
print("\n── 9. 复杂查询: 北京电子产品客户消费排名 ──")
complex_query = (
    orders
    .join(products, col("orders.product_id") == col("products.product_id"))
    .filter((col("city") == "Beijing") & (col("category") == "Electronics"))
    .group_by("customer")
    .agg(
        func.sum("amount").alias("total_spend"),
        func.count("*").alias("order_count"),
    )
    .order_by(col("total_spend").desc())
)
print("SQL:", complex_query.to_sql())
complex_query.show()

# ── Cleanup ──────────────────────────────────────────────
session.execute("DROP TABLE IF EXISTS demo.orders")
session.execute("DROP TABLE IF EXISTS demo.products")
session.execute("DROP DATABASE IF EXISTS demo")
session.close()

print("\n" + "=" * 60)
print("  Demo complete! Tables cleaned up.")
print("=" * 60)
