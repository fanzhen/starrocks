# StarRocks DataFrame API 设计文档

> 分支: `fanzhen/main-stella-dataframe`
> 日期: 2026-04-28

---

## 1. 目标

为 StarRocks 提供 **Python DataFrame API**，统一结构化分析和多模态计算的用户入口：

1. **惰性查询构建**: 用户通过 `df.filter().group_by().agg()` 链式操作构建查询，不立即执行
2. **双引擎执行**: SQL 操作下推到 StarRocks MPP 引擎，Python UDF / 多模态操作路由到 Daft on Ray
3. **服务端托管**: FE 统一路由决策（CBO + 统计信息），Daft Coordinator 作为 FE sidecar 管理 Ray 执行，用户不感知底层分裂
4. **StarRocks 专有特性**: 全文检索、BM25 评分、Bitmap/HLL、外表查询等均可通过 DataFrame API 操作

---

## 2. DataFrame API 的用户视角

### 2.1 参考系：PySpark / Snowpark / Ibis

DataFrame API 的行业标准是 PySpark。Snowpark 把同样的模式用在了客户端-服务端数据库上。我们在 API 风格上与 PySpark/Snowpark 对齐，在能力上扩展 StarRocks 专有特性。

**核心 DataFrame 操作 → SQL 映射**:


| DataFrame 操作                                | 语义      | 生成的 SQL                        |
| ------------------------------------------- | ------- | ------------------------------ |
| `df.filter(col("a") > 1)`                   | 行过滤     | `WHERE a > 1`                  |
| `df.select("a", "b")`                       | 列投影     | `SELECT a, b`                  |
| `df.group_by("a").agg(func.sum("b"))`       | 分组聚合    | `GROUP BY a` + `SUM(b)`        |
| `df.join(df2, on="key")`                    | 表连接     | `INNER JOIN ... ON ...`        |
| `df.order_by(col("a").desc())`              | 排序      | `ORDER BY a DESC`              |
| `df.limit(10)`                              | 限制行数    | `LIMIT 10`                     |
| `df.filter(col("c").match_phrase("query"))` | 全文检索    | `WHERE c MATCH_PHRASE 'query'` |
| `df.with_bm25("c", "query")`                | BM25 评分 | `BM25(c, 'query') AS score`    |


### 2.2 用户 API 设计

#### 连接与读表

```python
from starrocks import Session, col, func

session = Session("127.0.0.1:9030", user="root", database="tpch")

# 读表 → 惰性 DataFrame（不触发执行）
orders = session.table("orders")
customers = session.table("customers")

# 从 SQL 创建 DataFrame
df = session.sql("SELECT * FROM orders WHERE o_orderdate >= '2024-01-01'")

# 查看表结构
print(orders.schema)    # 列名 + 类型
print(orders.columns)   # ['o_orderkey', 'o_custkey', ...]
```

#### 查询链（惰性）

```python
# 所有操作返回新 DataFrame，不触发执行
result = (
    orders
    .filter(col("o_orderdate") >= "2024-01-01")
    .join(customers, col("o_custkey") == col("c_custkey"))
    .group_by("c_nation")
    .agg(
        func.sum("o_totalprice").alias("total_revenue"),
        func.count("*").alias("order_count"),
    )
    .order_by(col("total_revenue").desc())
    .limit(20)
)
```

#### Action（触发执行）

```python
result.show()                    # 打印表格到控制台
result.show(50)                  # 打印 50 行
pdf = result.to_pandas()         # 转 Pandas DataFrame
n = result.count()               # SELECT COUNT(*)
print(result.to_sql())           # 打印生成的 SQL（不执行）
result.explain()                 # EXPLAIN 执行计划
```

#### 表达式与函数

```python
# 比较 / 逻辑 / 算术
col("a") > 1
col("a").between(10, 20)
col("a").is_in(1, 2, 3)
col("a").is_null()
(col("a") > 1) & (col("b") < 10)

# 聚合
func.sum("revenue")
func.count_distinct("user_id")
func.approx_count_distinct("user_id")

# 日期 / 字符串 / 数学
func.year(col("dt"))
func.date_trunc("month", col("dt"))
func.concat(col("first"), func.lit(" "), col("last"))
func.round(col("price"), 2)

# 条件
func.when(col("status") == "active", 1).otherwise(0)
func.coalesce(col("a"), col("b"), func.lit(0))

# 窗口
func.row_number().over(Window.partition_by("dept").order_by(col("salary").desc()))
```

#### StarRocks 专有特性

```python
# 全文检索
df.filter(col("content").match_phrase("machine learning"))
df.filter(col("content").match_all("高性能 实时 分析"))

# BM25 相关性评分
df.with_bm25("content", "machine learning", alias="score") \
  .order_by(col("score").desc()).limit(10)

# Bitmap / HLL
df.group_by("page").agg(func.bitmap_union(col("user_bitmap")))

# 外表查询
hive_df = session.catalog("hive_catalog").table("analytics.events")
hive_df.join(session.table("users"), on="user_id")
```

---

## 3. 为什么需要 DataFrame API

### 3.1 现状的问题

用 Python 操作 StarRocks 目前有三种方式，都有明显短板：


| 方式                   | 问题                                                     |
| -------------------- | ------------------------------------------------------ |
| 手写 SQL 字符串           | 无 IDE 补全、无类型检查、SQL 注入风险、复杂查询的字符串拼接不可维护                 |
| SQLAlchemy ORM       | 失去 StarRocks 专有特性（MATCH_PHRASE、BM25、Bitmap 等），ORM 映射开销 |
| `pd.read_sql()` 全量拉取 | 大数据量不可行（10 亿行拉到客户端内存？），无法利用 StarRocks 的分布式计算能力         |


### 3.2 DataFrame API 解决什么

- **可组合性**: 操作可以链式组合、条件分支、函数封装，比 SQL 字符串拼接更安全
- **IDE 友好**: Python 方法名有自动补全和类型提示，不需要记忆 SQL 语法
- **计算下推**: 所有操作转换为 SQL 在 StarRocks 集群执行，客户端只接收结果
- **StarRocks 原生**: 可以暴露 StarRocks 的全部 SQL 能力，包括专有特性

### 3.3 为什么不直接用 Daft

Daft 是优秀的多模态 DataFrame 引擎，我们也选择它作为多模态执行层（§9.3）。但直接用 Daft 替代 StarRocks DataFrame 不可行，因为企业数据管道的核心痛点不在多模态处理本身，而在**结构化数据基础设施与多模态能力的衔接**。

**Daft 单独使用的短板**：

| 维度 | StarRocks | Daft |
|------|-----------|------|
| 存储 | 列式存储引擎，zone map / bloom filter / 倒排索引 / 多级缓存 | 无存储，依赖外部 Parquet/S3 |
| SQL 优化 | 成熟 CBO，统计信息驱动，物化视图自动 rewrite | 基础查询优化，无 CBO |
| 并发分析 | MPP 向量化执行，万亿行级，百并发 | 单任务为主，百 GB-TB 级 |
| 数据治理 | ACID、RBAC、审计、多租户隔离 | 无 |
| 实时写入 | 秒级可见的实时导入 | 无 |

对于企业 80% 的工作负载（SQL 报表、ETL、实时看板），StarRocks 比任何 Python DataFrame 引擎强一个量级。Daft 的价值在剩下的 20%——多模态处理、ML 推理、图像/文本分析。

**StarRocks DataFrame 的核心价值是：让用户在 StarRocks 数据基础设施上，用统一的 DataFrame API 同时完成结构化分析和多模态处理，数据不搬家、治理不断裂。**

以下三个场景说明"结构化 + 多模态"的衔接为什么不能分裂成两个系统：

**场景 1：电商商品智能标注**

10 亿商品存储在 StarRocks（品类、价格、销量、库存、图片 URL）。运营需要对"电子类、月销 > 1000、无标签"的商品批量生成 CLIP embedding 用于以图搜图。

```python
# StarRocks DataFrame：一条管道，结构化筛选在 StarRocks（索引加速），embedding 生成在 Daft
session.table("products") \
    .filter((col("category") == "electronics") & (col("monthly_sales") > 1000) & col("tags").is_null()) \
    .map_batches(generate_clip_embedding) \
    .to_starrocks("product_embeddings")
```

纯 Daft 做法：先从 StarRocks 导出数据到 Parquet/S3（ETL 管道、调度、数据一致性），再用 Daft 读取处理，最后导回 StarRocks（又一个 ETL）。三步变一步，且中间的筛选无法利用 StarRocks 的索引。

**场景 2：日志异常检测**

百亿行日志存储在 StarRocks，建有倒排索引（全文检索）和时间分区。安全团队需要对"最近 1 小时、包含 'authentication failed' 的日志"跑异常检测模型。

```python
# StarRocks 倒排索引 + 时间分区裁剪：百亿行中毫秒级定位候选集（可能只有几千行）
# 只有候选集进入 ML 模型，而不是全量扫描
session.table("security_logs") \
    .filter(col("ts") >= "2026-04-30 09:00:00") \
    .filter(col("message").match_phrase("authentication failed")) \
    .map_batches(anomaly_detect_model) \
    .filter(col("anomaly_score") > 0.9) \
    .to_pandas()
```

纯 Daft：无倒排索引、无分区裁剪。要么全量扫描百亿行（小时级），要么自建索引基础设施（等于重建 StarRocks 的存储层）。

**场景 3：统一数据平台**

同一份数据同时服务 BI 看板（SQL，亚秒响应，百并发）和数据科学团队（Python，ML 实验）。

- StarRocks DataFrame：一个系统、一个连接、一套权限。BI 用 SQL，数据科学用 DataFrame API + `map_batches`，底层同一份数据。
- 纯 Daft + 独立 BI 工具：两套系统、两套权限、数据一致性需要额外保障（ETL 同步延迟、schema drift）。

---

## 4. 业界方案调研与选型

### 4.1 四种架构模式


| 模式                | 代表             | 客户端                            | 服务端改动                   | 通信             |
| ----------------- | -------------- | ------------------------------ | ----------------------- | -------------- |
| **SQL 生成**        | Snowpark, Ibis | DataFrame → SQL 字符串            | **无**                   | 标准数据库协议        |
| **Plan Protocol** | Spark Connect  | DataFrame → Protobuf Plan      | **高**（新增 gRPC endpoint） | gRPC + Arrow   |
| **DAG 代码生成**      | MaxFrame       | DataFrame → Python DAG → 服务端执行 | **极高**（需 Python 运行时）    | REST + msgpack |
| **In-process**    | Polars, DuckDB | DataFrame → 本地执行               | N/A（嵌入式）                | 无              |


### 4.2 为什么选择 SQL 生成（Snowpark 模式）

1. **零服务端改动**: StarRocks 已有成熟的 SQL Parser → Analyzer → Optimizer → Executor 全链路。DataFrame API 只需在客户端生成 SQL，通过 MySQL 协议提交即可。不需要新增 gRPC 服务、不需要改 FE/BE。
2. **完全特性支持**: 可以生成任何 StarRocks 支持的 SQL 语法。Spark Connect 模式需要在 Protobuf schema 中逐个定义支持的操作，而 SQL 生成天然支持所有 SQL 能力。
3. **开发成本低一个数量级**: 纯 Python 项目，独立于 StarRocks 主仓库。Spark Connect 模式需要修改 FE Java 代码、定义 Protobuf schema、实现 gRPC 服务端 — 工程量大且侵入性强。
4. **可调试性**: 用户可以 `print(df.to_sql())` 查看生成的 SQL，直接复制到 mysql 客户端执行验证。Plan Protocol 模式下用户看到的是二进制 Protobuf，无法直接调试。
5. **可维护性**: SQL 是稳定的接口契约。FE/BE 内部重构不影响 DataFrame SDK，只要 SQL 语义不变。

### 4.3 为什么不选其他方案

**不选 Ibis 后端（作为唯一方案）**:

- Ibis API 围绕通用关系代数设计，**无法暴露 StarRocks 专有特性**（MATCH_PHRASE、bm25()、BITMAP_UNION、物化视图等）
- StarRocks 的类型系统（BITMAP、HLL、ARRAY、MAP、STRUCT、JSON）与 Ibis 类型映射存在 gap
- 后续可以作为可选集成，但不应作为唯一方案

**不选 Spark Connect 模式（gRPC + Plan Protocol）**:

- 需要在 FE 新增 gRPC 服务端 + Plan 反序列化 + 与现有 Planner 对接 — 工程量大
- StarRocks FE 已有 SQL Parser + Analyzer + Planner，再建一套 Plan 入口是重复建设
- SQL 生成可以达到相同的用户体验，工程成本低一个数量级

**不选 MaxFrame 模式（DAG + 服务端 Python）**:

- 需要在 StarRocks 服务端运行 Python 环境 — 架构复杂度极高
- MaxFrame 的 Pandas 完全兼容策略（包括 UDF）需要服务端 Python worker，StarRocks 不需要这个能力

---

## 5. SQL 引擎架构

DataFrame API 的基础层：用户的链式操作构建 LogicalPlan 树，SQL Compiler 将其编译为 StarRocks SQL，通过 MySQL / Arrow Flight 协议提交执行。这一层覆盖全部 SQL 能力，是纯结构化查询的完整方案，也是双引擎架构（§9）的基础。

```
┌──────────────────────────────────────────────────────────┐
│  Python Client                                            │
│                                                           │
│  Session ─→ DataFrame ─→ Column/Expr                     │
│    (连接)     (惰性操作链)    (表达式树节点)                 │
│                    │                                      │
│              ┌─────▼──────────────┐                       │
│              │  LogicalPlan Tree   │                       │
│              └─────┬──────────────┘                       │
│                    │                                      │
│              ┌─────▼──────────────┐                       │
│              │   SQL Compiler      │                       │
│              └─────┬──────────────┘                       │
│                    │                                      │
│              ┌─────▼──────────────┐                       │
│              │  Result Fetcher     │                       │
│              └─────┬──────────────┘                       │
└────────────────────┼──────────────────────────────────────┘
                     │ MySQL / Arrow Flight SQL
                     ▼
              ┌──────────────┐
              │ StarRocks     │
              │ FE → BE       │  ← 零改动
              └──────────────┘
```

### 5.1 核心组件

| 组件 | 职责 | 关键类 |
|------|------|--------|
| **Session** | 管理连接，提供 `table()` / `sql()` 入口 | `Session` |
| **DataFrame** | 惰性操作链，每个方法返回新 DataFrame | `DataFrame`, `GroupedDataFrame` |
| **Column/Expr** | 表达式树节点，支持运算符重载 | `Column`, `col()`, `func` |
| **LogicalPlan** | 操作的 AST 表示（内部） | `TableScan`, `Filter`, `Projection`, `Join`, `Aggregate`, `Sort`, `Limit` |
| **SQL Compiler** | LogicalPlan → SQL 字符串的递归编译器 | `SQLCompiler` |
| **Result Fetcher** | 执行 SQL，转换结果格式 | `ResultFetcher` |

### 5.2 数据流

```
用户调用 df.filter(col("a") > 1).select("a", "b")
  │
  ▼ (构建 LogicalPlan 树)
Projection(columns=[a, b])
  └── Filter(condition=a > 1)
        └── TableScan(table="t")
  │
  ▼ (用户调用 .show() 触发执行)
SQL Compiler 递归遍历 → "SELECT `a`, `b` FROM `t` WHERE `a` > 1 LIMIT 20"
  │
  ▼ (通过 MySQL/Arrow Flight 发送)
StarRocks FE → BE 执行 → 结果返回 → 格式化输出
```

### 5.3 LogicalPlan 节点

```python
class LogicalPlan(ABC):       # 逻辑计划基类
class TableScan(LogicalPlan):      # FROM table_name
class RawSQL(LogicalPlan):         # FROM (raw_sql) AS alias
class Projection(LogicalPlan):     # SELECT expr1, expr2, ...
class Filter(LogicalPlan):         # WHERE condition
class Join(LogicalPlan):           # left JOIN right ON condition
class Aggregate(LogicalPlan):      # GROUP BY keys + agg_exprs
class Sort(LogicalPlan):           # ORDER BY
class Limit(LogicalPlan):          # LIMIT n
class SetOperation(LogicalPlan):   # UNION / INTERSECT / EXCEPT
class MapBatches(LogicalPlan):     # Python UDF（触发双引擎路由，见 §9）
```

### 5.4 SQL Compiler

SQL 编译器将 LogicalPlan 树递归编译为 StarRocks SQL 字符串。当前采用手写递归编译器，复杂场景可切换到 [SQLGlot](https://github.com/tobymao/sqlglot)。

```python
# 用户代码
result = (
    orders
    .filter(col("o_orderdate") >= "2024-01-01")
    .join(customers, col("o_custkey") == col("c_custkey"))
    .group_by("c_nation")
    .agg(func.sum("o_totalprice").alias("total_revenue"))
    .order_by(col("total_revenue").desc())
    .limit(20)
)

# 生成的 SQL
SELECT `c_nation`, SUM(`o_totalprice`) AS `total_revenue`
FROM `orders`
INNER JOIN `customers` ON `orders`.`o_custkey` = `customers`.`c_custkey`
WHERE `o_orderdate` >= '2024-01-01'
GROUP BY `c_nation`
ORDER BY `total_revenue` DESC
LIMIT 20
```

---

## 6. 项目结构

```
python/starrocks/                   # 主仓库 python/ 目录
├── __init__.py                     # 导出 Session, col, func, Image, Embedding, Tensor
├── session.py                      # Session（MySQL + Arrow Flight + Ray 连接）
├── dataframe.py                    # DataFrame, GroupedDataFrame
├── column.py                       # Column 类 + col() 函数
├── functions.py                    # func 命名空间
├── types.py                        # 类型映射 + 多模态类型（Image/Embedding/Tensor）
├── plan/
│   ├── logical.py                  # LogicalPlan 节点（含 MapBatches）
│   └── expr.py                     # Expr 节点
├── compiler/
│   └── sql_compiler.py             # LogicalPlan → SQL 编译器
├── connection/
│   ├── mysql.py                    # MySQL 协议
│   ├── flight.py                   # Arrow Flight SQL 客户端
│   └── ray_cluster.py              # Ray 集群连接
├── execution/
│   ├── pipeline.py                 # PipelineExecutor（SQL/Daft 自动路由）
│   ├── daft_ops.py                 # Daft 上执行 Filter/Projection/Limit
│   └── type_mapping.py             # StarRocks ↔ Daft 类型映射
├── result/
│   └── fetcher.py                  # 结果获取与格式化
└── daft_utils.py                   # DaftDataFrame 包装类
```

**依赖**: `pymysql`（必需），`pandas` / `pyarrow` / `getdaft` / `ray`（可选）

---

## 7. 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 架构模式 | SQL 生成 + MySQL 协议 | 零服务端改动，完全特性支持，开发成本低 |
| 执行模型 | 惰性求值（Lazy） | 与 PySpark/Snowpark 一致，允许全局优化 SQL 生成 |
| 数据通道 | MySQL + Arrow Flight 双通道 | 小结果集用 MySQL，大结果集用 Arrow Flight 零拷贝 |
| SQL 编译器 | 手写递归，可选 SQLGlot | 快速交付，复杂场景再引入 SQLGlot |
| DataFrame 不可变性 | 每个操作返回新 DataFrame | 与 PySpark 语义一致，避免副作用 |
| 列名消歧 | 编译器自动生成表别名 | 多表 join 时避免歧义 |
| 字面量安全 | 所有字面量参数化 | 防 SQL 注入 |
| Daft 段执行 | MapBatches 惰性节点 + PipelineExecutor 分段路由 | SQL 段留给 StarRocks，Daft 段（built-in + UDF）路由到 Daft on Ray |

---

## 8. 技术风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| SQL 生成正确性 | 语法错误或语义不符预期 | 大量 SQL 编译单元测试 |
| 多表 Join 列名冲突 | `a.id` vs `b.id` 消歧 | 编译器自动生成表别名 |
| SQL 注入 | 用户字面量包含恶意 SQL | 字面量参数化处理 |
| 大结果集内存溢出 | `to_pandas()` 拉取全量数据 | `to_pandas(batch_size=N)` 分批获取 |
| 跨引擎调试复杂度 | Pipeline 横跨 Java FE + C++ BE + Python Ray | Trace ID 穿透 + 日志聚合 |

---

## 9. 双引擎架构：结构化 + 多模态

纯 SQL 引擎（§5）可以覆盖结构化分析的全部需求。但当用户需要在数据管道中嵌入 **Python UDF / ML 推理 / 图像处理** 时，StarRocks MPP 引擎无法直接执行这些操作。本章讨论为什么需要双引擎、如何选型、以及两种架构方案。

### 9.1 业界参考系：三种 DataFrame 架构定位

| 定位 | 代表 | 核心执行引擎 | Ray 的角色 | SQL 引擎 |
|------|------|------------|-----------|---------|
| **SQL 引擎 + Python 扩展** | Snowflake (Snowpark) | 自有 MPP SQL 引擎 | 旁路计算环境（Container Services） | 主引擎，成熟 CBO |
| **Python 计算框架 + SQL 下推** | MaxCompute (MaxFrame) | Ray（内置 Ray 集群） | **核心执行基底** | MaxCompute SQL，作为优化下推 |
| **独立 DataFrame 引擎** | Daft | Rust 执行内核 | 可选分布式调度 | 无 |

**Snowflake 模式**：SQL 引擎为王，Python/ML 是附属扩展。SQL 执行效率极高，但多模态/ML 算子优化能力有限——Container Services 本质是容器调度层，不优化数据处理算子。

**MaxFrame 模式**：Python-first，DAG 优化器自动决定哪些操作下推到 SQL 引擎。Ray 执行层缺乏 Daft 级别的算子优化（无 Rust 内核、无深度查询优化）。

**Daft 模式**：Rust 执行内核 + 查询优化器，专为多模态数据设计（Image/Tensor/Embedding 一等类型）。无 SQL 引擎，结构化 OLAP 能力远弱于 StarRocks。

### 9.2 Snowflake 路线的多模态短板

StarRocks 在结构化场景的绝对优势已在 §3.3 论证。这里关注：如果走 Snowflake 路线（SQL 引擎 + 容器化 Python 扩展），多模态/ML 场景的短板在哪？

Daft 在多模态处理上显著优于 Snowflake Container Services / Ray Data：

| 能力 | Daft | Ray Data / Container Services |
|------|------|-------------------------------|
| 查询优化器 | 有（逻辑 → 物理计划） | 无 |
| 算子融合 | 有（多 map 合并） | 无 |
| Predicate/Projection pushdown | 有 | 无 |
| Rust 执行内核 | 有 | 无（纯 Python 调度） |

### 9.3 双引擎选型：StarRocks + Daft on Ray

不是"StarRocks 重新实现 Daft"，也不是"把 ML 任务扔给 Ray Data"。而是 **StarRocks（结构化王者）+ Daft on Ray（多模态王者）** 的协作：

- **职责正交**：StarRocks 做 SQL/索引/存储，Daft 做 embedding/推理/图像处理。各取所长，无重叠
- **Arrow 是共同语言**：StarRocks Arrow Flight 输出的 RecordBatch 与 Daft 内存格式同构（Arrow columnar），零拷贝
- **优化器各司其职**：StarRocks CBO 优化 SQL 计划，Daft 优化器优化多模态 pipeline

与 MaxFrame 的核心差异：MaxFrame 的 Ray 执行层是自研的（Mars），算子优化能力有限；我们选择 Daft，直接获得 Rust 内核 + 查询优化器 + 多模态类型系统。

#### Daft 能力模型与兼容性

Daft 提供两层能力，都不是"只有 UDF"：

| 层次 | 说明 | 示例 | 执行 |
|------|------|------|------|
| **Built-in Expressions** | Rust 原生算子，参与查询优化 | `col("img").image.decode()`, `.image.resize()`, `.url.download()`, `.str.contains()`, `.cast()`, 数值/时间/列表操作 | Rust 内核 |
| **UDF** | 用户自定义函数 (`@daft.udf` 装饰器) | CLIP embedding、自定义 NLP 模型、任意 Python 逻辑 | Python worker |

StarRocks DataFrame 通过 `map_batches(func)` 桥接 Daft：`func` 接收 Daft DataFrame，可以使用 Daft 的**全部 API**（built-in expressions + UDF + 第三方库如 transformers/torchvision）。这意味着我们天然兼容 Daft 生态的全部能力，无需逐个适配。

```python
# map_batches 内部是原生 Daft 代码，用户可以使用 Daft 的任何能力
def enrich(daft_df):
    return (
        daft_df
        .with_column("image", col("url").url.download().image.decode())  # Daft built-in
        .with_column("embedding", clip_udf(col("image")))                # Daft UDF
        .with_column("label", col("category").str.lower())               # Daft built-in
    )

session.table("products").filter(col("active") == True).map_batches(enrich).to_starrocks("enriched")
```

### 9.4 POC 架构：客户端路由

路由决策在 Python 客户端完成。`PipelineExecutor` 遍历 LogicalPlan 树，在 `MapBatches` 节点处切分：SQL 段编译为 SQL 发给 StarRocks，Daft 段（built-in expressions + UDF）在 Daft 上执行。

```
┌──────────────────────────────────────────────────────────┐
│  Python Client                                            │
│                                                           │
│  DataFrame → LogicalPlan → PipelineExecutor (路由)        │
│                               │             │             │
│                         SQL 段编译      Daft 段执行         │
│                               │             │             │
└───────────────────────────────┼─────────────┼─────────────┘
                          MySQL/Arrow    daft.from_arrow()
                          Flight              │
                           ▼                  ▼
                    ┌──────────────┐    ┌──────────────┐
                    │ StarRocks     │    │ Ray Cluster   │
                    │ FE → BE       │    │ Daft on Ray   │
                    └──────────────┘    └──────────────┘
```

**用户体验**：

```python
session = Session("sr:9030", arrow_flight_port=9408, ray="ray://head:10001")
session.table("products")
    .filter(col("category") == "electronics")      # → StarRocks SQL
    .map_batches(clip_embed)                         # → Daft on Ray
    .filter(col("score") > 0.8)                     # → Daft
    .to_starrocks("enriched_products")              # → StarRocks
```

**架构特点**：

| 优点 | 局限 |
|------|------|
| 零服务端改动，纯 Python 实现 | 路由是规则匹配（遇到 MapBatches 就切），无法利用统计信息做全局最优决策 |
| 验证了双引擎管道可行性 | 客户端是数据中转站——SQL 结果先拉到客户端，再推给 Daft |
| Arrow Flight 零拷贝传输 | 用户需要安装 Ray/Daft，管理 `ray.init()` 连接 |
| map_batches 内可使用 Daft 全部能力（built-in + UDF） | FE 不感知 Daft 的存在，无法统一 session/认证/审计/资源配额 |

### 9.5 目标架构：FE 路由 + Daft Coordinator Sidecar

路由决策和执行协调上移到 StarRocks FE 侧，用户不感知底层分裂。

#### 9.5.1 架构图

```
┌──────────────────────────────────────────────────────┐
│  Python Client (轻量)                                 │
│                                                      │
│  session = Session("sr:9030")                        │
│  session.table("t")                                  │
│    .filter(col("x") > 1)                             │
│    .map_batches(enrich)                              │
│    .to_pandas()                                      │
│                                                      │
│  提交: 逻辑计划 (含 UDF 引用)                          │
│  不需要安装 Ray/Daft，不需要 ray.init()               │
└──────────────┬───────────────────────────────────────┘
               │ MySQL / Arrow Flight SQL
               ▼
┌──────────────────────────────────────────────────────┐
│  StarRocks FE                                         │
│                                                      │
│  1. 解析逻辑计划                                      │
│  2. 路由决策器（CBO + 统计信息 + 历史执行数据）          │
│     → SQL 段: 正常 Fragment 部署到 BE                   │
│     → Daft 段: 发给 Daft Coordinator                   │
│  3. 协调执行顺序 + 收集结果                              │
└──────┬────────────────────────────┬──────────────────┘
       │ SQL Fragments              │ gRPC: Daft Plan
       ▼                            ▼
┌──────────────┐    ┌─────────────────────────────────┐
│  StarRocks BE │    │  Daft Coordinator (FE sidecar)   │
│  (C++ 执行)    │    │                                 │
│  OlapScan     │    │  承担 Daft Driver 角色:            │
│  Agg/Join     │    │  · DistributedPhysicalPlan       │
│  全文检索      │    │  · FlotillaRunner 调度           │
│               │    │  · UDF 注册表                     │
└──────┬────────┘    └──────────────┬──────────────────┘
       │                            │ dispatch tasks
       │ Arrow Flight (双向)         ▼
       │                       ┌──────────────────┐
       │ ①  BE → Worker:       │  Ray Cluster      │
       │    SQL 结果送入 Daft   │  Workers          │
       │ ②  Worker → BE:       │  (Daft Executor)  │
       │    Daft 结果写回 SR    │                   │
       └──────────────────────▶│                   │
       ◀───────────────────────│                   │
                               └──────────────────┘
```

**数据流方向**：BE ↔ Ray Worker 是**双向**的，不经客户端中转：
- **① BE → Worker**：SQL 段执行完毕后，BE 通过 Arrow Flight 将结果直接推送给 Ray Worker，作为 Daft 段的输入（例如 `filter(SQL) → map_batches(UDF)` 的衔接点）
- **② Worker → BE**：Daft 段处理完毕后，Ray Worker 通过 Arrow Flight 或 Stream Load 将结果写回 StarRocks（例如 `.to_starrocks("enriched_table")`）

#### 9.5.2 相比 POC 架构的核心改进

| 维度 | POC（客户端路由） | 目标（FE 路由） |
|------|----------------|---------------|
| 路由决策 | 规则匹配（遇 MapBatches 就切） | CBO + 统计信息 + 历史数据 |
| 数据流 | BE → 客户端 → Daft（客户端中转） | BE ↔ Ray Worker（双向直连，不经客户端） |
| 用户依赖 | 需安装 Ray/Daft/pyarrow | 只需 pymysql |
| Daft 能力 | 完整（map_batches 内原生 Daft） | 完整（Coordinator 运行完整 Daft Driver） |
| 统一治理 | FE 不感知 Daft | FE 统一管理 session/认证/审计/资源 |

#### 9.5.3 为什么选择 Sidecar

Daft Driver 核心是 Rust + Python（Rust 通过 PyO3 暴露给 Python）。嵌入 Java FE 有三条路：

| 方案 | 做法 | 可行性 |
|------|------|--------|
| A. FE 内嵌 Python | GraalPy / Jython / subprocess | 低：GraalPy 对 PyO3 兼容性不确定 |
| B. Java 重写 Daft Driver | Java 重写调度 + Ray Java API | 极低：等于重写 Daft，丧失升级能力 |
| **C. Sidecar** | **FE 旁部署 Python 进程，gRPC 交互** | **高：FE 只定义协议，Daft 完整复用** |

FE 做路由（Java，CBO），Coordinator 做执行协调（Python，完整 Daft Driver 栈），gRPC 通信可演进，Daft 升级只需更新 Sidecar。

Sidecar 运行完整的 Daft Driver（Rust 内核 + Python bindings），因此 Daft 的全部能力（built-in expressions + UDF + 查询优化）在目标架构中同样完整可用。

#### 9.5.4 Daft Driver-Executor 架构

Coordinator 内部复用 Daft 的 Driver-Executor 模型：

```
Daft Driver (Coordinator)
  │  LogicalPlanBuilder → optimize() → DistributedPhysicalPlan   (Rust)
  │  FlotillaRunner → RemoteFlotillaRunner (Ray Actor, Head Node)
  └──── dispatch tasks ────→ RaySwordfishActor (Ray Workers)
                                → NativeExecutor (Rust) + Python UDF
```

#### 9.5.5 实现挑战

| 挑战 | 说明 | 关键决策 |
|------|------|---------|
| **FE ↔ Coordinator 协议** | gRPC：提交 Daft 段计划、传递 Flight 端点、接收状态/错误 | 传递逻辑计划 vs 物理计划 |
| **函数注册与分发** | 从 inline closure 变为注册 + 名称引用（类似 Flink UDF） | 使用范式改变，但更适合生产 |
| **BE ↔ Ray 双向直连** | Arrow Flight 双向：BE 推送 SQL 结果到 Worker，Worker 写回结果到 BE | FE 协调端点注入 |
| **Sidecar 生命周期** | 随 FE 启停、健康检查、自动重启 | 每 FE 一个 vs 全局共享 |

---

## 10. 设计决策记录

| # | 问题 | 决策 | 理由 |
|---|------|------|------|
| 1 | **包名** | `starrocks-dataframe` | 清晰定位，不与现有 `starrocks` PyPI 包（SQLAlchemy dialect）命名冲突。`import starrocks` 的用户入口保持不变 |
| 2 | **与现有包的关系** | 互补共存 | 现有包是 ORM 接入层，新包是 DataFrame 分析层。可共享底层连接逻辑，但 API 层独立 |
| 3 | **FE ↔ Coordinator 协议** | gRPC + Protobuf | gRPC 支持双向流（适合 Daft 执行状态上报）、proto 定义强类型。Thrift 在 StarRocks 内部用于 FE↔BE，但 Coordinator 是新组件，不受历史约束。传递**逻辑计划**（非物理计划），Coordinator 内部做物理计划优化 |
| 4 | **函数注册范式** | 混合模式 | POC 阶段支持 inline closure（`map_batches(lambda df: ...)`）；目标架构采用注册 + 名称引用（`session.register_udf("clip_embed", func)` → `map_batches("clip_embed")`）。Inline closure 无法序列化到远端 Coordinator，注册模式是生产化的必经之路 |

