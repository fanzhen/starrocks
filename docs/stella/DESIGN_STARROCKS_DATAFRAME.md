# StarRocks DataFrame API 设计文档

> 分支: `fanzhen/main-stella-dataframe`
> 日期: 2026-04-28

---

## 1. 目标

为 StarRocks 提供 **Python DataFrame API**：

1. **惰性查询构建**: 用户通过 `df.filter().group_by().agg()` 链式操作构建查询，不立即执行
2. **服务端执行**: 所有计算下推到 StarRocks 集群，客户端只接收结果数据
3. **StarRocks 专有特性**: 全文检索（MATCH_PHRASE）、BM25 评分、Bitmap/HLL、外表查询等均可通过 DataFrame API 操作

底层采用 **SQL 生成 + MySQL 协议**（Snowpark 模式），纯 Python 客户端，零 StarRocks 服务端改动。

---

## 2. DataFrame API 的用户视角

### 2.1 参考系：PySpark / Snowpark / Ibis

DataFrame API 的行业标准是 PySpark。Snowpark 把同样的模式用在了客户端-服务端数据库上。我们在 API 风格上与 PySpark/Snowpark 对齐，在能力上扩展 StarRocks 专有特性。

**核心 DataFrame 操作 → SQL 映射**:

| DataFrame 操作 | 语义 | 生成的 SQL |
|---------------|------|-----------|
| `df.filter(col("a") > 1)` | 行过滤 | `WHERE a > 1` |
| `df.select("a", "b")` | 列投影 | `SELECT a, b` |
| `df.group_by("a").agg(func.sum("b"))` | 分组聚合 | `GROUP BY a` + `SUM(b)` |
| `df.join(df2, on="key")` | 表连接 | `INNER JOIN ... ON ...` |
| `df.order_by(col("a").desc())` | 排序 | `ORDER BY a DESC` |
| `df.limit(10)` | 限制行数 | `LIMIT 10` |
| `df.filter(col("c").match_phrase("query"))` | 全文检索 | `WHERE c MATCH_PHRASE 'query'` |
| `df.with_bm25("c", "query")` | BM25 评分 | `BM25(c, 'query') AS score` |

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

| 方式 | 问题 |
|------|------|
| 手写 SQL 字符串 | 无 IDE 补全、无类型检查、SQL 注入风险、复杂查询的字符串拼接不可维护 |
| SQLAlchemy ORM | 失去 StarRocks 专有特性（MATCH_PHRASE、BM25、Bitmap 等），ORM 映射开销 |
| `pd.read_sql()` 全量拉取 | 大数据量不可行（10 亿行拉到客户端内存？），无法利用 StarRocks 的分布式计算能力 |

### 3.2 DataFrame API 解决什么

- **可组合性**: 操作可以链式组合、条件分支、函数封装，比 SQL 字符串拼接更安全
- **IDE 友好**: Python 方法名有自动补全和类型提示，不需要记忆 SQL 语法
- **计算下推**: 所有操作转换为 SQL 在 StarRocks 集群执行，客户端只接收结果
- **StarRocks 原生**: 可以暴露 StarRocks 的全部 SQL 能力，包括专有特性

---

## 4. 业界方案调研与选型

### 4.1 四种架构模式

| 模式 | 代表 | 客户端 | 服务端改动 | 通信 |
|------|------|--------|-----------|------|
| **SQL 生成** | Snowpark, Ibis | DataFrame → SQL 字符串 | **无** | 标准数据库协议 |
| **Plan Protocol** | Spark Connect | DataFrame → Protobuf Plan | **高**（新增 gRPC endpoint） | gRPC + Arrow |
| **DAG 代码生成** | MaxFrame | DataFrame → Python DAG → 服务端执行 | **极高**（需 Python 运行时） | REST + msgpack |
| **In-process** | Polars, DuckDB | DataFrame → 本地执行 | N/A（嵌入式） | 无 |

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

## 5. 整体架构

```
┌──────────────────────────────────────────────────────────┐
│  Python Client (pip install starrocks-dataframe)         │
│                                                          │
│  Session ─→ DataFrame ─→ Column/Expr                    │
│    (连接)     (惰性操作链)    (表达式树节点)                │
│                    │                                     │
│              ┌─────▼──────────────┐                      │
│              │  LogicalPlan Tree   │                      │
│              │  (操作的 AST 表示)   │                      │
│              └─────┬──────────────┘                      │
│                    │                                     │
│              ┌─────▼──────────────┐                      │
│              │   SQL Compiler      │                      │
│              │  (AST → SQL 字符串)  │                      │
│              └─────┬──────────────┘                      │
│                    │                                     │
│              ┌─────▼──────────────┐                      │
│              │  Result Fetcher     │                      │
│              │  (MySQL → Pandas)   │                      │
│              └─────┬──────────────┘                      │
└────────────────────┼─────────────────────────────────────┘
                     │ MySQL Protocol (pymysql)
                     ▼
              ┌──────────────┐
              │ StarRocks FE  │  ← 零改动
              │  SQL → Plan   │
              └──────┬───────┘
                     │
              ┌──────▼───────┐
              │ StarRocks BE  │  ← 零改动
              │  分布式执行    │
              └──────────────┘
```

### 5.1 核心组件

| 组件 | 职责 | 关键类 |
|------|------|--------|
| **Session** | 管理 MySQL 连接，提供 `table()`/`sql()` 入口 | `Session` |
| **DataFrame** | 惰性操作链，每个方法返回新 DataFrame | `DataFrame`, `GroupedDataFrame` |
| **Column/Expr** | 表达式树节点，支持运算符重载 | `Column`, `col()`, `func` |
| **LogicalPlan** | 操作的 AST 表示（内部，不暴露给用户） | `TableScan`, `Filter`, `Projection`, `Join`, `Aggregate`, `Sort`, `Limit` |
| **SQL Compiler** | LogicalPlan → SQL 字符串的递归编译器 | `SQLCompiler` |
| **Result Fetcher** | 通过 MySQL 协议执行 SQL，转换结果格式 | `ResultFetcher` |

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
  ▼ (通过 pymysql 发送)
MySQL Protocol → StarRocks FE → BE 执行 → 结果返回
  │
  ▼ (格式化输出)
┌───┬───┐
│ a │ b │
├───┼───┤
│ 2 │ x │
│ 5 │ y │
└───┴───┘
```

### 5.3 LogicalPlan 节点

```python
class LogicalPlan(ABC):
    """逻辑计划基类"""

class TableScan(LogicalPlan):      # FROM table_name
class RawSQL(LogicalPlan):         # FROM (raw_sql) AS alias
class Projection(LogicalPlan):     # SELECT expr1, expr2, ...
class Filter(LogicalPlan):         # WHERE condition
class Join(LogicalPlan):           # left JOIN right ON condition
class Aggregate(LogicalPlan):      # GROUP BY keys + agg_exprs
class Sort(LogicalPlan):           # ORDER BY
class Limit(LogicalPlan):          # LIMIT n
class SetOperation(LogicalPlan):   # UNION / INTERSECT / EXCEPT
class SubqueryAlias(LogicalPlan):  # 子查询别名包装
```

### 5.4 SQL Compiler

SQL 编译器负责将 LogicalPlan 树递归编译为 StarRocks SQL 字符串。

**实现策略**:
- Phase 1-2: 手写字符串编译器（简单直接，快速迭代）
- Phase 3+: 可选切换到 [SQLGlot](https://github.com/tobymao/sqlglot) AST 构建（自动处理标识符引用、CTE 生成、方言差异）

**编译示例**:

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
starrocks-dataframe/                # 独立 Python 包
├── pyproject.toml
├── starrocks/
│   ├── __init__.py                 # 导出 Session, col, func
│   ├── session.py                  # Session 类
│   ├── dataframe.py                # DataFrame, GroupedDataFrame
│   ├── column.py                   # Column 类 + col() 函数
│   ├── functions.py                # func 命名空间
│   ├── types.py                    # StarRocks 类型映射
│   ├── plan/
│   │   ├── logical.py              # LogicalPlan 节点定义
│   │   └── expr.py                 # Expr 节点定义
│   ├── compiler/
│   │   └── sql_compiler.py         # LogicalPlan → SQL 编译器
│   ├── connection/
│   │   └── mysql.py                # MySQL 协议连接管理
│   └── result/
│       └── fetcher.py              # 结果获取与格式化
└── tests/
    ├── test_column.py
    ├── test_dataframe.py
    ├── test_compiler.py            # SQL 生成的单元测试（最重要）
    ├── test_functions.py
    └── test_integration.py         # 连接真实 StarRocks 的集成测试
```

**依赖**:
- `pymysql` — MySQL 协议客户端（轻量，纯 Python）
- `pandas` — 可选，`to_pandas()` 时需要
- `pyarrow` — 可选，`to_arrow()` 时需要

---

## 7. 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 架构模式 | SQL 生成 + MySQL 协议 | 零服务端改动，完全特性支持，开发成本低 |
| 执行模型 | 惰性求值（Lazy） | 与 PySpark/Snowpark 一致，允许全局优化 SQL 生成 |
| 通信协议 | MySQL 协议 (pymysql) | StarRocks 原生支持，无需额外基础设施 |
| SQL 编译器 | 手写递归 → 后续可选 SQLGlot | Phase 1 快速交付，复杂场景再引入 SQLGlot |
| 项目位置 | 独立 Python 包（prototype 阶段在主仓库 `python/` 目录） | 独立发布周期，不影响 FE/BE 编译 |
| DataFrame 不可变性 | 每个操作返回新 DataFrame | 与 PySpark 语义一致，避免副作用 |
| 列名消歧 | 编译器自动生成表别名 + 列引用携带表限定符 | 多表 join 时避免歧义 |
| 字面量安全 | 所有字面量参数化，不做字符串拼接 | 防 SQL 注入 |

---

## 8. 技术风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| SQL 生成正确性 | 生成的 SQL 语法错误或语义不符预期 | 大量 SQL 编译单元测试 + StarRocks 官方 SQL 测试集作为参考 |
| 多表 Join 列名冲突 | `a.id` vs `b.id` 消歧 | 编译器自动为子查询生成别名 |
| SQL 注入 | 用户传入的字面量包含恶意 SQL | 字面量参数化处理 |
| 大结果集内存溢出 | `to_pandas()` 拉取全量数据 | 提供 `to_pandas(batch_size=N)` 分批获取 |
| StarRocks 版本兼容性 | 不同版本 SQL 语法差异 | Session 初始化时检测版本 |

---

## 9. 架构演进路线：从 SQL 生成到多模态计算

### 9.1 业界参考系：三种 DataFrame 架构定位

在讨论演进方向之前，必须理清三种不同的 DataFrame 架构定位。它们经常被混为一谈，但本质上是不同的系统：

| 定位 | 代表 | 核心执行引擎 | Ray 的角色 | SQL 引擎 |
|------|------|------------|-----------|---------|
| **SQL 引擎 + Python 扩展** | Snowflake (Snowpark) | 自有 MPP SQL 引擎 | 旁路计算环境（Container Services） | 主引擎，成熟 CBO |
| **Python 计算框架 + SQL 下推** | MaxCompute (MaxFrame) | Ray（内置 Ray 集群） | **核心执行基底** | MaxCompute SQL，作为优化下推 |
| **独立 DataFrame 引擎** | Daft | Rust 执行内核 | 可选分布式调度 | 无 |

**Snowflake 模式**：SQL 引擎为王，Python/ML 是附属扩展。90% 工作负载走 SQL，10% 的 Python UDF/ML 推理委托给外部计算环境（容器化 Ray）。SQL 执行效率极高，但多模态/ML 算子优化能力有限。

**MaxFrame 模式**：Python-first 分布式计算框架，内置 Ray 集群。DAG 优化器自动决定哪些操作下推到 MaxCompute SQL 引擎、哪些在 Ray 上直接执行。前身是阿里开源的 Mars。与 Daft 定位相似，但 Ray 执行层缺乏 Daft 级别的算子优化（无 Rust 内核、无深度查询优化）。

**Daft 模式**：独立的 DataFrame 引擎，Rust 执行内核 + 查询优化器（逻辑计划 → 物理计划），专为多模态数据设计（Image/Tensor/Embedding 一等类型）。可单机跑，也可通过 Ray 分布式。无 SQL 引擎，结构化 OLAP 能力远弱于 StarRocks/Snowflake。

### 9.2 Snowflake 路线的优劣势分析

**Snowflake 路线（SQL 引擎为核心）的优势**集中在结构化数据场景：

| 维度 | Snowflake 路线 | Daft |
|------|---------------|------|
| SQL 优化器 | 数十年积累的 CBO，统计信息、物化视图、自动 rewrite | 基础查询优化，无 CBO |
| 存储引擎 | 自有列式存储，zone map、bloom filter、多级缓存 | 依赖外部存储（Parquet/S3） |
| 并发与治理 | ACID、time travel、RBAC、多租户隔离 | 无 |
| 结构化查询性能 | MPP + 向量化执行，万亿行级 | 适合百 GB-TB 级 |
| 全文检索/向量索引 | StarRocks 原生支持 tantivy、GIN、向量索引 | 无 |

对于企业 80% 的工作负载（SQL 分析、报表、ETL、检索），StarRocks 的 MPP 引擎比任何 Python DataFrame 引擎强一个量级。

**Snowflake 路线的短板**在多模态/ML 场景：

Snowflake Container Services 本质上是一个**容器调度层**——提供计算资源，但不深入优化数据处理算子。这使它更接近 Ray Data 而非 Daft。Daft 的 Flotilla 基准测试表明，Daft 在多模态处理上显著优于 Ray Data，核心原因是 Daft 有查询优化器 + Rust 执行内核，而 Ray Data 只有调度：

| 能力 | Daft | Ray Data / Snowflake Container |
|------|------|-------------------------------|
| 查询优化器 | 有（逻辑计划 → 物理计划） | 无 |
| 算子融合 | 有（多个 map 合并为一次 data pass） | 无 |
| Predicate pushdown | 有（filter 下推到 Parquet row group） | 无 |
| Projection pruning | 有（只读需要的列） | 无 |
| Rust 执行内核 | 有（filter/sort/join/hash） | 无（纯 Python 调度开销） |
| 智能内存管理 | 有（大于内存数据集 spill to disk） | 有限 |

### 9.3 推荐架构：StarRocks + Daft on Ray

综合以上分析，我们推荐的架构不是"StarRocks 重新实现 Daft"，也不是"把所有 ML 任务简单扔给 Ray Data"，而是 **StarRocks（SQL 王者）+ Daft on Ray（多模态王者）的协作架构**，通过 DataFrame API 统一用户入口：

```
┌──────────────────── 用户视角 ────────────────────────┐
│                                                       │
│            StarRocks DataFrame API                    │
│         （统一接口，用户不感知底层分裂）                  │
│                                                       │
└──────────────┬──────────────────────┬─────────────────┘
               │                      │
        SQL 操作路由             多模态/ML 操作路由
               │                      │
               ▼                      ▼
    ┌─────────────────┐    ┌─────────────────────────┐
    │ StarRocks MPP    │    │  Ray Cluster             │
    │                  │    │  ┌───────────────────┐   │
    │ · filter/join    │    │  │  Daft 执行引擎     │   │
    │ · agg/window     │◄──►│  │  · 查询优化器      │   │
    │ · 全文检索/BM25   │Arrow│  │  · Rust 内核       │   │
    │ · 向量索引       │Flight│  │  · 算子融合        │   │
    │                  │    │  │  · Python UDF      │   │
    │  (结构化数据)     │    │  │  (多模态数据)       │   │
    └─────────────────┘    │  └───────────────────┘   │
                           └─────────────────────────┘
```

**为什么没有冲突**：

1. **职责正交**：StarRocks 做结构化 OLAP（SQL、索引、存储引擎），Daft 做多模态算子优化（embedding、推理、图像处理）。各取所长，无重叠竞争
2. **Ray 是共同基底**：Daft 本身就跑在 Ray 上，StarRocks 的 Ray 集成自然包含 Daft
3. **Arrow 是共同语言**：StarRocks Arrow Flight + Daft Arrow 内存格式，零拷贝数据交换
4. **优化器各司其职**：StarRocks CBO 优化 SQL 执行计划，Daft 查询优化器优化多模态 pipeline。DataFrame API 层做路由决策

**与 MaxFrame 的关系**：这套架构与 MaxFrame 的理念非常相似——都是 SQL 引擎 + Ray 的混合执行模型。核心差异在于：MaxFrame 的 Ray 执行层是自研的（继承自 Mars），算子优化能力有限；我们选择 Daft 作为 Ray 上的执行引擎，直接获得 Rust 内核 + 查询优化器 + 多模态类型系统的能力，避免重复造轮子。

### 9.4 五阶段演进路线

```
阶段 1:  DataFrame → SQL → StarRocks（当前 Plan Phase 1-6）
         纯 Python 客户端，SQL 生成 + MySQL 协议，零服务端改动
         覆盖全部 SQL 能力 + StarRocks 专有特性

阶段 2:  + Rust Native Functions in BE
         复用 tantivy FFI 模式，在 BE 注册 Rust 实现的标量/向量函数
         高频标准化算子（cosine_similarity、embedding lookup）以 C++/Rust 原生性能运行
         用户通过 DataFrame API 调用，生成含自定义函数的 SQL

阶段 3:  + Arrow Flight 数据通道
         StarRocks ↔ 外部系统的高效列式数据交换
         替代 MySQL 协议的结果获取路径，零拷贝传输
         为阶段 4 的 StarRocks ↔ Daft 数据流打基础

阶段 4:  + Daft on Ray 集成
         引入 Ray 集群 + Daft 执行引擎
         DataFrame API 层根据操作类型自动路由：SQL 操作 → StarRocks，多模态操作 → Daft on Ray
         Arrow Flight 做 StarRocks ↔ Daft 数据桥梁
         Python UDF、ML 推理、图像处理在 Daft 执行引擎上跑，享受算子优化

阶段 5:  + 统一调度与优化
         跨 StarRocks + Daft 的全局查询优化
         DataFrame API 层的智能路由：分析查询 cost model，决定最优执行边界
         混合查询（SQL join + ML 推理）的 pipeline 优化
```

### 9.5 各阶段用户体验

**阶段 1-2**（纯 SQL + Rust 扩展）：

```python
session = Session("sr:9030")

# SQL 操作 → StarRocks 执行
orders = session.table("orders")
result = orders.filter(col("date") >= "2024-01-01").group_by("city").agg(func.sum("revenue"))

# StarRocks 专有特性
docs = session.table("articles")
docs.filter(col("content").match_phrase("machine learning")).show()
```

**阶段 4**（StarRocks + Daft on Ray）：

```python
session = Session("sr:9030", ray="ray://head:10001")

# 用户不需要知道哪些操作在 StarRocks 跑，哪些在 Daft/Ray 跑
result = (
    session.table("products")
    .filter(col("category") == "electronics")           # → StarRocks SQL
    .select("id", "image_url", "description")           # → StarRocks SQL
    .map_batches(clip_embed, input="image_url",
                 output="embedding")                     # → Daft on Ray
    .map_batches(sentiment_analysis, input="description",
                 output="score")                         # → Daft on Ray
    .filter(col("score") > 0.8)                         # → Daft（或回推 StarRocks）
    .write.to_table("enriched_products")                # → 写回 StarRocks
)

# 混合查询：StarRocks join + Daft 预处理
enriched = session.table("enriched_products")
orders = session.table("orders")
report = (
    orders
    .join(enriched, on="product_id")                    # → StarRocks SQL
    .group_by("category")
    .agg(func.avg("score"), func.sum("revenue"))        # → StarRocks SQL
    .order_by(col("revenue").desc())
)
```

### 9.6 三层架构总结

```
Layer 3:  统一 DataFrame API（StarRocks Python SDK）
                ↓ 路由决策（操作类型 → 执行引擎选择）
Layer 2:  StarRocks SQL Engine  ←─Arrow Flight─→  Daft on Ray
          (结构化 OLAP)                             (多模态 ML)
                ↓                                       ↓
Layer 1:  StarRocks 存储                     Object Storage (S3/OSS)
          (列存/索引/缓存)                     (Parquet/图片/模型)
```

### 9.7 短期路线图补充

在完成阶段 1（当前 Plan Phase 1-6）的同时，以下能力可以并行推进：

1. **Ibis 后端**: 开发 `ibis-starrocks` 后端插件，让 Ibis 用户也能使用 StarRocks。自研 SDK 提供 StarRocks 专有特性，Ibis 后端提供生态兼容性，两者共存。

2. **Arrow Flight SQL**: 当 StarRocks 支持 Arrow Flight SQL 协议后，结果获取从 MySQL 协议切换到 Arrow Flight，获得零拷贝列式数据传输。这也是阶段 3-4 的前置依赖。

3. **Write 支持**: `df.write.to_table("target", mode="append")` — 通过 INSERT INTO SELECT 或 Stream Load 将 DataFrame 结果写入 StarRocks 表。

4. **AI/ML 集成**: 与 LangChain、LlamaIndex 集成，让 LLM 通过 DataFrame API 操作 StarRocks 数据。

---

## 10. 开放问题（待讨论）

1. **包名**: `starrocks-dataframe` vs `pystarrocks` vs `starrocks-python-sdk`？
2. **是否支持 async**: Phase 1 不做，后续可加 `async_session`。
3. **与现有 `starrocks` PyPI 包的关系**: 现有包是 SQLAlchemy dialect，新 SDK 互补而非替代。
4. **DDL 支持**: Phase 1 不做，用 `session.execute("CREATE TABLE ...")` 即可。
5. **Daft 集成时机**: 阶段 4 的 Daft on Ray 集成是否应在阶段 1 完成后立即启动？还是等 Arrow Flight SQL 就绪后再开始？
6. **多模态类型系统**: DataFrame API 层是否需要在阶段 1 就预留 Image/Tensor/Embedding 类型标注，为阶段 4 做准备？
