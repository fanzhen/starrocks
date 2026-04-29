# StarRocks DataFrame API — 实施计划

> 分支: `fanzhen/main-stella-dataframe`
> 日期: 2026-04-28
> 设计文档: [DESIGN_STARROCKS_DATAFRAME.md](./DESIGN_STARROCKS_DATAFRAME.md)

---

## 0. 环境准备 & 全局约束

### 0.1 Phase 编写规范

**目标与验收标准合并原则**：每个 Phase 的"目标"必须包含具体的验收标准，从用户视角出发描述测试用例。不单独列"目标"一节再另列"验证标准"——两者写在一起，保持紧凑且可执行。

**验收必须通过外部工具**：每个 Phase 的验收标准必须通过外部工具验证，而非依赖代码审查。具体要求：

- **Python 脚本验证**: 编写可重复执行的 Python 测试脚本，输出 PASS/FAIL
- **pytest 验证**: SQL 编译单元测试通过 `pytest` 执行，100% 通过
- **E2E 验证**: 连接真实 StarRocks 集群，执行 DataFrame 操作，对比结果与直接 SQL 执行结果一致

每个 Phase 完成的标志是：**验收脚本全部 PASS**，而非"代码写完"或"测试通过"。

**E2E 验证前提**：E2E 测试需要一个运行中的 StarRocks 实例。Phase 1 起即需要 StarRocks 环境（与 tantivy 项目不同，本项目无基础设施阶段例外——因为不涉及编译 FE/BE，只需要一个可连接的 StarRocks）。

### 0.2 各 Stage 环境要求

| Stage | 环境要求 | 说明 |
|-------|---------|------|
| Stage 1 | Python 3.10+ 本地环境 + StarRocks allin1 Docker 实例 | 纯 Python 客户端，无服务端改动 |
| Stage 2 | + Ray 单节点 + Daft | Daft on Ray POC，MySQL 桥梁模式 |
| Stage 3 | + StarRocks Arrow Flight SQL 支持 | Arrow Flight 替代 MySQL 协议的数据交换路径 |
| Stage 4 | + Ray 集群（多节点） | 完整 Daft 集成 + 自动路由 + 多模态类型 |

各 Stage 的具体环境增量在对应 Stage 开头说明。

### 0.3 分支 & Git

```bash
# 本地创建分支（已完成）
git checkout -b fanzhen/main-stella-dataframe origin/main
```

### 0.4 远程服务器 & StarRocks 部署

本项目是纯 Python 客户端，不需要编译 FE/BE，但 E2E 测试需要一个运行中的 StarRocks 实例。使用 **StarRocks allin1 Docker 镜像**快速部署。

**当前服务器**: `47.239.57.232`

#### 0.4.1 服务器从零搭建（换服务器时参照此节）

```bash
# === 变量 ===
SERVER=47.239.57.232
SSH="ssh -i ~/.ssh/my_ecs.pem root@$SERVER"

# === Step 1: 安装 Docker（Alibaba Cloud Linux 3 / CentOS 8+）===
$SSH "yum install -y yum-utils git"
$SSH "yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo"
$SSH "yum install -y --allowerasing docker-ce docker-ce-cli containerd.io"
$SSH "systemctl start docker && systemctl enable docker"
$SSH "docker --version"  # 确认安装成功

# === Step 2: clone 源码 ===
$SSH "git clone --branch fanzhen/main-stella-dataframe --single-branch https://github.com/fanzhen/starrocks.git /root/starrocks"

# === Step 3: 拉取 StarRocks allin1 镜像 ===
$SSH "docker pull starrocks/allin1-ubuntu:latest"

# === Step 4: 启动容器（必须用 --network=host）===
# 注意：--network=host 是必须的，否则 FE/BE 心跳会因为 hostname 解析到容器内网 IP 而失败
$SSH "docker run -d --name sr-dataframe --privileged --network=host starrocks/allin1-ubuntu:latest"

# === Step 5: 等待 StarRocks 启动（约 30-60 秒）===
sleep 60

# === Step 6: 验证 FE + BE + E2E ===
$SSH "docker ps"  # 确认容器 STATUS 为 healthy
$SSH "docker exec sr-dataframe mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW FRONTENDS\G'"  # Alive=true
$SSH "docker exec sr-dataframe mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW BACKENDS\G'"   # Alive=true

# E2E 全链路验证
$SSH "docker exec sr-dataframe mysql -h127.0.0.1 -P9030 -uroot -e '
  CREATE DATABASE IF NOT EXISTS test_dataframe;
  USE test_dataframe;
  CREATE TABLE IF NOT EXISTS ping (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;
  INSERT INTO ping VALUES (1);
  SELECT * FROM ping;
'"  # 应返回 id=1

# === Step 7: 确认源码 ===
$SSH "ls /root/starrocks/docs/stella/"  # 应看到 DESIGN 和 PLAN 文档
```

#### 0.4.2 关键注意事项

| 问题 | 原因 | 解决 |
|------|------|------|
| `FE saved address not match` | 容器使用 bridge 网络，hostname 解析到 10.88.x.x | 必须 `--network=host` |
| BE heartbeat 端口 | allin1 镜像已内置 BE，无需手动 ADD BACKEND | 验证 `SHOW BACKENDS` Alive=true 即可 |
| 容器重启后数据丢失 | allin1 镜像数据在容器内 | 生产环境需挂载 volume，测试环境可接受 |

#### 0.4.3 本地连接方式

本地连接远程 StarRocks 有两种方式：

**方式 A：安全组开放 9030 端口（推荐）**

在阿里云 ECS 控制台 → 安全组 → 入方向规则，添加：
- 协议: TCP
- 端口范围: 9030/9030
- 授权对象: 0.0.0.0/0（或限制为你的 IP）

开放后可直接连接：
```bash
mysql -h47.239.57.232 -P9030 -uroot -e 'SELECT 1'
```

**方式 B：SSH Tunnel（安全组未开放时使用）**

```bash
# 建立 SSH 隧道：本地 9030 → 远程 127.0.0.1:9030
ssh -i ~/.ssh/my_ecs.pem -L 9030:127.0.0.1:9030 -N -f root@47.239.57.232

# 通过隧道连接
mysql -h127.0.0.1 -P9030 -uroot -e 'SELECT 1'

# Python 连接时使用 host=127.0.0.1
export SR_HOST=127.0.0.1
```

#### 0.4.4 容器管理命令

```bash
# 停止/启动/重启
$SSH "docker stop sr-dataframe"
$SSH "docker start sr-dataframe"
$SSH "docker restart sr-dataframe"

# 进入容器
$SSH "docker exec -it sr-dataframe bash"

# 查看日志
$SSH "docker logs sr-dataframe --tail 50"

# mysql 客户端（容器内）
$SSH "docker exec sr-dataframe mysql -h127.0.0.1 -P9030 -uroot -e 'SELECT 1'"

# mysql 客户端（从本地直连，需要服务器安全组开放 9030 端口）
mysql -h$SERVER -P9030 -uroot -e 'SELECT 1'
```

### 0.5 本地 Python 开发环境

```bash
# 本项目是纯 Python，在本地开发即可
python3 -m venv .venv
source .venv/bin/activate
pip install pymysql pandas pyarrow pytest

# 验证连接远程 StarRocks（需要安全组开放 9030 端口）
python3 -c "
import pymysql
conn = pymysql.connect(host='47.239.57.232', port=9030, user='root')
cur = conn.cursor()
cur.execute('SELECT 1')
print('StarRocks connected:', cur.fetchone())
conn.close()
"
```

### 0.6 项目初始化

```bash
# 在 StarRocks 主仓库中创建 Python 子项目（prototype 阶段）
mkdir -p python/starrocks
mkdir -p python/tests
```

项目结构：
```
python/
├── pyproject.toml
├── starrocks/
│   ├── __init__.py
│   ├── session.py
│   ├── dataframe.py
│   ├── column.py
│   ├── functions.py
│   ├── types.py
│   ├── plan/
│   │   ├── __init__.py
│   │   ├── logical.py
│   │   └── expr.py
│   ├── compiler/
│   │   ├── __init__.py
│   │   └── sql_compiler.py
│   ├── connection/
│   │   ├── __init__.py
│   │   └── mysql.py
│   └── result/
│       ├── __init__.py
│       └── fetcher.py
└── tests/
    ├── conftest.py
    ├── test_column.py
    ├── test_compiler.py
    ├── test_dataframe.py
    ├── test_functions.py
    └── test_integration.py
```

### 0.7 验证流程（每个 Phase 通用）

```
1. 本地: 编写代码 + pytest 单元测试
2. 本地: pytest python/tests/ 全部通过
3. 本地: python3 python/tests/test_integration.py（连接远程 StarRocks E2E）
4. 本地: git add <files> && git commit -m "[Feature] ..."
5. E2E 验收脚本全部 PASS → Phase complete
```

### 0.8 StarRocks 测试环境

E2E 测试连接远程 StarRocks。测试配置通过环境变量：

```bash
export SR_HOST=47.239.57.232
export SR_PORT=9030
export SR_USER=root
export SR_PASSWORD=
export SR_DATABASE=test_dataframe
```

---

## Stage 1: DataFrame SQL 引擎

> **目标**: 纯 Python 客户端，通过 SQL 生成 + MySQL 协议实现完整的 DataFrame API，零 StarRocks 服务端改动。
>
> **对应 DESIGN 9.4 阶段 1**: DataFrame → SQL → StarRocks

---

### Phase 1: 核心框架 + 基础查询

#### 1.1 目标与验收标准

用户可以 `session.table("t").filter(...).select(...).show()`，完整链路 E2E 打通。

**验收用例**:

| # | 用例 | 验收方式 | PASS 条件 |
|---|------|---------|-----------|
| 1 | Session 连接 StarRocks | `session = Session(host, port, user)` 不报错 | 连接成功 |
| 2 | `session.table("t")` 读表 | `df = session.table("t"); print(df.schema)` | 输出列名和类型 |
| 3 | `col("a") > 1` 表达式构建 | `print((col("a") > 1).to_sql())` | 输出 `` `a` > 1 `` |
| 4 | `df.filter().to_sql()` | `df.filter(col("a") > 1).to_sql()` | 输出 `SELECT * FROM t WHERE a > 1` |
| 5 | `df.select("a","b").to_sql()` | `df.select("a","b").to_sql()` | 输出 `SELECT a, b FROM t` |
| 6 | `df.filter().select().to_sql()` | 链式操作 | 正确的 SQL |
| 7 | `df.show()` E2E | 连接 StarRocks → 读表 → filter → show | 控制台打印正确结果 |
| 8 | `df.to_pandas()` E2E | 返回 pandas DataFrame | 数据与直接 SQL 查询一致 |
| 9 | `df.count()` E2E | 返回整数 | 值与 `SELECT COUNT(*) FROM t` 一致 |
| 10 | `df.explain()` | 输出 StarRocks 执行计划 | 包含 `OlapScanNode` 等关键字 |
| 11 | 运算符组合 | `(col("a") > 1) & (col("b") < 10)` | 正确的 AND SQL |
| 12 | 字面量安全 | `col("name") == "O'Brien"` | 正确转义单引号 |

```python
#!/usr/bin/env python3
"""verify_phase1.py — Phase 1 验收脚本"""
import os, sys
sys.path.insert(0, "python")
from starrocks import Session, col

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition:
        print(f"PASS: {name}"); PASS += 1
    else:
        print(f"FAIL: {name}"); FAIL += 1

host = os.getenv("SR_HOST", "127.0.0.1")
port = int(os.getenv("SR_PORT", "9030"))

# TC1: 连接
session = Session(host=host, port=port, user="root", database="test_dataframe")
check("TC1: Session connect", session is not None)

# TC2: 读表
session.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
session.execute("DROP TABLE IF EXISTS test_dataframe.t1")
session.execute("""
    CREATE TABLE test_dataframe.t1 (
        id INT, name VARCHAR(50), value DOUBLE
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
""")
session.execute("INSERT INTO test_dataframe.t1 VALUES (1,'alice',10.5),(2,'bob',20.3),(3,'charlie',30.1)")

df = session.table("t1")
check("TC2: table() schema", len(df.columns) == 3)

# TC3: 表达式
expr_sql = (col("value") > 15).to_sql()
check("TC3: col > 15", "15" in expr_sql and ">" in expr_sql)

# TC4-6: SQL 生成
sql4 = df.filter(col("value") > 15).to_sql()
check("TC4: filter to_sql", "WHERE" in sql4 and "15" in sql4)

sql5 = df.select("id", "name").to_sql()
check("TC5: select to_sql", "id" in sql5 and "name" in sql5)

sql6 = df.filter(col("value") > 15).select("id", "name").to_sql()
check("TC6: chain to_sql", "WHERE" in sql6 and "id" in sql6)

# TC7: show() E2E
df.filter(col("value") > 15).show()
check("TC7: show() no error", True)

# TC8: to_pandas() E2E
pdf = df.filter(col("value") > 15).to_pandas()
check("TC8: to_pandas()", len(pdf) == 2 and set(pdf["name"]) == {"bob", "charlie"})

# TC9: count() E2E
n = df.count()
check("TC9: count()", n == 3)

# TC10: explain()
plan = df.explain()
check("TC10: explain()", len(plan) > 0)

# TC11: 运算符组合
sql11 = df.filter((col("value") > 10) & (col("value") < 25)).to_sql()
check("TC11: AND operator", "AND" in sql11.upper())

# TC12: 字面量安全
sql12 = df.filter(col("name") == "O'Brien").to_sql()
check("TC12: quote escape", "O" in sql12 and "Brien" in sql12)

# Cleanup
session.execute("DROP TABLE IF EXISTS test_dataframe.t1")
session.close()

print(f"\n=== Result: {PASS} PASS, {FAIL} FAIL ===")
print("PHASE 1 ACCEPTED" if FAIL == 0 else "PHASE 1 REJECTED")
sys.exit(0 if FAIL == 0 else 1)
```

#### 1.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 1.1 | `python/pyproject.toml` | 项目元数据，dependencies: pymysql, pandas(optional), pyarrow(optional) |
| 1.2 | `python/starrocks/__init__.py` | 导出 `Session`, `col`, `func` |
| 1.3 | `python/starrocks/connection/mysql.py` | pymysql 连接管理（connect, execute, fetchall, close） |
| 1.4 | `python/starrocks/plan/expr.py` | Expr 节点: `ColumnRef`, `Literal`, `BinaryOp`, `UnaryOp`, `FunctionCall`, `Alias` |
| 1.5 | `python/starrocks/column.py` | Column 类: 运算符重载（`__eq__`, `__gt__`, `__and__` 等）, `to_sql()`, `alias()`, `is_null()`, `between()`, `is_in()`, `like()` |
| 1.6 | `python/starrocks/plan/logical.py` | LogicalPlan 节点: `TableScan`, `Filter`, `Projection` |
| 1.7 | `python/starrocks/compiler/sql_compiler.py` | SQLCompiler: `compile(plan) → str`，支持 TableScan, Filter, Projection |
| 1.8 | `python/starrocks/result/fetcher.py` | ResultFetcher: `execute_to_pandas()`, `execute_show()`, `execute_explain()` |
| 1.9 | `python/starrocks/types.py` | StarRocks 类型 → Python 类型映射 |
| 1.10 | `python/starrocks/dataframe.py` | DataFrame: `filter()`, `select()`, `where()`, `show()`, `to_pandas()`, `to_sql()`, `explain()`, `count()`, `first()`, `columns`, `schema` |
| 1.11 | `python/starrocks/session.py` | Session: `__init__()`, `table()`, `sql()`, `execute()`, `close()` |
| 1.12 | `python/tests/test_compiler.py` | SQL 编译单元测试（不连接 StarRocks） |
| 1.13 | `python/tests/test_column.py` | Column 表达式单元测试 |
| 1.14 | `python/tests/test_integration.py` | E2E 集成测试 |

---

### Phase 2: 聚合 + 排序 + 分页

#### 2.1 目标与验收标准

用户可以 `df.group_by().agg()`, `df.order_by()`, `df.limit()`, `df.distinct()`。

**验收用例**:

| # | 用例 | SQL 验证 | PASS 条件 |
|---|------|---------|-----------|
| 1 | `group_by("city").agg(func.sum("revenue"))` | 正确 SQL + E2E 结果一致 | GROUP BY + SUM |
| 2 | `group_by("city").agg(func.count("*"), func.avg("price"))` | 多聚合 | COUNT + AVG |
| 3 | `order_by(col("revenue").desc())` | ORDER BY | DESC 排序 |
| 4 | `limit(10)` | LIMIT 10 | 正确行数 |
| 5 | `distinct()` | SELECT DISTINCT | 去重正确 |
| 6 | `func.count_distinct("user_id")` | COUNT(DISTINCT user_id) | 正确 SQL |
| 7 | 链式: `filter → group_by → agg → order_by → limit` | 完整查询 | SQL 正确 + E2E 结果正确 |

```python
#!/usr/bin/env python3
"""verify_phase2.py — Phase 2 验收脚本"""
import os, sys
sys.path.insert(0, "python")
from starrocks import Session, col, func

PASS = FAIL = 0
def check(name, condition):
    global PASS, FAIL
    if condition: print(f"PASS: {name}"); PASS += 1
    else: print(f"FAIL: {name}"); FAIL += 1

session = Session(host=os.getenv("SR_HOST","47.239.57.232"), port=int(os.getenv("SR_PORT","9030")),
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
```

#### 2.2 前置依赖

- Phase 1 完成

#### 2.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 2.1 | `plan/logical.py` | 新增 `Aggregate`, `Sort`, `Limit`, `Distinct` 节点 |
| 2.2 | `compiler/sql_compiler.py` | 新增 Aggregate, Sort, Limit, Distinct 编译 |
| 2.3 | `functions.py` | `func.sum()`, `func.avg()`, `func.count()`, `func.min()`, `func.max()`, `func.count_distinct()` |
| 2.4 | `dataframe.py` | `GroupedDataFrame` 类 + `DataFrame.group_by()`, `order_by()`, `limit()`, `distinct()` |
| 2.5 | `column.py` | `Column.asc()`, `Column.desc()` |
| 2.6 | `tests/test_compiler.py` | 新增聚合/排序 SQL 编译测试 |
| 2.7 | `tests/test_integration.py` | 新增 E2E 测试 |

---

### Phase 3: Join + 子查询 + Union

#### 3.1 目标与验收标准

用户可以 `df.join(df2, on=...)`, `session.sql("...")`, `df.union(df2)`。多表 join 时列名自动消歧。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `df.join(df2, on="key")` inner join | SQL 正确 + E2E 结果正确 |
| 2 | `df.join(df2, on=..., how="left")` | LEFT JOIN SQL |
| 3 | `df.join(df2, col("a.id") == col("b.id"))` 表达式 join | ON 条件正确 |
| 4 | 列名消歧: 两表都有 `id` 列 | 生成 `t1.id`, `t2.id` 限定符 |
| 5 | `session.sql("SELECT ...")` | 返回 DataFrame，可继续链式操作 |
| 6 | `df.union(df2)` | UNION ALL SQL |
| 7 | `df.union_distinct(df2)` | UNION DISTINCT SQL |
| 8 | 三表 join E2E | 正确结果 |

#### 3.2 前置依赖

- Phase 2 完成

#### 3.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 3.1 | `plan/logical.py` | 新增 `Join`, `SetOperation`, `RawSQL`, `SubqueryAlias` 节点 |
| 3.2 | `compiler/sql_compiler.py` | Join 编译（含表别名自动生成 + 列名消歧），Union 编译，子查询编译 |
| 3.3 | `dataframe.py` | `DataFrame.join()`, `union()`, `union_distinct()`, `alias()` |
| 3.4 | `session.py` | `Session.sql()` — RawSQL → DataFrame |
| 3.5 | `tests/test_compiler.py` | Join SQL 编译测试（重点: 列名消歧、多种 join 类型） |
| 3.6 | `tests/test_integration.py` | 多表 join E2E 测试 |

---

### Phase 4: 高级表达式 + 函数库

#### 4.1 目标与验收标准

完整的 `func` 函数库（字符串、日期、数学、条件），Window 函数，`with_column` / `drop` / `rename`。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `func.year(col("dt"))` | `YEAR(dt)` SQL |
| 2 | `func.date_trunc("month", col("dt"))` | `DATE_TRUNC('month', dt)` |
| 3 | `func.concat(col("a"), func.lit(" "), col("b"))` | `CONCAT(a, ' ', b)` |
| 4 | `func.when(col("a") > 1, "yes").otherwise("no")` | `CASE WHEN a > 1 THEN 'yes' ELSE 'no' END` |
| 5 | `func.coalesce(col("a"), func.lit(0))` | `COALESCE(a, 0)` |
| 6 | `func.row_number().over(Window.partition_by("dept").order_by("salary"))` | 窗口函数 SQL |
| 7 | `df.with_column("new_col", col("a") + col("b"))` | SELECT 增加列 |
| 8 | `df.drop("col_to_remove")` | SELECT 减少列 |
| 9 | `df.rename({"old": "new"})` | AS 重命名 |
| 10 | E2E: 日期函数 + 聚合 | 结果与直接 SQL 一致 |
| 11 | E2E: 窗口函数 ranking | 排名正确 |

#### 4.2 前置依赖

- Phase 3 完成

#### 4.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 4.1 | `plan/expr.py` | 新增 `CaseWhen`, `WindowExpr` 节点 |
| 4.2 | `functions.py` | 完整函数库: 字符串(concat/substring/upper/lower/length), 日期(year/month/day/date_trunc/now), 数学(abs/round/ceil/floor), 条件(when/coalesce/if_), 窗口(row_number/rank/dense_rank/lag/lead) |
| 4.3 | `column.py` | `Column.over(window)` 窗口函数支持 |
| 4.4 | `dataframe.py` | `Window` 类, `with_column()`, `drop()`, `rename()`, `with_window()` |
| 4.5 | `compiler/sql_compiler.py` | CaseWhen, Window 表达式编译 |
| 4.6 | `tests/` | 函数库 + 窗口函数 SQL 编译测试 + E2E |

---

### Phase 5: StarRocks 专有特性

#### 5.1 目标与验收标准

全文检索（MATCH_PHRASE/MATCH_ALL/MATCH_ANY）、BM25、Bitmap/HLL 函数、外表查询。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `col("content").match_phrase("query")` | `content MATCH_PHRASE 'query'` SQL |
| 2 | `col("content").match_all("a b")` | `content MATCH_ALL 'a b'` SQL |
| 3 | `col("content").match_any("a b")` | `content MATCH_ANY 'a b'` SQL |
| 4 | `df.with_bm25("content", "query")` | `BM25(content, 'query') AS score` |
| 5 | `func.bitmap_union(col("bm"))` | `BITMAP_UNION(bm)` SQL |
| 6 | `func.approx_count_distinct(col("uid"))` | `APPROX_COUNT_DISTINCT(uid)` SQL |
| 7 | `func.array_agg(col("v"))` | `ARRAY_AGG(v)` SQL |
| 8 | `session.catalog("hive_catalog").table("db.t")` | 正确的 catalog 切换 |
| 9 | E2E: 全文检索（需要 tantivy GIN 索引表） | MATCH 查询返回正确结果 |
| 10 | E2E: 外表查询（需要外表 catalog） | 跨 catalog join 正确 |

#### 5.2 前置依赖

- Phase 4 完成
- E2E TC9: 需要 StarRocks 实例上有 tantivy GIN 索引表
- E2E TC10: 需要配置的外表 catalog（可标记为可选测试）

#### 5.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 5.1 | `column.py` | `Column.match_phrase()`, `match_all()`, `match_any()`, `match_regexp()` |
| 5.2 | `plan/expr.py` | `MatchExpr` 节点 |
| 5.3 | `functions.py` | `func.bitmap_union()`, `func.bitmap_count()`, `func.hll_union()`, `func.approx_count_distinct()`, `func.array_agg()`, `func.bm25()` |
| 5.4 | `dataframe.py` | `DataFrame.with_bm25()` |
| 5.5 | `session.py` | `Session.catalog()` — catalog 切换 |
| 5.6 | `compiler/sql_compiler.py` | MATCH 表达式编译, catalog.db.table 三段名编译 |
| 5.7 | `tests/` | StarRocks 专有功能 SQL 编译测试 + 可选 E2E |

---

### Phase 6: 生产化 + 发布

#### 6.1 目标与验收标准

可通过 `pip install` 安装使用，有完善的错误处理和文档。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `pip install ./python/` 安装成功 | 无错误 |
| 2 | 连接失败 → 友好错误信息 | `ConnectionError` 而非原始 pymysql 异常 |
| 3 | SQL 执行失败 → 友好错误信息 | 包含生成的 SQL + StarRocks 错误信息 |
| 4 | `to_pandas(batch_size=1000)` 分批获取 | 大表不 OOM |
| 5 | 字面量参数化 | `col("a") == "'; DROP TABLE t; --"` 不触发注入 |
| 6 | Jupyter Notebook 示例可运行 | 全部 cell 无错误 |
| 7 | pytest 覆盖率 > 80% | `pytest --cov` 报告 |

#### 6.2 前置依赖

- Phase 5 完成

#### 6.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 6.1 | `connection/mysql.py` | 连接池、重试、超时配置 |
| 6.2 | `starrocks/exceptions.py` | 自定义异常: `ConnectionError`, `QueryError`, `CompilationError` |
| 6.3 | `result/fetcher.py` | `to_pandas(batch_size=N)` 分批获取 |
| 6.4 | `compiler/sql_compiler.py` | 字面量参数化（防 SQL 注入） |
| 6.5 | `pyproject.toml` | 版本号、分类信息、依赖声明 |
| 6.6 | `python/examples/` | Jupyter Notebook 示例（基础分析、多表 join、全文检索） |
| 6.7 | 全部 `tests/` | 补充测试用例，覆盖率 > 80% |

---

### Phase 7: 向量函数映射 ✅

Stage 1 的收尾工作。StarRocks 已内置 `cosine_similarity`、`l2_distance`、`cosine_similarity_norm` 等向量函数，在 `functions.py` 中添加对应的 DataFrame API 映射即可，零服务端改动。

| 步骤 | 文件 | 内容 |
|------|------|------|
| 7.1 | `python/starrocks/functions.py` | `func.cosine_similarity()`, `func.l2_distance()`, `func.cosine_similarity_norm()` |
| 7.2 | `python/tests/test_functions.py` | 向量函数 SQL 编译测试 |

---

## Stage 2: Daft on Ray POC

> **目标**: 验证 §9.3 推荐架构 (StarRocks + Daft on Ray) 的核心管道：StarRocks 查数据 → Daft 做 Python 处理 → 结果写回 StarRocks。
>
> 跳过 Arrow Flight，用 MySQL → pandas → Daft 桥梁（POC 阶段足够）。阶段 3 引入 Arrow Flight 后替换。
>
> **对应 DESIGN §9.4 阶段 2**: Daft on Ray POC
>
> **前置依赖**: Stage 1 完成
>
> **环境要求**: 在 Stage 1 基础上增加：
> - 远程服务器: `pip3.11 install "ray[default]" getdaft`
> - Ray 单节点: `ray start --head`

---

### Phase 8: 环境搭建 + Daft 基础集成

#### 8.1 目标与验收标准

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | Ray + Daft 安装验证 | `import ray; import daft` 成功 |
| 2 | `df.to_daft()` | StarRocks 查询 → Daft DataFrame，行数/列名一致 |
| 3 | `session.write_daft(daft_df, "target")` | Daft → StarRocks 写入，数据一致 |
| 4 | 往返一致性 | StarRocks → Daft → StarRocks，数据完全一致 |

#### 8.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 8.1 | `python/starrocks/dataframe.py` | `to_daft()`: `to_pandas()` → `daft.from_pandas()` |
| 8.2 | `python/starrocks/session.py` | `write_daft()`: Daft → pandas → INSERT INTO VALUES (批次 1000 行) |
| 8.3 | `python/starrocks/daft_utils.py` | `DaftDataFrame` 包装类 (to_pandas, to_starrocks, show, daft) |
| 8.4 | `python/tests/test_daft_integration.py` | Mock unit tests (不连 StarRocks) |

---

### Phase 9: map_batches 端到端管道

#### 9.1 目标与验收标准

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `to_daft()` + Daft UDF + `write_daft()` 全链路 | 数据正确写回 StarRocks |
| 2 | `df.map_batches(func)` 一步语法 | 触发 StarRocks 查询 + Daft 处理 |
| 3 | Daft UDF: 文本长度计算 | `text_len` 列值正确 |
| 4 | Daft UDF: 数值变换 (val * 2) | 结果数值正确 |
| 5 | 写回后 StarRocks 可查询/聚合 | `SELECT SUM(text_len)` 正确 |

#### 9.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 9.1 | `python/starrocks/dataframe.py` | `map_batches(func)`: to_daft → func(daft_df) → DaftDataFrame |
| 9.2 | `python/tests/verify_phase9_daft.py` | E2E 验收: 建表 → to_daft → UDF → write_daft → 验证 |

---

## Stage 3: Arrow Flight 数据通道

> **目标**: 用 Arrow Flight SQL 替代 Stage 2 的 MySQL → pandas 桥梁，实现 StarRocks ↔ Daft 零拷贝列式数据交换。
>
> **对应 DESIGN §9.4 阶段 3**: + Arrow Flight 数据通道
>
> **前置依赖**: Stage 2 完成（POC 验证混合管道可行）
>
> **环境要求**: 在 Stage 2 基础上增加：
> - StarRocks 需支持 Arrow Flight SQL 协议（依赖上游特性就绪）
> - Python 依赖: `pyarrow` (含 Flight 模块)
> - 安全组额外开放 Arrow Flight 端口（默认 8040）

---

### Phase 10: Arrow Flight SQL 客户端

#### 10.1 目标与验收标准

实现 Arrow Flight SQL Python 客户端，可通过 Arrow Flight 协议从 StarRocks 获取查询结果。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | Arrow Flight 连接 StarRocks | `FlightConnection(host, flight_port)` 成功 |
| 2 | `execute_to_arrow(sql)` 返回 Arrow Table | 数据与 MySQL 协议结果一致 |
| 3 | `to_pandas()` 通过 Arrow Flight | 结果与 MySQL 协议的 `to_pandas()` 一致 |
| 4 | 大结果集 (>1M 行) 流式获取 | 内存占用可控，不 OOM |
| 5 | 性能: Arrow Flight vs MySQL 协议 | 大结果集场景 Arrow Flight ≥ 2x 吞吐 |
| 6 | 连接失败 → 友好错误信息 | 包含 Flight 端口和服务状态 |

#### 10.2 前置依赖

- Stage 2 完成（Daft POC 验证混合管道可行）
- StarRocks 实例支持 Arrow Flight SQL 协议

#### 10.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 10.1 | `python/starrocks/connection/flight.py` | Arrow Flight SQL 连接管理（connect, execute, fetch_arrow, close） |
| 10.2 | `python/starrocks/result/fetcher.py` | 新增 `execute_to_arrow()`, Arrow Table → Pandas 转换 |
| 10.3 | `python/starrocks/connection/__init__.py` | 连接工厂: 根据协议类型创建 MySQL 或 Flight 连接 |
| 10.4 | `python/tests/test_flight.py` | Arrow Flight 连接 + 查询单元测试 |
| 10.5 | E2E 验证脚本 | MySQL vs Arrow Flight 结果一致性 + 性能对比 |

---

### Phase 11: 双通道切换 (MySQL / Arrow Flight)

#### 11.1 目标与验收标准

Session 支持 MySQL 和 Arrow Flight 双通道，用户可通过参数选择或自动选择最优通道。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `Session(host, protocol="mysql")` | 使用 MySQL 协议 |
| 2 | `Session(host, protocol="flight")` | 使用 Arrow Flight 协议 |
| 3 | `Session(host, protocol="auto")` | 自动选择（小结果集用 MySQL，大结果集用 Flight） |
| 4 | Flight 不可用时 fallback 到 MySQL | 透明降级，打印 warning |
| 5 | `df.to_arrow()` 新增 API | 直接返回 Arrow Table（仅 Flight 通道） |
| 6 | 所有已有 E2E 测试在 Flight 通道下通过 | 回归无失败 |

#### 11.2 前置依赖

- Phase 10 完成

#### 11.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 11.1 | `python/starrocks/session.py` | `protocol` 参数: "mysql" / "flight" / "auto" |
| 11.2 | `python/starrocks/result/fetcher.py` | 双通道 dispatch: 根据 Session protocol 选择获取方式 |
| 11.3 | `python/starrocks/dataframe.py` | `DataFrame.to_arrow()` — 返回 pyarrow.Table |
| 11.4 | `python/tests/test_integration.py` | 双通道回归测试 |

---

## Stage 4: 完整 Daft 集成 + 自动路由

> **目标**: 实现 §9.3 架构的完整形态——DataFrame API 层根据操作类型自动路由（SQL 操作 → StarRocks，多模态操作 → Daft on Ray），Arrow Flight 做数据桥梁，多模态类型系统。
>
> **对应 DESIGN §9.4 阶段 4**: + 完整 Daft 集成 + 自动路由
>
> **前置依赖**: Stage 3 完成（Arrow Flight 是高效数据交换的前提）
>
> **环境要求**: 在 Stage 3 基础上增加：
> - Ray 集群: `pip install ray[default]`，至少 1 head + 1 worker 节点
> - Daft: `pip install getdaft[ray]`
> - Ray Dashboard 端口: 8265
> - Session 新增 `ray` 参数: `Session("sr:9030", ray="ray://head:10001")`

---

### Phase 12: Ray 集群集成 + Daft 执行引擎

#### 12.1 目标与验收标准

Session 可连接 Ray 集群，DataFrame 可在 Daft 执行引擎上运行基本操作。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `Session("sr:9030", ray="ray://head:10001")` | 同时连接 StarRocks 和 Ray |
| 2 | `df.to_daft()` 将 StarRocks 数据导入 Daft DataFrame | Arrow Flight 传输，数据一致 |
| 3 | Daft DataFrame 基本操作 (filter/select/sort) | 结果正确 |
| 4 | `daft_df.to_starrocks("target_table")` 写回 | 数据正确写入 StarRocks |
| 5 | Ray Dashboard 显示 Daft 任务 | 任务可见且状态正确 |

#### 12.2 前置依赖

- Stage 3 完成（Arrow Flight 数据通道就绪）
- Ray 集群部署就绪

#### 12.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 12.1 | `python/starrocks/connection/ray_cluster.py` | Ray 集群连接管理 |
| 12.2 | `python/starrocks/execution/daft_engine.py` | Daft 执行引擎封装: StarRocks Arrow → Daft DataFrame |
| 12.3 | `python/starrocks/session.py` | Session `ray` 参数, `to_daft()` 方法 |
| 12.4 | `python/starrocks/execution/__init__.py` | 执行引擎工厂 |
| 12.5 | `python/tests/test_daft.py` | Daft 集成测试 |

---

### Phase 13: map_batches / Python UDF

#### 13.1 目标与验收标准

DataFrame 支持 `map_batches()` 方法，用户可传入 Python 函数对数据批量处理，在 Daft on Ray 上执行。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `df.map_batches(my_func, input="col_a", output="col_b")` | Python 函数在 Ray 上执行，结果正确 |
| 2 | `map_batches` + ML 推理 (sentiment analysis) | 模型推理结果正确附加到 DataFrame |
| 3 | 链式: `df.filter(...).map_batches(...).filter(...)` | SQL + Python UDF 混合正确 |
| 4 | 多个 `map_batches` 自动融合 | Daft 算子融合优化生效 |
| 5 | UDF 错误处理 | Python 函数抛异常 → 友好错误信息 |

#### 13.2 前置依赖

- Phase 12 完成

#### 13.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 13.1 | `python/starrocks/dataframe.py` | `DataFrame.map_batches(func, input, output, **kwargs)` |
| 13.2 | `python/starrocks/execution/daft_engine.py` | map_batches → Daft UDF 转换 |
| 13.3 | `python/starrocks/plan/logical.py` | 新增 `MapBatches` 逻辑节点 |
| 13.4 | `python/starrocks/execution/router.py` | 操作路由: 遇到 MapBatches 节点 → 切换到 Daft 引擎 |
| 13.5 | `python/tests/test_map_batches.py` | UDF 测试 (纯函数 + ML 模型推理) |

---

### Phase 14: StarRocks ↔ Daft 数据桥梁

#### 14.1 目标与验收标准

实现 StarRocks 和 Daft 之间的高效数据交换，支持混合查询 pipeline。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | SQL filter → Daft map_batches → SQL agg | 三段 pipeline 正确执行 |
| 2 | 数据交换使用 Arrow Flight (非 CSV/临时表) | 零拷贝传输，性能可接受 |
| 3 | `df.write.to_table("target", mode="append")` | Daft 处理结果写回 StarRocks |
| 4 | 10M 行混合 pipeline E2E | 端到端正确 + 性能可接受 |

#### 14.2 前置依赖

- Phase 13 完成

#### 14.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 14.1 | `python/starrocks/execution/pipeline.py` | 混合 pipeline 构建: 分析逻辑计划 → 切分 SQL 段和 Daft 段 |
| 14.2 | `python/starrocks/execution/bridge.py` | Arrow Flight 数据桥梁: StarRocks → Arrow → Daft, Daft → Arrow → StarRocks |
| 14.3 | `python/starrocks/dataframe.py` | `WriteBuilder` 类: `df.write.to_table()`, `df.write.to_parquet()` |
| 14.4 | `python/tests/test_pipeline.py` | 混合 pipeline 测试 |

---

### Phase 15: 多模态类型系统

#### 15.1 目标与验收标准

DataFrame API 支持多模态类型标注（Image, Tensor, Embedding），与 Daft 的多模态类型系统对齐。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | `df.select(col("image_url").cast(Image))` | 类型标注成功，Daft 可识别 |
| 2 | `df.map_batches(resize_image, input=Image("url"), output=Image("resized"))` | 图像处理正确 |
| 3 | `Embedding` 类型与 `ARRAY<FLOAT>` 自动映射 | StarRocks ARRAY ↔ Daft Embedding 无损转换 |
| 4 | `df.schema` 显示多模态类型 | `image_url: Image`, `embedding: Embedding(384)` |

#### 15.2 前置依赖

- Phase 14 完成

#### 15.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 15.1 | `python/starrocks/types.py` | 多模态类型: `Image`, `Tensor`, `Embedding(dim)`, `Audio`, `Video` |
| 15.2 | `python/starrocks/execution/daft_engine.py` | 类型映射: StarRocks SQL types ↔ Daft multimodal types |
| 15.3 | `python/starrocks/column.py` | `Column.cast(multimodal_type)` |
| 15.4 | `python/tests/test_multimodal.py` | 多模态类型测试 |

---

## Stage 5: 跨引擎查询优化（未来方向）

> **目标**: 跨 StarRocks + Daft 的全局查询优化。DataFrame API 层的智能路由：分析查询 cost model，决定最优执行边界。混合查询（SQL join + ML 推理）的 pipeline 优化。
>
> **前置依赖**: Stage 4 完成
>
> **环境要求**: 同 Stage 4
>
> **注意**: 此 Stage 高度实验性，具体设计待 Stage 4 完成后根据实际经验再详细规划。以下为初步方向。

---

### Phase 16: 操作路由决策器

#### 16.1 目标与验收标准

实现基于 cost model 的操作路由决策器，自动选择最优执行引擎。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | 纯 SQL 操作 → 自动路由到 StarRocks | 无不必要的 Daft 调用 |
| 2 | 含 `map_batches` → 自动路由到 Daft | Python UDF 在 Ray 上执行 |
| 3 | `df.filter(...).map_batches(...).group_by(...).agg(...)` 混合查询 | filter/agg 推给 StarRocks，map_batches 在 Daft 执行 |
| 4 | `df.explain()` 显示执行引擎分配 | 每个操作标注 `[SR]` 或 `[Daft]` |
| 5 | 路由决策可被用户 override | `df.with_engine("starrocks")` 或 `df.with_engine("daft")` |

#### 16.2 前置依赖

- Stage 4 完成

#### 16.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 16.1 | `python/starrocks/execution/cost_model.py` | CostModel: 分析操作类型/数据量/选择性 → 选择执行引擎 |
| 16.2 | `python/starrocks/execution/router.py` | 重构路由: 从规则路由升级为 cost-based 路由 |
| 16.3 | `python/starrocks/dataframe.py` | `DataFrame.with_engine()` — 用户 override 执行引擎 |
| 16.4 | `python/starrocks/dataframe.py` | `DataFrame.explain()` 增强 — 显示引擎分配 |
| 16.5 | `python/tests/test_router.py` | 路由决策测试 |

---

### Phase 17: 跨引擎查询优化

#### 17.1 目标与验收标准

跨 StarRocks + Daft 的全局查询优化，减少数据传输，优化执行 pipeline。

**验收用例**:

| # | 用例 | PASS 条件 |
|---|------|-----------|
| 1 | Predicate pushdown 到 StarRocks | Daft 段的 filter 条件自动下推到 SQL 段 |
| 2 | Projection pruning 跨引擎 | 只传输下游需要的列 |
| 3 | 多个 Daft 段合并 | 连续的 map_batches 融合为一次数据传输 |
| 4 | 混合 pipeline 性能优化 | 相比 Stage 4 无优化版本，减少 ≥ 30% 数据传输量 |

#### 17.2 前置依赖

- Phase 16 完成

#### 17.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 17.1 | `python/starrocks/execution/optimizer.py` | 跨引擎优化器: predicate pushdown, projection pruning, segment merging |
| 17.2 | `python/starrocks/execution/pipeline.py` | Pipeline 重构: 支持优化 pass |
| 17.3 | `python/starrocks/execution/stats.py` | 执行统计收集: 数据量、传输时间、引擎耗时 |
| 17.4 | `python/tests/test_optimizer.py` | 优化器测试: 验证优化规则正确性 + 性能基准 |
