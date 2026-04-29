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

### 0.2 分支 & Git

```bash
# 本地创建分支（已完成）
git checkout -b fanzhen/main-stella-dataframe origin/main
```

### 0.3 远程服务器 & StarRocks 部署

本项目是纯 Python 客户端，不需要编译 FE/BE，但 E2E 测试需要一个运行中的 StarRocks 实例。使用 **StarRocks allin1 Docker 镜像**快速部署。

**当前服务器**: `47.239.57.232`

#### 0.3.1 服务器从零搭建（换服务器时参照此节）

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

#### 0.3.2 关键注意事项

| 问题 | 原因 | 解决 |
|------|------|------|
| `FE saved address not match` | 容器使用 bridge 网络，hostname 解析到 10.88.x.x | 必须 `--network=host` |
| BE heartbeat 端口 | allin1 镜像已内置 BE，无需手动 ADD BACKEND | 验证 `SHOW BACKENDS` Alive=true 即可 |
| 容器重启后数据丢失 | allin1 镜像数据在容器内 | 生产环境需挂载 volume，测试环境可接受 |

#### 0.3.3 本地连接方式

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

#### 0.3.4 容器管理命令

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

### 0.4 本地 Python 开发环境

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

### 0.5 项目初始化

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

### 0.6 验证流程（每个 Phase 通用）

```
1. 本地: 编写代码 + pytest 单元测试
2. 本地: pytest python/tests/ 全部通过
3. 本地: python3 python/tests/test_integration.py（连接远程 StarRocks E2E）
4. 本地: git add <files> && git commit -m "[Feature] ..."
5. E2E 验收脚本全部 PASS → Phase complete
```

### 0.7 StarRocks 测试环境

E2E 测试连接远程 StarRocks。测试配置通过环境变量：

```bash
export SR_HOST=47.239.57.232
export SR_PORT=9030
export SR_USER=root
export SR_PASSWORD=
export SR_DATABASE=test_dataframe
```

---

## Phase 1: 核心框架 + 基础查询

### 1.1 目标与验收标准

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

### 1.2 代码任务

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

## Phase 2: 聚合 + 排序 + 分页

### 2.1 目标与验收标准

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

### 2.2 前置依赖

- Phase 1 完成

### 2.3 代码任务

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

## Phase 3: Join + 子查询 + Union

### 3.1 目标与验收标准

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

### 3.2 前置依赖

- Phase 2 完成

### 3.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 3.1 | `plan/logical.py` | 新增 `Join`, `SetOperation`, `RawSQL`, `SubqueryAlias` 节点 |
| 3.2 | `compiler/sql_compiler.py` | Join 编译（含表别名自动生成 + 列名消歧），Union 编译，子查询编译 |
| 3.3 | `dataframe.py` | `DataFrame.join()`, `union()`, `union_distinct()`, `alias()` |
| 3.4 | `session.py` | `Session.sql()` — RawSQL → DataFrame |
| 3.5 | `tests/test_compiler.py` | Join SQL 编译测试（重点: 列名消歧、多种 join 类型） |
| 3.6 | `tests/test_integration.py` | 多表 join E2E 测试 |

---

## Phase 4: 高级表达式 + 函数库

### 4.1 目标与验收标准

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

### 4.2 前置依赖

- Phase 3 完成

### 4.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 4.1 | `plan/expr.py` | 新增 `CaseWhen`, `WindowExpr` 节点 |
| 4.2 | `functions.py` | 完整函数库: 字符串(concat/substring/upper/lower/length), 日期(year/month/day/date_trunc/now), 数学(abs/round/ceil/floor), 条件(when/coalesce/if_), 窗口(row_number/rank/dense_rank/lag/lead) |
| 4.3 | `column.py` | `Column.over(window)` 窗口函数支持 |
| 4.4 | `dataframe.py` | `Window` 类, `with_column()`, `drop()`, `rename()`, `with_window()` |
| 4.5 | `compiler/sql_compiler.py` | CaseWhen, Window 表达式编译 |
| 4.6 | `tests/` | 函数库 + 窗口函数 SQL 编译测试 + E2E |

---

## Phase 5: StarRocks 专有特性

### 5.1 目标与验收标准

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

### 5.2 前置依赖

- Phase 4 完成
- E2E TC9: 需要 StarRocks 实例上有 tantivy GIN 索引表
- E2E TC10: 需要配置的外表 catalog（可标记为可选测试）

### 5.3 代码任务

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

## Phase 6: 生产化 + 发布

### 6.1 目标与验收标准

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

### 6.2 前置依赖

- Phase 5 完成

### 6.3 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 6.1 | `connection/mysql.py` | 连接池、重试、超时配置 |
| 6.2 | `starrocks/exceptions.py` | 自定义异常: `ConnectionError`, `QueryError`, `CompilationError` |
| 6.3 | `result/fetcher.py` | `to_pandas(batch_size=N)` 分批获取 |
| 6.4 | `compiler/sql_compiler.py` | 字面量参数化（防 SQL 注入） |
| 6.5 | `pyproject.toml` | 版本号、分类信息、依赖声明 |
| 6.6 | `python/examples/` | Jupyter Notebook 示例（基础分析、多表 join、全文检索） |
| 6.7 | 全部 `tests/` | 补充测试用例，覆盖率 > 80% |
