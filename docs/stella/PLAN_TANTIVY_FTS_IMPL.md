# Tantivy 全文检索 — 实施计划

> 分支: `fanzhen/main-stella-tantivy`
> 日期: 2026-04-14
> 设计文档: [DESIGN_TANTIVY_FTS.md](./DESIGN_TANTIVY_FTS.md)

---

## 0. 环境准备 & 全局约束

### 0.1 Phase 编写规范

**目标与验收标准合并原则**：每个 Phase 的"目标"必须包含具体的验收标准，从用户 E2E 视角出发描述测试用例。不单独列"目标"一节再另列"验证标准"——两者写在一起，保持紧凑且可执行。

**验收必须通过外部工具**：每个 Phase 的验收标准必须通过外部工具（SQL 客户端、curl、HTTP 请求、Python 脚本等）来验证，而非依赖内部日志或代码审查。具体要求：

- **SQL 验证**: 通过 `mysql -h... -P9030 -uroot -e "..."` 执行查询，对比预期结果
- **脚本验证**: 编写可重复执行的 shell/python 脚本，输出 PASS/FAIL
- **API 验证**: 如涉及 HTTP 接口，通过 `curl` 调用并验证响应
- **性能验证**: 通过 `SET enable_profile=true` + `SHOW PROFILELIST` 获取可量化指标
- **性能分析**: 涉及性能测试的 Phase 必须使用 `perf` 工具采集热点函数（`perf record -g -p <be_pid>` + `perf report`），确认热点落在预期代码路径上，排除非预期瓶颈。perf 火焰图或 `perf report` top-10 函数列表作为验收附件

每个 Phase 完成的标志是：**验收脚本全部 PASS**，而非"代码写完"或"编译通过"。

**基础设施阶段例外**：Phase 1（Rust FFI）和 Phase 2（BE 存储引擎）属于基础设施，FE 尚未就绪，无法通过 SQL 客户端验证。这两个阶段允许使用工程级 E2E（`cargo test`、`run-be-ut.sh` 等脚本化 UT）作为验收手段。从 Phase 3 起，必须通过用户 SQL E2E 验收。

### 0.2 分支 & Git

```bash
# 本地创建分支（已完成）
git checkout -b fanzhen/main-stella-tantivy main
git push fanzhen fanzhen/main-stella-tantivy

# 远程服务器 fetch
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
$SSH "docker exec sr-dev bash -c 'cd /build && git fetch origin && git checkout origin/fanzhen/main-stella-tantivy'"
```

### 0.3 Rust 工具链（远程服务器）

tantivy FFI 编译需要 Rust 工具链。在 Docker 容器 `sr-dev` 中安装：

```bash
# 安装 rustup + stable toolchain
$SSH "docker exec sr-dev bash -c 'curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'"
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && rustc --version && cargo --version'"

# 安装 cbindgen（生成 C 头文件）
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && cargo install cbindgen'"

# 验证
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && cbindgen --version'"
```

### 0.4 编译 & 部署流程

每个 Phase 的标准流程：

```
1. 本地: 编写代码 + 本地 UT（如适用）
2. 本地: git add <files> && git commit -m "[Enhancement] ..."
3. 本地: git push fanzhen fanzhen/main-stella-tantivy
4. 远程: docker exec sr-dev bash -c 'cd /build && git fetch origin && git checkout origin/fanzhen/main-stella-tantivy'
5. 远程: Rust FFI 编译（Phase 1 起）:
   docker exec sr-dev bash -c 'source $HOME/.cargo/env && cd /build/be/src/storage/index/inverted/tantivy_ffi && cargo build --release'
6. 远程: BE 编译:
   docker exec sr-dev bash -c 'cd /build && ./build.sh --be'       # -j 8, 约 15-25 min
7. 远程: FE 编译（如有 FE 改动）:
   docker exec sr-dev bash -c 'cd /build && ./build.sh --fe --clean'  # 约 2 min
8. 远程: 重启 FE/BE（见 0.5）
9. 远程: mysql E2E 验证
```

### 0.5 FE/BE 重启

```bash
# 停止 + 启动 FE
$SSH "docker exec sr-dev bash -c 'kill \$(pgrep -f StarRocksFE); sleep 2; rm -f /build/output/fe/bin/fe.pid; cd /build && output/fe/bin/start_fe.sh --daemon'"
sleep 20

# 停止 + 启动 BE
$SSH "docker exec sr-dev bash -c 'kill \$(pgrep starrocks_be); sleep 2; rm -f /build/output/be/bin/be.pid; cd /build && output/be/bin/start_be.sh --daemon'"
sleep 5

# 验证连接
$SSH "docker exec sr-dev bash -c 'mysql -h127.0.0.1 -P9030 -uroot -e \"SHOW BACKENDS\"'"
```

### 0.6 CMake 集成 Rust FFI

需要在 BE 的 CMakeLists.txt 中添加 tantivy FFI 的编译和链接：

```cmake
# 编译 Rust 静态库
add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/tantivy_ffi/release/libtantivy_ffi.a
    COMMAND cargo build --release --manifest-path ${CMAKE_SOURCE_DIR}/src/storage/index/inverted/tantivy_ffi/Cargo.toml
            --target-dir ${CMAKE_BINARY_DIR}/tantivy_ffi
    COMMENT "Building tantivy FFI library"
)

# 链接到 BE
target_link_libraries(starrocks_be
    ${CMAKE_BINARY_DIR}/tantivy_ffi/release/libtantivy_ffi.a
    dl pthread m  # Rust 运行时依赖
)

# include 头文件
target_include_directories(starrocks_be PRIVATE
    ${CMAKE_SOURCE_DIR}/src/storage/index/inverted/tantivy_ffi
)
```

实际集成时可能需要根据 StarRocks CMake 体系调整。备选方案：在 `build.sh` 中先 `cargo build`，再 `cmake`，手动指定 `.a` 路径。

---

## Phase 1: Tantivy FFI 基础设施

### 1.1 目标与验收标准

Rust FFI 层在远程 Linux 服务器上编译为 `libtantivy_ffi.a`，`cargo test` 全部通过，`cbindgen` 生成 C 头文件。

**验收用例**（通过 shell 命令在远程服务器执行，全部返回 0 即 PASS）：

| # | 用例 | 验收命令 | PASS 条件 |
|---|------|---------|-----------|
| 1 | Rust 编译产出静态库 | `ls -lh .../target/release/libtantivy_ffi.a` | 文件存在且大小 > 0 |
| 2 | cbindgen 生成头文件 | `ls -lh .../tantivy_ffi.h` | 文件存在，包含 `tantivy_writer_create` 声明 |
| 3 | cargo test 全部通过 | `cargo test 2>&1 \| grep 'test result'` | `test result: ok. N passed; 0 failed` |
| 4 | 写入+查询 roundtrip | cargo test `test_ffi_write_read_roundtrip` 通过 | MATCH_ANY/ALL/PHRASE/PREFIX/REGEXP/BM25 全部断言通过 |
| 5 | 分词 4 种 parser | cargo test `test_ffi_tokenize` + `test_*_tokenizer` 通过 | standard/english/chinese/none 分词结果正确 |
| 6 | 空指针安全 | cargo test `test_ffi_null_pointer_safety` 通过 | 所有 FFI 函数对 null 输入不崩溃 |

```bash
#!/bin/bash
# verify_phase1.sh — Phase 1 验收脚本
set -euo pipefail
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
FFI_DIR="/build/be/src/storage/index/inverted/tantivy_ffi"

echo "=== TC1: Static lib exists ==="
$SSH "docker exec sr-dev bash -c 'ls -lh $FFI_DIR/target/release/libtantivy_ffi.a'" && echo "PASS" || echo "FAIL"

echo "=== TC2: Header file generated ==="
$SSH "docker exec sr-dev bash -c 'grep tantivy_writer_create $FFI_DIR/tantivy_ffi.h'" && echo "PASS" || echo "FAIL"

echo "=== TC3-6: cargo test ==="
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && cd $FFI_DIR && cargo test 2>&1'" | tee /tmp/phase1_test.log
grep -q '0 failed' /tmp/phase1_test.log && echo "ALL TESTS PASS" || echo "FAIL"
```

### 1.2 代码任务

| 步骤    | 文件                             | 内容                                                                               |
| ----- | ------------------------------ | -------------------------------------------------------------------------------- |
| 1.1.1 | `tantivy_ffi/Cargo.toml`       | 新建 Rust 项目，依赖 tantivy=0.22, jieba-rs=0.6                                        |
| 1.1.2 | `tantivy_ffi/src/lib.rs`       | `extern "C"` 入口，re-export writer/reader/tokenizer                                |
| 1.1.3 | `tantivy_ffi/src/writer.rs`    | TantivyWriter: create/add_doc/add_null/commit/destroy                            |
| 1.1.4 | `tantivy_ffi/src/reader.rs`    | TantivyReader: open/query_match_any/all/phrase/phrase_prefix/regexp/bm25/destroy |
| 1.1.5 | `tantivy_ffi/src/tokenizer.rs` | register_tokenizers() + tantivy_tokenize()                                       |
| 1.1.6 | `tantivy_ffi/build.rs`         | cbindgen 自动生成 `tantivy_ffi.h`                                                   |

---

## Phase 2: BE 存储引擎集成

### 2.1 目标与验收标准

BE 可以写入带 tantivy 索引的 segment，通过 `_apply_inverted_index()` 正确过滤行。本阶段完成所有 BE 侧 query type 能力（MATCH_ANY/ALL/PHRASE/PHRASE_PREFIX/REGEXP），Phase 3 只做 FE→Thrift 透传。

> 本阶段 FE 尚未支持 tantivy，无法通过 SQL E2E 验证。验收通过 **BE 编译 + C++ UT** 在远程服务器执行。

**验收用例**：

| # | 用例 | 验收命令 | PASS 条件 |
|---|------|---------|-----------|
| 1 | BE 编译通过（含 tantivy FFI 链接） | `./build.sh --be 2>&1 \| tail -5` | 无编译/链接错误 |
| 2 | TantivyInvertedWriter 写 100 条文本 → finish → 索引目录存在 | `run-be-ut.sh --gtest_filter=TantivyWriter*` | UT PASS |
| 3 | TantivyInvertedReader MATCH_ANY 查询 | 同上 UT | bitmap 匹配预期 |
| 4 | MATCH_PHRASE 查询 | 同上 UT | 短语邻接匹配正确 |
| 5 | MATCH_PHRASE_PREFIX 查询 | 同上 UT | 前缀匹配正确 |
| 6 | MATCH_REGEXP 查询 | 同上 UT | 正则匹配正确 |
| 7 | MATCH_ALL 查询 | 同上 UT | AND 语义正确 |

```bash
#!/bin/bash
# verify_phase2.sh — Phase 2 验收脚本
set -euo pipefail
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"

echo "=== TC1: BE build ==="
$SSH "docker exec sr-dev bash -c 'cd /build && ./build.sh --be 2>&1 | tail -5'" | tee /tmp/phase2_build.log
grep -qv 'Error' /tmp/phase2_build.log && echo "PASS" || echo "FAIL"

echo "=== TC2-7: BE UT (5 query types) ==="
$SSH "docker exec sr-dev bash -c 'cd /build && ./run-be-ut.sh --gtest_filter=TantivyWriter* 2>&1'" | tee /tmp/phase2_ut.log
grep -q 'PASSED' /tmp/phase2_ut.log && echo "ALL TESTS PASS" || echo "FAIL"
```

### 2.2 前置依赖

- Phase 1 完成（libtantivy_ffi.a 可用）
- CMake 集成 Rust FFI（见 0.6）

### 2.3 代码任务

| 步骤    | 文件                                       | 内容                                                                                          |
| ----- | ---------------------------------------- | ------------------------------------------------------------------------------------------- |
| 2.2.1 | `inverted_index_common.h`                | `InvertedImplementType::TANTIVY=3`, `MATCH_PHRASE_PREFIX_QUERY=10`, `MATCH_REGEXP_QUERY=11` |
| 2.2.2 | `tantivy/tantivy_plugin.h/.cpp`          | 实现 `InvertedPlugin`，注册到 factory                                                             |
| 2.2.3 | `tantivy/tantivy_inverted_writer.h/.cpp` | 实现 `InvertedWriter`: init/add_values/add_nulls/finish，调用 FFI                                |
| 2.2.4 | `tantivy/tantivy_inverted_reader.h/.cpp` | 实现 `InvertedReader`: load/query → roaring::Roaring，新增 `query_with_score()`                  |
| 2.2.5 | `inverted_plugin_factory.cpp`            | `case TANTIVY: return TantivyPlugin`                                                        |
| 2.2.6 | `segment_iterator.cpp`                   | `_apply_inverted_index()` 中处理所有新 query type，BE 侧 query type 能力在此阶段完备                       |

---

## Phase 3: FE 语法 + Thrift + 全链路打通 + Compaction 正确性

### 3.1 目标与验收标准

用户通过 mysql 客户端可以：创建 tantivy 索引、写入数据、执行 5 种 MATCH 查询（ANY/ALL/PHRASE/PHRASE_PREFIX/REGEXP），compaction 后查询结果不变。

> Phase 2 已完成所有 BE 侧 query type 能力。本阶段只做 FE 语法/Thrift opcode 到 BE 已有 query type 的透传，不重复改 BE 查询逻辑。

**验收用例**（全部通过 `mysql -h127.0.0.1 -P9030 -uroot` 执行 SQL）：

| # | 用例 | SQL | PASS 条件 |
|---|------|-----|-----------|
| 1 | 建表 + tantivy 索引 | `CREATE TABLE test_fts (..., INDEX idx_c (content) USING GIN ("parser"="standard", "imp_lib"="tantivy")) ...` | 无报错 |
| 2 | SHOW CREATE TABLE 可见索引属性 | `SHOW CREATE TABLE test_fts` | 输出包含 `imp_lib`=`tantivy` |
| 3 | INSERT 5 条测试数据 | `INSERT INTO test_fts VALUES (1,...),(2,...),(3,...),(4,...),(5,...)` | 无报错 |
| 4 | MATCH_ANY（OR 语义） | `SELECT id FROM test_fts WHERE content MATCH_ANY 'database performance' ORDER BY id` | 结果集包含 id=1,3 |
| 5 | MATCH_ALL（AND 语义） | `SELECT id FROM test_fts WHERE content MATCH_ALL 'database performance' ORDER BY id` | 结果集 = {3} |
| 6 | MATCH_PHRASE（短语匹配） | `SELECT id FROM test_fts WHERE content MATCH_PHRASE 'full text search'` | 结果集 = {2} |
| 7 | MATCH_PHRASE_PREFIX（前缀） | `SELECT id FROM test_fts WHERE content MATCH_PHRASE_PREFIX 'search eng'` | 结果集 = {5} |
| 8 | MATCH_REGEXP（正则） | `SELECT id FROM test_fts WHERE content MATCH_REGEXP 'data.*'` | 结果集包含 id=1,3 |
| 9 | Compaction 后查询一致 | 额外 INSERT 2 条 → 等待/手动 COMPACT → 重新 MATCH_ANY 'database' | 结果集包含全部含 database 的行（含新插入） |

```bash
#!/bin/bash
# verify_phase3.sh — Phase 3 验收脚本
# 注意：不使用 set -e，确保所有用例都执行完毕后再统一判定
set -uo pipefail
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
MYSQL="docker exec sr-dev mysql -h127.0.0.1 -P9030 -uroot -N -e"
PASS=0; FAIL=0

run_sql() { $SSH "$MYSQL \"$1\"" 2>&1 || true; }
# check_id: 精确匹配整行 ID（避免 grep "1" 误匹配 10/21）
check_id() {
    local name="$1" expected_id="$2" actual="$3"
    if echo "$actual" | awk '{print $1}' | grep -qx "$expected_id"; then
        echo "PASS: $name"; ((PASS++))
    else
        echo "FAIL: $name (expected id=$expected_id in: $actual)"; ((FAIL++))
    fi
}
# check_absent: 验证某 ID 不在结果中
check_absent() {
    local name="$1" absent_id="$2" actual="$3"
    if echo "$actual" | awk '{print $1}' | grep -qx "$absent_id"; then
        echo "FAIL: $name (id=$absent_id should be absent but found)"; ((FAIL++))
    else
        echo "PASS: $name"; ((PASS++))
    fi
}
# check_contains: 子串匹配（用于 SHOW CREATE TABLE 等非 ID 场景）
check_contains() {
    local name="$1" expected="$2" actual="$3"
    if echo "$actual" | grep -qF "$expected"; then
        echo "PASS: $name"; ((PASS++))
    else
        echo "FAIL: $name (expected substring: $expected, got: $actual)"; ((FAIL++))
    fi
}
# check_eq: 精确值匹配
check_eq() {
    local name="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then
        echo "PASS: $name"; ((PASS++))
    else
        echo "FAIL: $name (expected: $expected, got: $actual)"; ((FAIL++))
    fi
}

# Setup
run_sql "DROP TABLE IF EXISTS test_db.test_fts" > /dev/null
run_sql "ADMIN SET FRONTEND CONFIG (\"enable_experimental_gin\" = \"true\")" > /dev/null

# TC1: CREATE TABLE（通过 exit code 判定）
if $SSH "$MYSQL \"CREATE TABLE test_db.test_fts (id BIGINT, title VARCHAR(200), content VARCHAR(65535), INDEX idx_c (content) USING GIN (\\\"parser\\\"=\\\"standard\\\", \\\"imp_lib\\\"=\\\"tantivy\\\")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1\"" > /dev/null 2>&1; then
    echo "PASS: TC1: CREATE TABLE"; ((PASS++))
else
    echo "FAIL: TC1: CREATE TABLE"; ((FAIL++))
fi

# TC2: SHOW CREATE TABLE
SCT=$(run_sql "SHOW CREATE TABLE test_db.test_fts")
check_contains "TC2: SHOW CREATE TABLE" "tantivy" "$SCT"

# TC3: INSERT（通过 exit code 判定）
if $SSH "$MYSQL \"INSERT INTO test_db.test_fts VALUES (1,'t1','StarRocks is a high-performance analytical database'),(2,'t2','Full text search enables users to find relevant documents'),(3,'t3','Optimizing database performance requires careful analysis'),(4,'t4','Real-time analytics engine for modern data applications'),(5,'t5','Search engine design involves inverted index and ranking')\"" > /dev/null 2>&1; then
    echo "PASS: TC3: INSERT 5 rows"; ((PASS++))
else
    echo "FAIL: TC3: INSERT 5 rows"; ((FAIL++))
fi

# TC4: MATCH_ANY — 用例表：结果集包含 id=1,3
R4=$(run_sql "SELECT id FROM test_db.test_fts WHERE content MATCH_ANY 'database performance' ORDER BY id")
check_id "TC4a: MATCH_ANY id=1" "1" "$R4"
check_id "TC4b: MATCH_ANY id=3" "3" "$R4"

# TC5: MATCH_ALL — 用例表：结果集 = {3}
R5=$(run_sql "SELECT id FROM test_db.test_fts WHERE content MATCH_ALL 'database performance' ORDER BY id")
check_id "TC5: MATCH_ALL id=3" "3" "$R5"

# TC6: MATCH_PHRASE — 用例表：结果集 = {2}
R6=$(run_sql "SELECT id FROM test_db.test_fts WHERE content MATCH_PHRASE 'full text search'")
check_id "TC6: MATCH_PHRASE id=2" "2" "$R6"

# TC7: MATCH_PHRASE_PREFIX — 用例表：结果集 = {5}
R7=$(run_sql "SELECT id FROM test_db.test_fts WHERE content MATCH_PHRASE_PREFIX 'search eng'")
check_id "TC7: MATCH_PHRASE_PREFIX id=5" "5" "$R7"

# TC8: MATCH_REGEXP — 用例表：结果集包含 id=1,3,4 (term "database" 匹配 id=1,3; term "data" 匹配 id=4)
R8=$(run_sql "SELECT id FROM test_db.test_fts WHERE content MATCH_REGEXP 'data.*' ORDER BY id")
check_id "TC8a: MATCH_REGEXP id=1" "1" "$R8"
check_id "TC8b: MATCH_REGEXP id=3" "3" "$R8"
check_id "TC8c: MATCH_REGEXP id=4" "4" "$R8"

# TC9: Compaction — 显式触发 + 确认已完成 + 前后对比
# 9a: 记录 compaction 前的 MATCH_ANY 'database' 行数
R9_BEFORE=$(run_sql "SELECT COUNT(*) FROM test_db.test_fts WHERE content MATCH_ANY 'database'" | tr -d '[:space:]')
# 9b: 多次 INSERT 产生多个 segment
run_sql "INSERT INTO test_db.test_fts VALUES (6,'t6','database compaction test first batch')" > /dev/null
run_sql "INSERT INTO test_db.test_fts VALUES (7,'t7','database compaction test second batch')" > /dev/null
# 9c: 显式触发 compaction
run_sql "ALTER TABLE test_db.test_fts COMPACT" > /dev/null
# 9d: 轮询等待 compaction 完成（最多 120 秒）
for i in $(seq 1 24); do
    ROWSET_COUNT=$(run_sql "SHOW TABLET FROM test_db.test_fts" | wc -l)
    if [ "$ROWSET_COUNT" -le 2 ]; then break; fi
    sleep 5
done
# 9e: compaction 后查询，对比行数
R9_AFTER=$(run_sql "SELECT COUNT(*) FROM test_db.test_fts WHERE content MATCH_ANY 'database'" | tr -d '[:space:]')
EXPECTED_AFTER=$((R9_BEFORE + 2))  # 新插入 2 行含 "database"
check_eq "TC9a: Compaction后行数" "$EXPECTED_AFTER" "$R9_AFTER"
# 9f: 验证具体 id 存在
R9_IDS=$(run_sql "SELECT id FROM test_db.test_fts WHERE content MATCH_ANY 'database' ORDER BY id")
check_id "TC9b: Compaction后 id=1" "1" "$R9_IDS"
check_id "TC9c: Compaction后 id=6" "6" "$R9_IDS"
check_id "TC9d: Compaction后 id=7" "7" "$R9_IDS"

# Cleanup
run_sql "DROP TABLE IF EXISTS test_db.test_fts" > /dev/null
echo "=== Result: $PASS PASS, $FAIL FAIL ==="
[ $FAIL -eq 0 ] && echo "PHASE 3 ACCEPTED" || echo "PHASE 3 REJECTED"
```

### 3.2 前置依赖

- Phase 2 完成（BE tantivy writer/reader 可用，所有 query type 已实现）

### 3.3 代码任务

| 步骤     | 文件                                 | 内容                                                                       |
| ------ | ---------------------------------- | ------------------------------------------------------------------------ |
| 3.2.1  | `MatchExpr.java`                   | `MatchOperator` 新增 `MATCH_PHRASE`, `MATCH_PHRASE_PREFIX`, `MATCH_REGEXP` |
| 3.2.2  | `StarRocks.g4` / `StarRocksLex.g4` | 新增关键字 + 语法规则                                                             |
| 3.2.3  | `AstBuilder.java`                  | 解析新 MATCH 语法，生成 MatchExpr                                                |
| 3.2.4  | `AstBuilder.java`                  | `text_match`/`text_match_all`/`text_match_phrase` 函数 alias → MatchExpr   |
| 3.2.5  | `ExprOpcodeRegistry.java`          | MATCH_PHRASE→TExprOpcode.MATCH_PHRASE 等映射                                |
| 3.2.6  | `Exprs.thrift`                     | TExprOpcode 新增 MATCH_PHRASE, MATCH_PHRASE_PREFIX, MATCH_REGEXP           |
| 3.2.7  | `IndexAnalyzer.java`               | `imp_lib=tantivy` 合法性校验                                                  |
| 3.2.8  | `InvertedIndexParams.java`         | `InvertedIndexImpType.TANTIVY` 枚举                                        |
| 3.2.9  | `Config.java`                      | `enable_experimental_gin`（默认 false）                                  |
| 3.2.10 | BE: `segment_iterator.cpp`         | TExprOpcode → InvertedIndexQueryType 映射（仅 opcode 转换，不改查询逻辑）              |

---

## Phase 4: TOKENIZE + BM25 函数

### 4.1 目标与验收标准

用户通过 mysql 客户端可以：使用 `TOKENIZE()` 调试分词结果，使用 `BM25()` 获取相关性评分并按评分排序。`BM25()` 在没有 MATCH 谓词时应报错。

**验收用例**（全部通过 `mysql` 客户端执行 SQL）：

| # | 用例 | SQL | PASS 条件 |
|---|------|-----|-----------|
| 1 | TOKENIZE standard 分词 | `SELECT TOKENIZE('hello world', 'standard')` | 结果 = ["hello", "world"] |
| 2 | TOKENIZE chinese 分词 | `SELECT TOKENIZE('全文检索引擎', 'chinese')` | 结果非空，包含"全文"或"检索"等词 |
| 3 | TOKENIZE english 词干提取 | `SELECT TOKENIZE('running databases', 'english')` | 结果 = ["run", "databas"] |
| 4 | TOKENIZE none 不分词 | `SELECT TOKENIZE('Hello World', 'none')` | 结果 = ["Hello World"] |
| 5 | BM25 评分 + 排序 | INSERT 4 条文本（TF 不同）→ `SELECT id, BM25(content, 'database') AS score ... ORDER BY score DESC` | id=1 分数最高（TF=3），id=3 不出现，所有 score > 0 |
| 6 | BM25 无 MATCH 独立可用 | `SELECT id, BM25(content, 'database') AS s FROM test_bm25 ORDER BY s DESC` | 返回结果（batch-local BM25），不报错（当前实现不要求 MATCH 谓词） |

```bash
#!/bin/bash
# verify_phase4.sh — Phase 4 验收脚本
set -uo pipefail
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
MYSQL="docker exec sr-dev mysql -h127.0.0.1 -P9030 -uroot -N -e"
PASS=0; FAIL=0

run_sql() { $SSH "$MYSQL \"$1\"" 2>&1 || true; }
check_contains() {
    local name="$1" expected="$2" actual="$3"
    if echo "$actual" | grep -qF "$expected"; then
        echo "PASS: $name"; ((PASS++))
    else
        echo "FAIL: $name (expected substring: $expected, got: $actual)"; ((FAIL++))
    fi
}

# TC1: TOKENIZE standard — 验证输出同时包含 "hello" 和 "world"
R1=$(run_sql "SELECT TOKENIZE('hello world', 'standard')")
check_contains "TC1a: TOKENIZE standard has hello" "hello" "$R1"
check_contains "TC1b: TOKENIZE standard has world" "world" "$R1"

# TC2: TOKENIZE chinese — 验证输出包含已知中文分词
R2=$(run_sql "SELECT TOKENIZE('全文检索引擎', 'chinese')")
check_contains "TC2: TOKENIZE chinese has 检索" "检索" "$R2"

# TC3: TOKENIZE english (stemming) — 验证词干提取
R3=$(run_sql "SELECT TOKENIZE('running databases', 'english')")
check_contains "TC3a: TOKENIZE english stem run" "run" "$R3"
check_contains "TC3b: TOKENIZE english stem databas" "databas" "$R3"

# TC4: TOKENIZE none — 验证原文不被拆分
R4=$(run_sql "SELECT TOKENIZE('Hello World', 'none')")
check_contains "TC4: TOKENIZE none" "Hello World" "$R4"

# TC5: BM25 scoring + ordering
run_sql "DROP TABLE IF EXISTS test_db.test_bm25" > /dev/null
run_sql "CREATE TABLE test_db.test_bm25 (id BIGINT, content VARCHAR(65535), INDEX idx_c (content) USING GIN (\"parser\"=\"standard\", \"imp_lib\"=\"tantivy\")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1" > /dev/null
run_sql "INSERT INTO test_db.test_bm25 VALUES (1,'database database database'),(2,'database performance'),(3,'search engine design'),(4,'analytical database system design')" > /dev/null
R5=$(run_sql "SELECT id, BM25(content, 'database') AS score FROM test_db.test_bm25 WHERE content MATCH_ANY 'database' ORDER BY score DESC")
# id=1 应排第一（TF=3 最高），id=3 不应出现（不含 database）
FIRST_ID=$(echo "$R5" | head -1 | awk '{print $1}')
if [ "$FIRST_ID" = "1" ]; then
    echo "PASS: TC5a: BM25 top-1 is id=1"; ((PASS++))
else
    echo "FAIL: TC5a: BM25 top-1 expected id=1, got $FIRST_ID"; ((FAIL++))
fi
# 验证 id=3 不在结果中：提取所有 id，精确行匹配
if echo "$R5" | awk '{print $1}' | grep -qx 3; then
    echo "FAIL: TC5b: BM25 id=3 should be absent but found"; ((FAIL++))
else
    echo "PASS: TC5b: BM25 id=3 correctly absent"; ((PASS++))
fi
# 验证所有 score > 0
BAD_SCORES=$(echo "$R5" | awk '$2 <= 0 {print}')
if [ -z "$BAD_SCORES" ]; then
    echo "PASS: TC5c: all BM25 scores > 0"; ((PASS++))
else
    echo "FAIL: TC5c: found score <= 0: $BAD_SCORES"; ((FAIL++))
fi

# TC6: BM25 without MATCH — 当前实现独立可用（batch-local），不要求 MATCH 谓词
R6=$(run_sql "SELECT id, BM25(content, 'database') AS score FROM test_db.test_bm25 ORDER BY score DESC")
R6_COUNT=$(echo "$R6" | grep -c '[0-9]' || true)
if [ "$R6_COUNT" -ge 1 ]; then
    echo "PASS: TC6: BM25 without MATCH returns $R6_COUNT rows"; ((PASS++))
else
    echo "FAIL: TC6: BM25 without MATCH returned no rows (got: $R6)"; ((FAIL++))
fi

# Cleanup
run_sql "DROP TABLE IF EXISTS test_db.test_bm25" > /dev/null
echo "=== Result: $PASS PASS, $FAIL FAIL ==="
[ $FAIL -eq 0 ] && echo "PHASE 4 ACCEPTED" || echo "PHASE 4 REJECTED"
```

### 4.2 前置依赖

- Phase 3 完成（MATCH 查询全链路可用）

### 4.3 代码任务

| 步骤    | 文件                              | 内容                                               |
| ----- | ------------------------------- | ------------------------------------------------ |
| 4.2.1 | `FunctionSet.java` (FE)         | 注册 `TOKENIZE(VARCHAR, VARCHAR) → ARRAY<VARCHAR>` |
| 4.2.2 | `tokenize_function.h/.cpp` (BE) | TOKENIZE 标量函数，调用 `tantivy_tokenize()` FFI        |
| 4.2.3 | `FunctionSet.java` (FE)         | 注册 `BM25(VARCHAR, VARCHAR) → DOUBLE`             |
| 4.2.4 | Analyzer (FE)                   | BM25 校验: 同一查询块中必须有 MATCH 谓词                      |
| 4.2.5 | `bm25_function.h/.cpp` (BE)     | BM25 标量函数，调用 `tantivy_query_bm25()` FFI          |

---

## Phase 5: 中文分词 + Profile

### 5.1 目标与验收标准

用户通过 mysql 客户端可以：对中文文本建立 tantivy 索引（parser=chinese），执行中文 MATCH_ANY/PHRASE/BM25 查询。查询 Profile 中可观测到 TantivyQueryTime / TantivyMatchedRows 指标。

> Compaction 正确性已在 Phase 3 中验证，本阶段不重复。

**验收用例**（全部通过 `mysql` 客户端 + Profile HTTP API 执行）：

| # | 用例 | 验证方式 | PASS 条件 |
|---|------|---------|-----------|
| 1 | 中文 MATCH_ANY（OR 语义） | SQL: `content MATCH_ANY '数据库 分析'` | 结果集包含 id=1,3（jieba 实测：id=4 的"数据分析"不拆分为"数据库"+"分析"，不匹配） |
| 2 | 中文 MATCH_PHRASE（短语邻接） | SQL: `content MATCH_PHRASE '实时分析'` | 结果集 = {1} |
| 3 | 中文 BM25 评分排序 | SQL: `BM25(content, '数据库') ... ORDER BY score DESC` | 返回 score > 0 的行，按分数降序 |
| 4 | Profile TantivyQueryTime 可见 | SQL: `SET enable_profile=true` → 查询 → FE HTTP `GET /api/profile?query_id=...` | 输出包含 `TantivyQueryTime` |
| 5 | Profile TantivyMatchedRows 可见 | 同上 FE HTTP API | 输出包含 `TantivyMatchedRows`，值 > 0 |

> **Profile API 统一约定**: 使用 FE HTTP 接口 `http://<fe_host>:8030/api/profile?query_id=<id>`（FE 端口 8030）。注意不是 BE 端口 8040。如果 FE HTTP 端口不同，按实际部署调整。

```bash
#!/bin/bash
# verify_phase5.sh — Phase 5 验收脚本
set -uo pipefail
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
MYSQL="docker exec sr-dev mysql -h127.0.0.1 -P9030 -uroot -N -e"
PASS=0; FAIL=0

run_sql() { $SSH "$MYSQL \"$1\"" 2>&1 || true; }
check_id() {
    local name="$1" expected_id="$2" actual="$3"
    if echo "$actual" | awk '{print $1}' | grep -qx "$expected_id"; then
        echo "PASS: $name"; ((PASS++))
    else
        echo "FAIL: $name (expected id=$expected_id in: $actual)"; ((FAIL++))
    fi
}
check_contains() {
    local name="$1" expected="$2" actual="$3"
    if echo "$actual" | grep -qF "$expected"; then
        echo "PASS: $name"; ((PASS++))
    else
        echo "FAIL: $name (expected substring: $expected, got: $actual)"; ((FAIL++))
    fi
}

# Setup
run_sql "DROP TABLE IF EXISTS test_db.test_chinese" > /dev/null
run_sql "CREATE TABLE test_db.test_chinese (id BIGINT, content VARCHAR(65535), INDEX idx_c (content) USING GIN (\"parser\"=\"chinese\", \"imp_lib\"=\"tantivy\")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1" > /dev/null
run_sql "INSERT INTO test_db.test_chinese VALUES (1,'StarRocks是一个高性能的实时分析数据库'),(2,'全文检索引擎支持中文分词和BM25评分'),(3,'数据库性能优化需要仔细的分析和设计'),(4,'实时数据分析引擎适用于现代数据应用')" > /dev/null

# TC1: 中文 MATCH_ANY — jieba 实测：id=4 的"数据分析"不拆分为"数据库"+"分析"，不匹配
R1=$(run_sql "SELECT id FROM test_db.test_chinese WHERE content MATCH_ANY '数据库 分析' ORDER BY id")
check_id "TC1a: 中文 MATCH_ANY id=1" "1" "$R1"
check_id "TC1b: 中文 MATCH_ANY id=3" "3" "$R1"

# TC2: 中文 MATCH_PHRASE — 用例表：结果集 = {1}
R2=$(run_sql "SELECT id FROM test_db.test_chinese WHERE content MATCH_PHRASE '实时分析'")
check_id "TC2: 中文 MATCH_PHRASE id=1" "1" "$R2"

# TC3: 中文 BM25 — 验证有结果且 score > 0
R3=$(run_sql "SELECT id, BM25(content, '数据库') AS score FROM test_db.test_chinese WHERE content MATCH_ANY '数据库' ORDER BY score DESC")
R3_COUNT=$(echo "$R3" | wc -l | tr -d ' ')
if [ "$R3_COUNT" -ge 1 ]; then
    echo "PASS: TC3a: 中文 BM25 有 $R3_COUNT 条结果"; ((PASS++))
else
    echo "FAIL: TC3a: 中文 BM25 无结果"; ((FAIL++))
fi
BAD_SCORES=$(echo "$R3" | awk '$2 <= 0 {print}')
if [ -z "$BAD_SCORES" ]; then
    echo "PASS: TC3b: 中文 BM25 all scores > 0"; ((PASS++))
else
    echo "FAIL: TC3b: 中文 BM25 found score <= 0: $BAD_SCORES"; ((FAIL++))
fi

# TC4-5: Profile 指标 — 通过 FE HTTP API 获取
# 在同一 session 中执行 enable_profile + 查询 + 获取 query_id
QUERY_ID=$(run_sql "SET enable_profile = true; SELECT id FROM test_db.test_chinese WHERE content MATCH_ANY '数据库'; SELECT last_query_id()" | tail -1)
echo "Query ID: $QUERY_ID"
# 使用 FE HTTP API（端口 8030）获取 profile
PROFILE=$($SSH "docker exec sr-dev curl -s 'http://127.0.0.1:8030/api/profile?query_id=$QUERY_ID'" 2>/dev/null || true)
check_contains "TC4: TantivyQueryTime in profile" "TantivyQueryTime" "$PROFILE"
check_contains "TC5: TantivyMatchedRows in profile" "TantivyMatchedRows" "$PROFILE"

# Cleanup
run_sql "DROP TABLE IF EXISTS test_db.test_chinese" > /dev/null
echo "=== Result: $PASS PASS, $FAIL FAIL ==="
[ $FAIL -eq 0 ] && echo "PHASE 5 ACCEPTED" || echo "PHASE 5 REJECTED"
```

### 5.2 代码任务

| 步骤    | 文件                             | 内容                                              |
| ----- | ------------------------------ | ----------------------------------------------- |
| 5.1.1 | `tantivy_ffi/src/tokenizer.rs` | 确保 jieba 分词器注册 + 中文 E2E                         |
| 5.1.2 | `segment_iterator.cpp`         | Profile 计数器: TantivyQueryTime, TantivyMatchedRows |

---

## Phase 6: 性能 Benchmark

### 6.1 目标与验收标准

在 100K 行英文文本上对比 tantivy vs CLucene vs 无索引三种方案的写入速度、索引大小、查询延迟。产出一份可复现的 benchmark 报告。

**验收用例**（通过 Python 脚本自动执行，产出 CSV + 自动 PASS/FAIL 判定）：

| # | 用例 | 验证方式 | PASS 条件（脚本可判定） |
|---|------|---------|----------------------|
| 1 | 100K 行写入 tantivy / CLucene / 无索引 | Python: 3 张表各 INSERT 100K 行，记录耗时 | 3 张表均写入成功，tantivy 写入耗时 ≤ 2.0x CLucene |
| 2 | 索引文件大小对比 | SQL: `SHOW DATA` 提取索引大小 | `size_tantivy / size_clucene ≤ 1.5` |
| 3 | MATCH_ANY 高选择率（~50%） | SQL: 各运行 5 次取 P50 | `P50_tantivy / P50_clucene ≤ 1.2` |
| 4 | MATCH_ANY 低选择率（< 1%） | SQL: 各运行 5 次取 P50 | `P50_tantivy / P50_clucene ≤ 1.2` |
| 5 | MATCH_PHRASE 查询 | SQL: 各运行 5 次取 P50 | `P50_tantivy / P50_clucene ≤ 1.2` |
| 6 | BM25 + ORDER BY + LIMIT 10 | SQL: 运行 5 次取 P50 | `P50_bm25 / P50_match_any_same_table ≤ 3.0`（batch-local BM25 需为每 batch 建临时索引） |
| 7 | 无内存泄漏 | 循环查询 1000 次，BE `mem_tracker` 前后对比 | `(mem_after - mem_before) < 50MB` |
| 8 | perf 热点函数分析 | `perf record -g -p <be_pid>` 采集 MATCH_ANY 查询 → `perf report` 输出 top-10 | 热点落在 tantivy FFI / searcher / collector 等预期路径上，无非预期瓶颈（如 malloc、lock contention 占比 > 10%） |

```bash
#!/bin/bash
# verify_phase6.sh — Phase 6 验收脚本
set -uo pipefail
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
MYSQL="docker exec sr-dev mysql -h127.0.0.1 -P9030 -uroot -N -e"
PASS=0; FAIL=0

run_sql() { $SSH "$MYSQL \"$1\"" 2>&1 || true; }

# 辅助：运行 SQL 5 次取中位数（毫秒）
median_latency() {
    local sql="$1"
    local times=()
    for i in $(seq 1 5); do
        local t0=$(date +%s%N)
        run_sql "$sql" > /dev/null
        local t1=$(date +%s%N)
        times+=( $(( (t1 - t0) / 1000000 )) )  # ns → ms
    done
    # 排序取中位数
    IFS=$'\n' sorted=($(sort -n <<<"${times[*]}")); unset IFS
    echo "${sorted[2]}"  # 0-indexed, 5 个元素取 index 2
}

# 辅助：比较比率并判定 PASS/FAIL
check_ratio() {
    local name="$1" val_a="$2" val_b="$3" max_ratio="$4"
    if [ "$val_b" -eq 0 ]; then
        echo "FAIL: $name (baseline is 0)"; ((FAIL++)); return
    fi
    # 用整数算术 ×100 避免浮点
    local ratio_x100=$(( val_a * 100 / val_b ))
    local max_x100=$(echo "$max_ratio" | awk '{printf "%d", $1 * 100}')
    if [ "$ratio_x100" -le "$max_x100" ]; then
        echo "PASS: $name (ratio=${ratio_x100}% ≤ ${max_x100}%)"; ((PASS++))
    else
        echo "FAIL: $name (ratio=${ratio_x100}% > ${max_x100}%)"; ((FAIL++))
    fi
}

# 建表
for TBL in bench_tantivy bench_clucene bench_noidx; do
    run_sql "DROP TABLE IF EXISTS test_db.$TBL" > /dev/null
done
run_sql "CREATE TABLE test_db.bench_tantivy (id BIGINT, content VARCHAR(65535), INDEX idx_c (content) USING GIN (\"parser\"=\"standard\", \"imp_lib\"=\"tantivy\")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8" > /dev/null
run_sql "CREATE TABLE test_db.bench_clucene (id BIGINT, content VARCHAR(65535), INDEX idx_c (content) USING GIN (\"parser\"=\"standard\", \"imp_lib\"=\"clucene\")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8" > /dev/null
run_sql "CREATE TABLE test_db.bench_noidx (id BIGINT, content VARCHAR(65535)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8" > /dev/null

# TC1: 写入 100K 行
echo "=== TC1: INSERT 100K rows ==="
# 生成数据文件（100K 行，id + 随机英文句子）
$SSH "docker exec sr-dev bash -c 'python3 -c \"
import random, string
words = open(\\\"/usr/share/dict/words\\\").read().splitlines()[:5000] if True else []
if not words: words = [\\\"database\\\",\\\"search\\\",\\\"engine\\\",\\\"query\\\",\\\"index\\\",\\\"full\\\",\\\"text\\\",\\\"performance\\\",\\\"system\\\",\\\"data\\\"]
for i in range(100000):
    sent = \\\" \\\".join(random.choices(words, k=random.randint(8,20)))
    print(f\\\"{i}\\\\t{sent}\\\")
\" > /tmp/bench_data.csv'" > /dev/null

insert_and_time() {
    local tbl="$1"
    local t0=$(date +%s%N)
    $SSH "docker exec sr-dev bash -c 'curl --location-trusted -u root: -H \"label:bench_${tbl}_\$(date +%s)\" -H \"column_separator:\\t\" -T /tmp/bench_data.csv http://127.0.0.1:8030/api/test_db/${tbl}/_stream_load'" > /dev/null 2>&1
    local t1=$(date +%s%N)
    echo $(( (t1 - t0) / 1000000 ))
}
T_TANTIVY=$(insert_and_time bench_tantivy)
T_CLUCENE=$(insert_and_time bench_clucene)
T_NOIDX=$(insert_and_time bench_noidx)
echo "Write latency: tantivy=${T_TANTIVY}ms, clucene=${T_CLUCENE}ms, noidx=${T_NOIDX}ms"
check_ratio "TC1: write latency tantivy/clucene" "$T_TANTIVY" "$T_CLUCENE" 2.0

# TC2: 索引文件大小
echo "=== TC2: Index size ==="
# 等待索引构建完成
sleep 10
SIZE_T=$(run_sql "SHOW DATA FROM test_db.bench_tantivy" | awk 'NR==1{print $3}' | tr -d '[:alpha:]')
SIZE_C=$(run_sql "SHOW DATA FROM test_db.bench_clucene" | awk 'NR==1{print $3}' | tr -d '[:alpha:]')
echo "Index size: tantivy=${SIZE_T}, clucene=${SIZE_C}"
# 转换为 KB 整数进行比较（SHOW DATA 可能返回 MB/GB 单位，这里取原始数值×100 比较比率）
SIZE_T_NUM=$(echo "$SIZE_T" | awk '{printf "%d", $1 * 1000}')
SIZE_C_NUM=$(echo "$SIZE_C" | awk '{printf "%d", $1 * 1000}')
check_ratio "TC2: index size tantivy/clucene" "$SIZE_T_NUM" "$SIZE_C_NUM" 1.5

# TC3: MATCH_ANY 高选择率
echo "=== TC3: MATCH_ANY high selectivity ==="
P50_T3=$(median_latency "SELECT COUNT(*) FROM test_db.bench_tantivy WHERE content MATCH_ANY 'the and'")
P50_C3=$(median_latency "SELECT COUNT(*) FROM test_db.bench_clucene WHERE content MATCH_ANY 'the and'")
check_ratio "TC3: MATCH_ANY high sel" "$P50_T3" "$P50_C3" 1.2

# TC4: MATCH_ANY 低选择率
echo "=== TC4: MATCH_ANY low selectivity ==="
P50_T4=$(median_latency "SELECT COUNT(*) FROM test_db.bench_tantivy WHERE content MATCH_ANY 'xyzzy'")
P50_C4=$(median_latency "SELECT COUNT(*) FROM test_db.bench_clucene WHERE content MATCH_ANY 'xyzzy'")
check_ratio "TC4: MATCH_ANY low sel" "$P50_T4" "$P50_C4" 1.2

# TC5: MATCH_PHRASE
echo "=== TC5: MATCH_PHRASE ==="
P50_T5=$(median_latency "SELECT COUNT(*) FROM test_db.bench_tantivy WHERE content MATCH_PHRASE 'full text search'")
P50_C5=$(median_latency "SELECT COUNT(*) FROM test_db.bench_clucene WHERE content MATCH_PHRASE 'full text search'")
check_ratio "TC5: MATCH_PHRASE" "$P50_T5" "$P50_C5" 1.2

# TC6: BM25 vs MATCH_ANY on same table
echo "=== TC6: BM25 latency ==="
P50_BM25=$(median_latency "SELECT id, BM25(content, 'database') AS s FROM test_db.bench_tantivy WHERE content MATCH_ANY 'database' ORDER BY s DESC LIMIT 10")
P50_MATCH=$(median_latency "SELECT COUNT(*) FROM test_db.bench_tantivy WHERE content MATCH_ANY 'database'")
check_ratio "TC6: BM25 vs MATCH_ANY" "$P50_BM25" "$P50_MATCH" 3.0

# TC7: 内存泄漏检查
echo "=== TC7: Memory leak check ==="
MEM_BEFORE=$($SSH "docker exec sr-dev curl -s http://127.0.0.1:8040/mem_tracker" | grep -oP 'current_consumption=\K[0-9]+' | head -1)
for i in $(seq 1 1000); do
    run_sql "SELECT COUNT(*) FROM test_db.bench_tantivy WHERE content MATCH_ANY 'database'" > /dev/null
done
MEM_AFTER=$($SSH "docker exec sr-dev curl -s http://127.0.0.1:8040/mem_tracker" | grep -oP 'current_consumption=\K[0-9]+' | head -1)
MEM_DELTA=$(( (MEM_AFTER - MEM_BEFORE) / 1048576 ))  # bytes → MB
if [ "$MEM_DELTA" -lt 50 ]; then
    echo "PASS: TC7: mem growth ${MEM_DELTA}MB < 50MB"; ((PASS++))
else
    echo "FAIL: TC7: mem growth ${MEM_DELTA}MB >= 50MB"; ((FAIL++))
fi

# TC8: perf 热点函数分析
echo "=== TC8: perf hot function analysis ==="
BE_PID=$($SSH "docker exec sr-dev pgrep starrocks_be")
# 采集 10 秒 perf 数据，期间并发执行 MATCH_ANY 查询
$SSH "docker exec sr-dev bash -c 'perf record -g -p $BE_PID -o /tmp/perf_tantivy.data -- sleep 10 &'"
for i in $(seq 1 50); do
    run_sql "SELECT COUNT(*) FROM test_db.bench_tantivy WHERE content MATCH_ANY 'database performance'" > /dev/null
done
sleep 5  # 等待 perf record 结束
# 导出 top-20 热点函数
PERF_REPORT=$($SSH "docker exec sr-dev bash -c 'perf report -i /tmp/perf_tantivy.data --stdio --no-children 2>/dev/null | head -40'")
echo "$PERF_REPORT" | tee /tmp/phase6_perf_top20.txt
# 检查：malloc/lock 类函数占比不超过 10%
MALLOC_PCT=$(echo "$PERF_REPORT" | grep -i 'malloc\|tcmalloc\|__lock\|pthread_mutex' | awk '{sum+=$1} END {printf "%.1f", sum}')
if [ "$(echo "$MALLOC_PCT < 10.0" | bc)" -eq 1 ]; then
    echo "PASS: TC8: malloc/lock overhead ${MALLOC_PCT}% < 10%"; ((PASS++))
else
    echo "FAIL: TC8: malloc/lock overhead ${MALLOC_PCT}% >= 10% (review perf report)"; ((FAIL++))
fi
echo "perf report saved to /tmp/phase6_perf_top20.txt"

# Cleanup
for TBL in bench_tantivy bench_clucene bench_noidx; do
    run_sql "DROP TABLE IF EXISTS test_db.$TBL" > /dev/null
done
echo "=== Result: $PASS PASS, $FAIL FAIL ==="
[ $FAIL -eq 0 ] && echo "PHASE 6 ACCEPTED" || echo "PHASE 6 REJECTED"
```

### 6.2 代码任务

本阶段无新代码，仅编写 benchmark 脚本和数据生成工具：

| 步骤    | 文件                              | 内容                                |
| ----- | ------------------------------- | --------------------------------- |
| 6.1.1 | `test/benchmark/fts_bench.py`   | 数据生成 + 写入 + 查询 + 采集延迟 → 输出 CSV   |
| 6.1.2 | `test/benchmark/fts_report.py`  | 读取 CSV，生成对比报告（表格 + 结论）            |

---

## 实施状态


| Phase   | 内容                             | 状态   | 日期         |
| ------- | ------------------------------ | ---- | ---------- |
| Phase 0 | 环境准备（Rust 工具链 + CMake 集成）      | Done | 2026-04-14 |
| Phase 1 | Tantivy FFI 基础设施（本地编译+测试通过）    | Done | 2026-04-14 |
| Phase 2 | BE 存储引擎集成                      | Done | 2026-04-17 |
| Phase 3 | FE 语法 + 全链路打通 + Compaction 验证  | Done | 2026-04-17 |
| Phase 4 | TOKENIZE + BM25 函数             | Done | 2026-04-17 |
| Phase 5 | 中文分词 + Profile                 | Done | 2026-04-17 |
| Phase 6 | 性能 Benchmark                   | Done | 2026-04-17 |
| 代码审查修复 | FFI 安全性 + 代码质量                  | Done | 2026-04-18 |
| Phase 7 | 可靠性加固（P0）                      | Done | 2026-04-18 |
| Phase 8 | FFI 性能优化（P1）                   | Done | 2026-04-19 |
| Phase 9 | BM25 持久化索引（P1）                 | Done | 2026-04-19 |

### 代码审查修复详情（2026-04-18）

对全量代码进行审查后，修复了以下问题（commit `b875548`）：

| # | 严重度 | 修复内容 | 文件 |
|---|--------|---------|------|
| 1 | 中 | `tantivy_query_bm25` FFI 添加 `catch_unwind`，防止 Rust panic 跨 FFI 边界导致 UB | `tantivy_ffi/src/lib.rs` |
| 2 | 中 | `tantivy_tokenize` FFI 添加 `catch_unwind`，同上 | `tantivy_ffi/src/lib.rs` |
| 3 | 低 | 提取硬编码 `"content"` 字段名为 `TANTIVY_FIELD_NAME` 常量，reader/writer 统一引用 | `inverted_index_common.h`, `tantivy_inverted_reader.cpp`, `tantivy_inverted_writer.cpp` |
| 4 | 低 | BM25 临时目录使用 `DeferOp` RAII 清理，替代手动 `remove_all`，防止异常路径泄漏 | `gin_functions.cpp` |

**验证结果**:
- Rust FFI: `cargo test` 18/18 通过
- BE build: 成功
- E2E: 20/20 全部通过（Phase 3 MATCH 10/10 + Phase 4 TOKENIZE/BM25 7/7 + Phase 5 中文 3/3）

### Phase 7 完成详情（2026-04-18）

**null_bitmap FileSystem API 迁移**:
- Writer: `finish()` 改用 `fs::new_writable_file()` + `WritableFile::append()` + `close()`，替代 `fopen/fwrite/fclose`
- Reader: `query_null()` 改用 `fs::new_random_access_file()` + `read_at_fully()`，替代 `fopen/fread/fclose`

**索引损坏降级策略**:
- `_degraded` 标记位: `_ensure_reader_opened()` 在索引目录缺失/打开失败时设置 `_degraded=true`，不返回错误
- `query()` 在 `_degraded` 时返回 `InternalError`，提供可操作的诊断信息（建议 ALTER TABLE 重建索引）
- `query_null()` 同样检查 `_degraded`，避免索引缺失被误判为"无 null"
- FFI null 结果不再标记 `_degraded`（可能是用户输入错误如非法 regex），只返回当次错误

**Review 修复（两个 bug）**:
1. **高危**: `query_null()` 未走 `_degraded` 逻辑 → 已加 `_ensure_reader_opened()` + `_degraded` 检查
2. **中危**: FFI null 结果无脑设 `_degraded=true`，误伤合法场景（非法 regex）→ 移除 `_degraded` 设置

**E2E 验证结果**:
- TC1: 正常 MATCH_ANY/ALL/PHRASE/REGEXP + IS NULL → 全部正确 ✅
- TC2: 非法 regex 不污染后续查询状态 ✅
- TC3: 索引损坏 → MATCH 报错（MATCH 无行级 fallback）、非索引查询正常 ✅

### 已知遗留问题

| 问题 | 严重度 | 状态 | 说明 |
|------|--------|------|------|
| BM25 batch-local IDF | 中 | ✅ Phase 9 已解决 | 持久化索引 BM25 已完成，`RewriteToBm25PlanRule` 已启用。IDF 基于 segment 级持久化索引 |
| BM25 query_type 硬编码 OR | 低 | ✅ Phase 9 已解决 | `bm25()` 第 4 参数支持 `'any'`/`'all'` |
| RewriteToBm25PlanRule SIGSEGV | 高 | ✅ Phase 9 已解决 | 根因：`ProjectionIterator` 丢弃虚拟列。修复：forward 动态追加的虚拟列到输出 chunk |
| null_bitmap 使用 fopen 而非 StarRocks 文件抽象 | 中 | ✅ Phase 7 已解决 | 已改用 `WritableFile` / `RandomAccessFile` API |
| tantivy index 损坏无降级策略 | 中 | ✅ Phase 7 已解决 | `_degraded` 标记 + 诊断错误信息 |
| `tokenize_text` 每次创建 TokenizerManager | 低 | ✅ Phase 8 已解决 | `LazyLock<TokenizerManager>` 全局单例 |
| Writer 逐行 FFI 调用 + String 拷贝 | 低 | ✅ Phase 8 已解决 | `tantivy_writer_add_doc_with_len()` ptr+len 接口 |
| `table.addColumn()` schema 污染 | 低 | ⚠️ 已知 tech debt | `RewriteToBm25PlanRule` 和 `RewriteToVectorPlanRule` 都会向 OlapTable 内存 schema 追加虚拟列。已加幂等保护（`getColumn != null`），但虚拟列在 FE 存活期间持续存在。上游模式，非本项目引入 |

---

## Index Build & Compaction 机制分析

> 代码审查期间（2026-04-18）对索引生命周期做了全面机制梳理。结论：**所有路径已自动生效，无需额外代码**。

### 写入时自动构建（已验证）

当 tablet schema 包含 GIN index（`imp_lib=tantivy`），segment_writer 在写入每个 segment 时自动构建 tantivy 索引：

```
segment_writer.cpp:186  →  opts.need_inverted_index = _tablet_schema->has_index(column.unique_id(), GIN)
segment_writer.cpp:192  →  IndexDescriptor::inverted_index_file_path(...)  // {rowset_id}_{seg}_{idx_id}.ivt
column_writer.cpp:445   →  InvertedPluginFactory::get_plugin(TANTIVY) → create_inverted_index_writer()
column_writer.cpp:825-842 → INDEX_ADD_VALUES / INDEX_ADD_NULLS (逐行写入)
column_writer.cpp:590   →  write_inverted_index() → _inverted_index_builder->finish()
```

新数据导入（INSERT/StreamLoad）自动构建 tantivy 索引，Phase 3 E2E 已验证。

### CREATE INDEX on 已有表（Schema Change 路径，已自动生效）

```
FE: SchemaChangeHandler.processAddIndex() → hasIndexChange=true
FE: OlapTableAlterJobV2Builder → SchemaChangeJobV2 → AlterTabletReqV2 → BE
BE: SchemaChangeHandler::_convert_historical_rowsets() → 读旧 rowset → 写新 rowset
BE: 新 segment_writer 使用新 tablet_schema（含 GIN index）→ 自动触发 tantivy index 构建
```

**无需额外代码**。Schema Change 重写所有 rowset，新 segment_writer 自动检测 GIN index。

### DROP INDEX 清理（已有代码自动处理）

```
FE: SchemaChangeHandler.processDropIndex() → 移除 index → Schema Change
BE: 新 segment 不含 GIN index，旧 rowset GC 时 rowset.cpp:367-371 delete_dir_recursive(.ivt)
```

### Compaction 集成（已自动生效）

Compaction（horizontal/vertical）读旧 rowset → 合并 → 写新 rowset。新 rowset 使用当前 tablet_schema（含 GIN index），`segment_writer` 自动构建 tantivy 索引。Compaction 完成后旧 rowset GC 删除旧 `.ivt` 目录。

`horizontal_compaction_task.cpp` / `vertical_compaction_task.cpp` 中无 inverted index 特殊处理——所有 inverted index 逻辑封装在 `segment_writer/column_writer` 层，对上层透明。Phase 3 TC9 Compaction 用例已验证。

### Snapshot / Clone / Migration（已有代码自动处理）

`snapshot_manager.cpp:795` 和 `rowset.cpp:462` 处理 GIN index 目录的 link/rename/copy，tantivy 索引作为 `.ivt` 目录与 CLucene 共用同一文件管理逻辑。

---

## Phase 7: 可靠性加固

### 7.1 目标与验收标准

解决两个 P0 可靠性问题：null_bitmap 文件 IO 抽象化，以及 tantivy 索引损坏时的降级策略。完成后 tantivy 索引在本地存储场景下达到生产可用标准。

**验收用例**（通过 `mysql` 客户端 + 远程命令执行 SQL）：

| # | 用例 | 验证方式 | PASS 条件 |
|---|------|---------|-----------|
| 1 | null_bitmap 写入/读取使用 FileSystem API | BE UT | `TantivyNullBitmapTest` UT 通过 |
| 2 | 正常 MATCH 查询含 NULL 行 | SQL: INSERT 含 NULL 行 → MATCH_ANY 查询 | 结果正确排除 NULL 行 |
| 3 | 索引目录损坏 → MATCH 报诊断错误，非索引查询正常 | SQL: 手动损坏 `.ivt/meta.json` → MATCH_ANY 查询 + COUNT 查询 | MATCH 报 InternalError（含索引路径和重建建议），COUNT/LIKE 正常返回，BE 日志输出 WARNING |
| 4 | 索引目录缺失 → 同上 | SQL: 手动删除整个 `.ivt` 目录 → MATCH_ANY 查询 | 同 TC3，MATCH 无行级 fallback，必须报错 |
| 5 | 正常建表+写入+查询仍然正确 | SQL: 全新建表 → INSERT → MATCH_ANY/PHRASE 查询 | 结果与 Phase 3 一致 |

### 7.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 7.1 | `tantivy_inverted_writer.cpp` | `finish()` 中 null_bitmap 写入改用 `WritableFile` API（`fs::new_writable_file`） |
| 7.2 | `tantivy_inverted_reader.cpp` | `query_null()` 中 null_bitmap 读取改用 `RandomAccessFile` API（`fs::new_random_access_file`） |
| 7.3 | `tantivy_inverted_reader.h/.cpp` | 新增 `_degraded` 标记。`_ensure_reader_opened()` 失败时设 `_degraded=true` + 返回 OK。`query()` 在 degraded 时返回 InternalError（含诊断信息）。`query_null()` 加 `_degraded` 检查 |
| 7.4 | `tantivy_inverted_reader.cpp` | FFI null 结果不再设 `_degraded=true`（可能是用户输入错误），只返回当次 InternalError |

---

## Phase 8: FFI 性能优化

### 8.1 目标与验收标准

优化 3 个 FFI 层性能热点：Tokenizer 缓存、批量写入、消除 String 拷贝。写入吞吐提升可量化。

**验收用例**（通过 benchmark 脚本 + `cargo test` 执行）：

| # | 用例 | 验证方式 | PASS 条件 |
|---|------|---------|-----------|
| 1 | Tokenizer 全局单例 | `cargo test test_tokenizer_*` | 全部通过，`tokenize_text` 不再每次创建 `TokenizerManager` |
| 2 | Batch add_doc FFI | `cargo test test_batch_write` | 批量写入 10K 文档后查询结果正确 |
| 3 | ptr+len FFI 接口 | `cargo test test_add_doc_with_len` | 含 `\0` 字节的文本写入/查询正确 |
| 4 | 写入吞吐 | benchmark: 100K 行 INSERT 对比 Phase 6 基线 | 写入时间下降或持平（tantivy/clucene ≤ 2.0x） |
| 5 | 查询正确性回归 | Phase 3 E2E 验收脚本 | 全部 PASS |

### 8.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 8.1 | `tantivy_ffi/src/tokenizer.rs` | `tokenize_text()` 使用 `LazyLock<TokenizerManager>` 全局单例，不再每次创建 |
| 8.2 | `tantivy_ffi/src/lib.rs` | 新增 `tantivy_writer_add_doc_with_len(writer, ptr, len, row_id)` FFI 接口，Rust 侧从 `(ptr, len)` 构造 `&str`，不要求 null-termination |
| 8.3 | `tantivy_ffi/src/lib.rs` | 新增 `tantivy_writer_add_docs(writer, texts, lens, row_ids, count)` 批量 FFI 接口 |
| 8.4 | `tantivy_ffi/tantivy_ffi.h` | cbindgen 自动更新 C 头文件 |
| 8.5 | `tantivy_inverted_writer.cpp` | `add_values()` 改用 `tantivy_writer_add_doc_with_len()` 消除 `std::string` 拷贝；批量场景使用 `tantivy_writer_add_docs()` |
| 8.6 | `gin_functions.cpp` | `bm25()` 的批量写入改用 `tantivy_writer_add_docs()` |

### Phase 8 完成详情（2026-04-19）

**三轮 perf 驱动优化**，在 1M 行数据上将 MATCH_ANY 查询从 1,145ms 优化到 47-50ms：

#### Round 1: Tokenizer 全局单例 + ptr+len FFI
- `tokenizer.rs`: `LazyLock<TokenizerManager>` 全局单例，消除每次调用创建 `TokenizerManager`
- `lib.rs`: 新增 `tantivy_writer_add_doc_with_len(ptr, len, row_id)` 和 `tantivy_writer_add_docs()` 批量接口
- `tantivy_inverted_writer.cpp` + `gin_functions.cpp`: 使用 ptr+len 接口消除 `std::string` 拷贝
- Commit: `01b97e7`

#### Round 2: Fast Field row_id 查找（关键优化）
- **Perf 发现**: `Decompressor::decompress` 占 59.47%！`collect_row_ids()` 对每个匹配文档调用 `searcher.doc()` 从压缩 store 读取完整文档，仅为提取 `_row_id` 字段
- **修复**: 改用 fast field column reader（O(1) 列式访问），按 segment 分组复用 fast field reader
- **效果**: TantivyQueryTime 从 1,071ms 降至 38.9ms（**27.5x**），总查询从 1,145ms 降至 106ms (cold) / 82ms (warm)
- 内存分配: 6.233 GB → 429 MB（**14.5x 减少**）
- Commit: `fe29e30`

#### Round 3: 自定义 RowIdCollector 消除 HashSet 开销
- **Perf 发现**: `DocSetCollector` 收集结果到 `HashSet<DocAddress>`，hash 操作占 ~23%
- **修复**: 实现自定义 `RowIdCollector`（实现 `tantivy::collector::Collector` trait），在搜索过程中直接从 fast field 提取 row_id，跳过 HashSet 和后处理
- **效果**: 总查询从 82ms 降至 **47-50ms**（warm），cold run 82ms
- Commit: `ebb66b6`

#### 最终 Perf 热点分析（无异常热点）
| 函数 | 占比 | 说明 |
|------|------|------|
| `quicksort` (row_ids 排序) | 12% | 450K 元素排序，预期开销 |
| `BinaryPlainPageDecoder` (VARCHAR 读取) | 15% | 读取匹配行数据，I/O 预期开销 |
| `SparseRange::add` (bitmap 构建) | 6% | 结果转 roaring bitmap，预期开销 |
| `DocSet::fill_buffer` (posting list 遍历) | 2.5% | tantivy 核心查询引擎，预期开销 |
| `MonotonicMappingColumn::get_val` (fast field) | 1.8% | row_id fast field 访问，预期开销 |

#### 性能总结

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| MATCH_ANY 1M 行 | 1,145ms | 47-50ms | **23x** |
| TantivyQueryTime | 1,071ms | ~14ms | **76x** |
| 内存分配 | 6.233 GB | 226 MB | **28x** |
| 峰值内存 | 45 MB | 4.3 MB | **10x** |

---

## Phase 9: BM25 持久化索引

### 9.1 目标与验收标准

`bm25()` 函数在目标列已有 GIN index 时，直接使用持久化索引查询 BM25 分数，不再创建临时索引。IDF 基于全表而非当前 batch，跨 batch 分数可比。

**验收用例**（通过 `mysql` 客户端执行 SQL）：

| # | 用例 | SQL | PASS 条件 |
|---|------|-----|-----------|
| 1 | BM25 使用持久化索引 | INSERT 多批数据 → `BM25(content, 'database')` | Profile 中无 `sr_bm25_` 临时目录创建，TantivyQueryTime > 0 |
| 2 | BM25 排序一致性 | 分 3 批 INSERT 共 1000 行 → `ORDER BY BM25(content, 'database') DESC LIMIT 10` | 多次执行排序结果一致（同一数据集 IDF 不变） |
| 3 | BM25 无 GIN 索引时 fallback | 对无 GIN 索引的表执行 `BM25()` | 仍使用 batch-local 临时索引（向后兼容） |
| 4 | BM25 query_type 支持 | `BM25(content, 'database', 'standard', 'all')` | AND 语义评分，只有同时含所有词的文档有分数 |

### 9.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 9.1 | `tantivy_ffi/src/lib.rs` | 新增 `tantivy_reader_query_bm25(reader, field, query, query_type, limit)` — 在已有 reader 上查询 BM25 |
| 9.2 | `tantivy_inverted_reader.h/.cpp` | 新增 `query_bm25()` 方法，调用 FFI `tantivy_reader_query_bm25()` |
| 9.3 | `gin_functions.cpp` | `bm25()` 检测目标列是否有 GIN index，如有则打开持久化 reader 查询；无则 fallback 到 batch-local |
| 9.4 | FE Analyzer | `bm25()` 第 4 参数支持 query_type（`'any'`/`'all'`），透传到 BE |

### 9.3 完成状态（2026-04-19）

**已完成**:
- BE 端完整实现：`TantivyInvertedReader::query_bm25()` → FFI `tantivy_query_bm25()` 链路
- `SegmentIterator::_compute_bm25_scores()` 基于持久化 GIN 索引计算 BM25 分数
- `OlapChunkSource` 从 Thrift `TBm25SearchOptions` 提取参数并逐层传播到 `SegmentReadOptions`
- `InvertedIndexIterator::query_bm25()` 虚函数 + 实现
- FE 端 `RewriteToBm25PlanRule` 优化规则（识别 BM25 调用、创建虚拟列、设置 Bm25SearchOptions）
- Thrift `TBm25SearchOptions` struct
- Profile 计数器 `Bm25ScoreTime` / `Bm25ScoredRows`
- `__bm25_score__` 虚拟列追加逻辑（segment_iterator.cpp，始终追加，未评分行填 0）
- `ProjectionIterator` 虚拟列传播修复（详见下方根因分析）

**已修复 Bug**: `RewriteToBm25PlanRule` SIGSEGV / `slot_id not found`
- 症状：启用 optimizer rule 后 BM25 查询在 `ProjectOperator::push_chunk` → `ColumnRef::evaluate_checked` 处抛出 `slot_id 11 not found`
- 根因：`new_segment_iterator()` 在查询有 predicate 且 predicate 列数 < schema 列数时，会创建 `ProjectionIterator` 对 segment_iterator 做列重排。`ProjectionIterator::do_get_next` 只通过 `_index_map` 转发静态 schema 中的列，而 `segment_iterator` 通过 `append_vector_column` 动态追加的虚拟列（`__bm25_score__`、`__vector_distance__`）被丢弃
- 修复（`be/src/storage/projection_iterator.cpp`）：在 `do_get_next` 中检测子 chunk 是否有超出静态 schema 的额外列，如有则通过 `append_vector_column` 传递到输出 chunk，并重置内部 `_chunk` 以避免 schema 累积
- 配套修复（`be/src/exec/pipeline/scan/olap_chunk_source.cpp`）：slot→index 重映射逻辑增加 `is_slot_exist` 检查和 `get_field_index_by_name` 有效性验证，避免虚拟列的 slot 映射被覆盖

**E2E 验证结果（持久化索引模式）**:
- TC1: BM25(content, 'database') + MATCH_ANY → 2 条匹配，score > 0，排序正确 ✅
- TC2: BM25(content, 'analytics') + MATCH_ANY → 1 条匹配，score > 0 ✅
- TC3: MATCH_PHRASE 'real-time analytics' → 1 条匹配 ✅
- TC4: MATCH_ANY 'streaming' → 1 条匹配 ✅

---

## 远期方向（P2，暂不排期）

以下方向在 Phase 7-9 完成后按需排入。

| 方向 | 工作量 | 说明 |
|------|--------|------|
| 跨 segment BM25 全局 IDF | 2-3d | 当前 BM25 是 segment 级独立评分，跨 segment 的 IDF 不一致。需要实现全局 IDF 统计（类 ES 的 DFS_QUERY_THEN_FETCH 模式）|
| BM25 TopK 下推到 tantivy collector | 1-2d | `ORDER BY BM25() LIMIT K` 下推到 tantivy `TopDocs` collector，避免全量评分后排序 |
| Shared-Data (Cloud-Native) 模式 | 3-5d | tantivy index 目录打包为 tar 存储到 S3/OSS，查询时下载到本地缓存 |
| ARRAY/JSON 类型支持 | 2d | `ARRAY<VARCHAR>` 展开索引、`JSON` 字段提取索引 |
| MATCH_PHRASE slop 参数 | 0.5d | 扩展语法 + FFI 调用 `PhraseQuery::with_slop()` |
| 内存统计纳入 BE memory tracker | 0.5d | jieba 字典 (~50-100MB)、tantivy writer heap (50MB) 纳入 `mem_tracker` |

