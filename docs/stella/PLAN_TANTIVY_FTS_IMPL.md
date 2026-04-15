# Tantivy 全文检索 — 实施计划

> 分支: `fanzhen/main-stella-tantivy`
> 日期: 2026-04-14
> 设计文档: [DESIGN_TANTIVY_FTS.md](./DESIGN_TANTIVY_FTS.md)

---

## 0. 环境准备

### 0.1 分支 & Git

```bash
# 本地创建分支（已完成）
git checkout -b fanzhen/main-stella-tantivy main
git push fanzhen fanzhen/main-stella-tantivy

# 远程服务器 fetch
SSH="ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254"
$SSH "docker exec sr-dev bash -c 'cd /build && git fetch origin && git checkout origin/fanzhen/main-stella-tantivy'"
```

### 0.2 Rust 工具链（远程服务器）

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

### 0.3 编译 & 部署流程

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
8. 远程: 重启 FE/BE（见 0.4）
9. 远程: mysql E2E 验证
```

### 0.4 FE/BE 重启

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

### 0.5 CMake 集成 Rust FFI

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

**目标**: Rust FFI 层可编译，BE C++ 可调用 tantivy 进行索引构建和查询。

### 1.1 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 1.1.1 | `tantivy_ffi/Cargo.toml` | 新建 Rust 项目，依赖 tantivy=0.22, tantivy-jieba=0.10 |
| 1.1.2 | `tantivy_ffi/src/lib.rs` | `extern "C"` 入口，re-export writer/reader/tokenizer |
| 1.1.3 | `tantivy_ffi/src/writer.rs` | TantivyWriter: create/add_doc/add_null/commit/destroy |
| 1.1.4 | `tantivy_ffi/src/reader.rs` | TantivyReader: open/query_match_any/all/phrase/phrase_prefix/regexp/bm25/destroy |
| 1.1.5 | `tantivy_ffi/src/tokenizer.rs` | register_tokenizers() + tantivy_tokenize() |
| 1.1.6 | `tantivy_ffi/cbindgen.toml` | cbindgen 配置 |
| 1.1.7 | 生成 `tantivy_ffi.h` | `cbindgen --config cbindgen.toml --output tantivy_ffi.h` |

### 1.2 单元测试

Rust 侧 `#[test]`：
```rust
#[test]
fn test_write_and_query() {
    // 1. 创建临时目录
    // 2. tantivy_writer_create("standard")
    // 3. 写入 100 条文本 (add_doc)
    // 4. commit
    // 5. tantivy_reader_open
    // 6. query_match_any → 验证命中行数
    // 7. query_phrase → 验证短语命中
    // 8. query_regexp → 验证正则命中
    // 9. query_bm25 → 验证 score > 0
    // 10. tantivy_tokenize("hello world", "standard") → ["hello", "world"]
}
```

```bash
# 运行 Rust 测试
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && cd /build/be/src/storage/index/inverted/tantivy_ffi && cargo test'"
```

### 1.3 BE C++ 调用验证（可选）

在 Phase 2 之前，可以写一个简单的 C++ 测试程序 `tantivy_ffi_test.cpp`，链接 `libtantivy_ffi.a`，验证 FFI 调用可行：

```cpp
#include "tantivy_ffi.h"
TEST_F(TantivyFFITest, BasicWriteAndRead) {
    auto* w = tantivy_writer_create("/tmp/test_idx", "content", "standard");
    tantivy_writer_add_doc(w, "hello world", 0);
    tantivy_writer_add_doc(w, "foo bar", 1);
    tantivy_writer_commit(w);
    tantivy_writer_destroy(w);

    auto* r = tantivy_reader_open("/tmp/test_idx");
    auto* b = tantivy_query_match_any(r, "content", "hello");
    ASSERT_EQ(tantivy_bitmap_count(b), 1);
    ASSERT_EQ(tantivy_bitmap_row_ids(b)[0], 0);
    tantivy_bitmap_destroy(b);
    tantivy_reader_destroy(r);
}
```

### 1.4 验证标准

- `cargo build --release` 成功，产出 `libtantivy_ffi.a`
- `cargo test` 全部通过
- `cbindgen` 生成 `tantivy_ffi.h` 无错误
- （可选）C++ UT 链接 `.a` 并通过

### 1.5 编译 & 部署

```bash
# Phase 1 只需要 Rust 编译，不需要完整 BE build
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && cd /build/be/src/storage/index/inverted/tantivy_ffi && cargo build --release 2>&1 | tail -5'"
$SSH "docker exec sr-dev bash -c 'ls -lh /build/be/src/storage/index/inverted/tantivy_ffi/target/release/libtantivy_ffi.a'"
$SSH "docker exec sr-dev bash -c 'source \$HOME/.cargo/env && cd /build/be/src/storage/index/inverted/tantivy_ffi && cargo test 2>&1'"
```

---

## Phase 2: BE 存储引擎集成

**目标**: BE 可以写入带 tantivy 索引的 segment，通过 `_apply_inverted_index()` 正确过滤行。

### 2.1 前置依赖

- Phase 1 完成（libtantivy_ffi.a 可用）
- CMake 集成 Rust FFI（见 0.5）

### 2.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 2.2.1 | `inverted_index_common.h` | `InvertedImplementType::TANTIVY=3`, `MATCH_PHRASE_PREFIX_QUERY=10`, `MATCH_REGEXP_QUERY=11` |
| 2.2.2 | `tantivy/tantivy_plugin.h/.cpp` | 实现 `InvertedPlugin`，注册到 factory |
| 2.2.3 | `tantivy/tantivy_inverted_writer.h/.cpp` | 实现 `InvertedWriter`: init/add_values/add_nulls/finish，调用 FFI |
| 2.2.4 | `tantivy/tantivy_inverted_reader.h/.cpp` | 实现 `InvertedReader`: load/query → roaring::Roaring，新增 `query_with_score()` |
| 2.2.5 | `inverted_plugin_factory.cpp` | `case TANTIVY: return TantivyPlugin` |
| 2.2.6 | `segment_iterator.cpp` | `_apply_inverted_index()` 中处理 MATCH_PHRASE_PREFIX_QUERY, MATCH_REGEXP_QUERY |

### 2.3 单元测试

```cpp
// tantivy_inverted_writer_test.cpp
TEST_F(TantivyWriterTest, WriteAndRead) {
    // 1. 创建 TantivyInvertedWriter，parser=standard
    // 2. add_values: 100 条文本
    // 3. finish()
    // 4. 验证 {segment_dir}/{col_uid}_tantivy/ 目录存在
    // 5. TantivyInvertedReader::load()
    // 6. query(MATCH_ANY_QUERY, "keyword") → 验证 bitmap 正确
    // 7. query(MATCH_PHRASE_QUERY, "hello world") → 验证
    // 8. query(MATCH_REGEXP_QUERY, "hel.*") → 验证
}
```

```bash
# 运行 BE UT
$SSH "docker exec sr-dev bash -c 'cd /build && ./run-be-ut.sh --gtest_filter=TantivyWriter*'"
```

### 2.4 验证标准

- BE 编译通过（包含 tantivy FFI 链接）
- UT: TantivyInvertedWriter 写入 → TantivyInvertedReader 查询 → bitmap 正确
- 暂无 E2E（FE 尚未支持 tantivy）

### 2.5 编译

```bash
$SSH "docker exec sr-dev bash -c 'cd /build && ./build.sh --be 2>&1 | tail -20'"
```

---

## Phase 3: FE 语法 + Thrift + 全链路打通

**目标**: 从 mysql 客户端可以创建 tantivy 索引、写入数据、执行 MATCH_ANY/ALL/PHRASE/PHRASE_PREFIX/REGEXP 查询。

### 3.1 前置依赖

- Phase 2 完成（BE tantivy writer/reader 可用）

### 3.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 3.2.1 | `MatchExpr.java` | `MatchOperator` 新增 `MATCH_PHRASE`, `MATCH_PHRASE_PREFIX`, `MATCH_REGEXP` |
| 3.2.2 | `StarRocks.g4` / `StarRocksLex.g4` | 新增关键字 + 语法规则 |
| 3.2.3 | `AstBuilder.java` | 解析新 MATCH 语法，生成 MatchExpr |
| 3.2.4 | `AstBuilder.java` | `text_match`/`text_match_all`/`text_match_phrase` 函数 alias → MatchExpr |
| 3.2.5 | `ExprOpcodeRegistry.java` | MATCH_PHRASE→TExprOpcode.MATCH_PHRASE 等映射 |
| 3.2.6 | `Exprs.thrift` | TExprOpcode 新增 MATCH_PHRASE, MATCH_PHRASE_PREFIX, MATCH_REGEXP |
| 3.2.7 | `IndexAnalyzer.java` | `imp_lib=tantivy` 合法性校验 |
| 3.2.8 | `InvertedIndexParams.java` | `InvertedIndexImpType.TANTIVY` 枚举 |
| 3.2.9 | `Config.java` | `enable_experimental_tantivy`（默认 false） |
| 3.2.10 | BE: `segment_iterator.cpp` | TExprOpcode → InvertedIndexQueryType 映射 |

### 3.3 E2E 验证

```sql
-- 0. 开启 tantivy 实验特性
-- FE config: enable_experimental_tantivy = true
-- 或 ADMIN SET FRONTEND CONFIG ("enable_experimental_tantivy" = "true");

-- 1. 建表 + tantivy 索引
CREATE TABLE test_fts (
    id BIGINT,
    title VARCHAR(200),
    content VARCHAR(65535),
    INDEX idx_content (content) USING GIN ("parser"="standard", "imp_lib"="tantivy")
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;

-- 2. 插入测试数据
INSERT INTO test_fts VALUES
(1, 'StarRocks Introduction', 'StarRocks is a high-performance analytical database'),
(2, 'Full Text Search', 'Full text search enables users to find relevant documents'),
(3, 'Database Performance', 'Optimizing database performance requires careful analysis'),
(4, 'Real-time Analytics', 'Real-time analytics engine for modern data applications'),
(5, 'Search Engine Design', 'Search engine design involves inverted index and ranking');

-- 3. MATCH_ANY（OR 语义）
SELECT id, title FROM test_fts WHERE content MATCH_ANY 'database performance';
-- 预期: id=1,3 (至少命中 "database" 或 "performance")

-- 4. MATCH_ALL（AND 语义）
SELECT id, title FROM test_fts WHERE content MATCH_ALL 'database performance';
-- 预期: id=3 (同时包含两个词)

-- 5. MATCH_PHRASE（短语匹配）
SELECT id, title FROM test_fts WHERE content MATCH_PHRASE 'full text search';
-- 预期: id=2

-- 6. MATCH_PHRASE_PREFIX（前缀）
SELECT id, title FROM test_fts WHERE content MATCH_PHRASE_PREFIX 'search eng';
-- 预期: id=5 ("search engine" 前缀匹配)

-- 7. MATCH_REGEXP（正则）
SELECT id, title FROM test_fts WHERE content MATCH_REGEXP 'real.*time';
-- 预期: id=4 (注意 parser=standard 时 "Real-time" → "real" "time"，
--        MATCH_REGEXP 在 term 级别匹配，此处匹配 term "real" 满足 'real.*time' 的 term
--        具体行为取决于 tantivy RegexpQuery 实现)

-- 8. 清理
DROP TABLE test_fts;
```

### 3.4 编译 & 部署

```bash
# FE + BE 全量编译
$SSH "docker exec sr-dev bash -c 'cd /build && ./build.sh --fe --clean'"
$SSH "docker exec sr-dev bash -c 'cd /build && ./build.sh --be'"

# 重启 FE + BE（见 0.4）

# E2E
$SSH "docker exec sr-dev bash -c 'mysql -h127.0.0.1 -P9030 -uroot -e \"
  CREATE TABLE test_db.test_fts (id BIGINT, content VARCHAR(65535),
    INDEX idx_c (content) USING GIN (\\\"parser\\\"=\\\"standard\\\", \\\"imp_lib\\\"=\\\"tantivy\\\"))
  DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1;
\"'"
```

### 3.5 验证标准

- FE 编译通过，ANTLR 语法无冲突
- Thrift 代码生成正确
- E2E: CREATE TABLE + INSERT + 5 种 MATCH 查询结果正确
- `SHOW CREATE TABLE` 中可见 tantivy 索引属性

---

## Phase 4: TOKENIZE + BM25 函数

**目标**: `TOKENIZE()` 分词调试 + `BM25()` 相关性评分 + ORDER BY relevance 排序。

### 4.1 前置依赖

- Phase 3 完成（MATCH 查询全链路可用）

### 4.2 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 4.2.1 | `FunctionSet.java` (FE) | 注册 `TOKENIZE(VARCHAR, VARCHAR) → ARRAY<VARCHAR>` |
| 4.2.2 | `tokenize_function.h/.cpp` (BE) | TOKENIZE 标量函数，调用 `tantivy_tokenize()` FFI |
| 4.2.3 | `FunctionSet.java` (FE) | 注册 `BM25(VARCHAR, VARCHAR) → DOUBLE` |
| 4.2.4 | Analyzer (FE) | BM25 校验: 同一查询块中必须有 MATCH 谓词 |
| 4.2.5 | `bm25_function.h/.cpp` (BE) | BM25 标量函数，调用 `tantivy_query_bm25()` FFI |

### 4.3 E2E 验证

```sql
-- 1. TOKENIZE 函数
SELECT TOKENIZE('hello world', 'standard');
-- 预期: ["hello", "world"]

SELECT TOKENIZE('StarRocks是一个高性能分析数据库', 'chinese');
-- 预期: ["starrocks", "是", "一个", "高性能", "分析", "数据库"]

SELECT TOKENIZE('full-text search engine', 'english');
-- 预期: ["full", "text", "search", "engin"]  (Porter2 词干提取)

-- 2. BM25 评分 + 排序
CREATE TABLE test_bm25 (
    id BIGINT,
    content VARCHAR(65535),
    INDEX idx_c (content) USING GIN ("parser"="standard", "imp_lib"="tantivy")
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1;

INSERT INTO test_bm25 VALUES
(1, 'database database database'),         -- "database" 出现 3 次
(2, 'database performance'),                -- 出现 1 次
(3, 'search engine design'),                -- 不含 "database"
(4, 'analytical database system design');   -- 出现 1 次，文档更长

SELECT id, BM25(content, 'database') AS score
FROM test_bm25
WHERE content MATCH_ANY 'database'
ORDER BY score DESC;
-- 预期: id=1 分数最高（TF 最高），id=2/4 较低，id=3 不出现

-- 3. BM25 无 MATCH 应报错
SELECT id, BM25(content, 'database') AS score FROM test_bm25 ORDER BY score DESC;
-- 预期: Error: BM25() requires a MATCH predicate on the same column in WHERE clause

-- 4. 清理
DROP TABLE test_bm25;
```

### 4.4 验证标准

- TOKENIZE: 4 种 parser 分词结果正确
- BM25: 评分 > 0，排序符合 TF-IDF 直觉
- BM25 无 MATCH 报错
- 无性能回归（TOKENIZE 纯计算，毫秒级）

---

## Phase 5: 中文分词 + Compaction + Profile

**目标**: 中文全文检索 E2E 验证，Compaction 后索引正确性，查询 Profile 可观测。

### 5.1 代码任务

| 步骤 | 文件 | 内容 |
|------|------|------|
| 5.1.1 | `tantivy_ffi/src/tokenizer.rs` | 确保 jieba 分词器注册 + 中文 E2E |
| 5.1.2 | `tantivy/tantivy_inverted_writer.cpp` | Compaction 时调用 `IndexWriter::merge()` |
| 5.1.3 | `segment_iterator.cpp` | Profile 计数器: TantivyQueryTime, TantivyQueryRows |

### 5.2 E2E 验证

```sql
-- 1. 中文全文检索
CREATE TABLE test_chinese (
    id BIGINT,
    content VARCHAR(65535),
    INDEX idx_c (content) USING GIN ("parser"="chinese", "imp_lib"="tantivy")
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1;

INSERT INTO test_chinese VALUES
(1, 'StarRocks是一个高性能的实时分析数据库'),
(2, '全文检索引擎支持中文分词和BM25评分'),
(3, '数据库性能优化需要仔细的分析和设计'),
(4, '实时数据分析引擎适用于现代数据应用');

SELECT id FROM test_chinese WHERE content MATCH_ANY '数据库 分析';
-- 预期: id=1,3,4 (任一词命中)

SELECT id FROM test_chinese WHERE content MATCH_PHRASE '实时分析';
-- 预期: id=1 ("实时" + "分析" 邻接)

SELECT id, BM25(content, '数据库') AS score
FROM test_chinese WHERE content MATCH_ANY '数据库'
ORDER BY score DESC;
-- 预期: 包含"数据库"的行按相关性排序

-- 2. Compaction 验证
-- 多次 INSERT 产生多个 segment，触发 compaction
INSERT INTO test_chinese VALUES (5, '新增数据用于触发合并');
INSERT INTO test_chinese VALUES (6, '再次新增数据测试合并后索引正确性');
-- 等待自动 compaction 或手动触发
-- ALTER TABLE test_chinese COMPACT;

-- 再次查询验证结果一致
SELECT id FROM test_chinese WHERE content MATCH_ANY '数据库';
-- 预期: 结果与 compaction 前一致

-- 3. Profile 验证
SET enable_profile = true;
SELECT id FROM test_chinese WHERE content MATCH_ANY '数据库';
-- 查看 last_query_id() → SHOW PROFILELIST → 确认 TantivyQueryTime 指标存在

DROP TABLE test_chinese;
```

### 5.3 验证标准

- 中文分词: MATCH_ANY/PHRASE/BM25 结果正确
- Compaction: 合并后查询结果不变
- Profile: TantivyQueryTime, TantivyQueryRows 指标可见

---

## Phase 6: 性能 Benchmark

**目标**: tantivy vs CLucene vs 无索引 baseline 性能对比。

### 6.1 Benchmark 方案

```sql
-- 生成测试数据（100K 行英文文本）
CREATE TABLE bench_tantivy (
    id BIGINT,
    content VARCHAR(65535),
    INDEX idx_c (content) USING GIN ("parser"="standard", "imp_lib"="tantivy")
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8;

CREATE TABLE bench_clucene (
    id BIGINT,
    content VARCHAR(65535),
    INDEX idx_c (content) USING GIN ("parser"="standard", "imp_lib"="clucene")
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8;

CREATE TABLE bench_noidx (
    id BIGINT,
    content VARCHAR(65535)
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8;

-- 写入相同数据到三张表
-- INSERT INTO bench_tantivy SELECT ...;
-- INSERT INTO bench_clucene SELECT ...;
-- INSERT INTO bench_noidx SELECT ...;

-- 查询对比
-- Q1: MATCH_ANY 高选择率
-- Q2: MATCH_ANY 低选择率
-- Q3: MATCH_PHRASE
-- Q4: BM25 + ORDER BY + LIMIT（仅 tantivy）
```

### 6.2 关注指标

| 指标 | 说明 |
|------|------|
| 索引写入时间 | INSERT 耗时 |
| 索引文件大小 | 磁盘占用 |
| 查询延迟 (P50/P99) | 各 query 类型 |
| BM25 评分延迟 | tantivy 独有 |

### 6.3 验证标准

- tantivy 查询延迟 ≤ CLucene（预期 ~2x 更快）
- 索引文件大小合理（不显著大于 CLucene）
- BM25 评分延迟可接受（100K 行 < 100ms）
- 无内存泄漏（Rust FFI 对象正确释放）

---

## 实施状态

| Phase | 内容 | 状态 | 日期 |
|-------|------|------|------|
| Phase 0 | 环境准备（Rust 工具链 + CMake 集成） | Pending | - |
| Phase 1 | Tantivy FFI 基础设施 | Pending | - |
| Phase 2 | BE 存储引擎集成 | Pending | - |
| Phase 3 | FE 语法 + 全链路打通 | Pending | - |
| Phase 4 | TOKENIZE + BM25 函数 | Pending | - |
| Phase 5 | 中文分词 + Compaction + Profile | Pending | - |
| Phase 6 | 性能 Benchmark | Pending | - |
