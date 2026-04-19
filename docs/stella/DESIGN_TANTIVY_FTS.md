# 基于 Tantivy 的全文检索设计文档

> 分支: `fanzhen/main-stella-tantivy`
> 日期: 2026-04-14

---

## 1. 目标

在 StarRocks 中实现**全文检索（Full-Text Search）**能力：

1. **文本匹配查询**: 对 VARCHAR 列执行分词后的全文搜索（term / AND / OR / phrase / 前缀 / 正则）
2. **相关性评分**: BM25 评分 + ORDER BY relevance，实现类 Elasticsearch 的 relevance ranking
3. **分词可观测**: TOKENIZE 函数，用户可调试和验证分词效果

底层引擎选择 **Tantivy**（Rust 全文检索库），V1 与现有 CLucene/Builtin 共存，长期目标是替代 CLucene 成为默认实现。

---

## 2. 全文检索的用户视角

### 2.1 参考系：Elasticsearch 与 Doris

全文检索的行业标准是 Elasticsearch。OLAP 系统做全文检索，核心是把 ES 中最常用的查询能力用 SQL 语法表达出来。Doris 已经走了这一步，我们在语法上与 Doris 对齐，在能力上超越（BM25 评分）。

**ES 核心全文查询 → SQL 映射**:

| ES 查询类型 | 语义 | Doris SQL | StarRocks SQL（本次实现） |
|------------|------|----------|------------------------|
| `match` (operator=OR) | 分词后任一 token 命中 | `col MATCH_ANY 'query'` | `col MATCH_ANY 'query'` |
| `match` (operator=AND) | 分词后所有 token 命中 | `col MATCH_ALL 'query'` | `col MATCH_ALL 'query'` |
| `match_phrase` | token 按顺序邻接出现 | `col MATCH_PHRASE 'query'` | `col MATCH_PHRASE 'query'` |
| `match_phrase_prefix` | phrase + 最后词前缀扩展 | `col MATCH_PHRASE_PREFIX 'query'` | `col MATCH_PHRASE_PREFIX 'query'` |
| `regexp` | 对索引 term 正则匹配 | `col MATCH_REGEXP 'pattern'` | `col MATCH_REGEXP 'pattern'` |
| `_score` (BM25) | 相关性评分 | ⚠️ 有基础设施但未交付 | `BM25(col, 'query')` ✅ |

### 2.2 SQL 语法设计

#### 建表 & 建索引

```sql
-- 建表时创建全文索引
CREATE TABLE articles (
    id BIGINT,
    title VARCHAR(200),
    content VARCHAR(65535),
    INDEX idx_title (title) USING GIN ("parser"="standard", "imp_lib"="tantivy"),
    INDEX idx_content (content) USING GIN ("parser"="chinese", "imp_lib"="tantivy")
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8;

-- 在已有表上创建
CREATE INDEX idx_content ON articles(content)
    USING GIN ("parser"="chinese", "imp_lib"="tantivy");

-- 删除索引
DROP INDEX idx_content ON articles;
```

**索引属性**:

| 属性 | 可选值 | 默认 | 说明 |
|------|--------|------|------|
| `imp_lib` | `tantivy` / `clucene` / `builtin` | `clucene` | 底层引擎 |
| `parser` | `none` / `standard` / `english` / `chinese` | `none` | 分词器 |

**parser 行为定义**:

| parser | 分割规则 | 大小写 | 词干提取 | 典型用途 |
|--------|---------|--------|---------|---------|
| `none` | 不分词，整串=1个term | 保留 | 无 | 精确匹配、编码值 |
| `standard` | 非字母数字分割 | 小写 | 无 | 通用文本 |
| `english` | 非字母数字分割 | 小写 | Porter2 | 英文文档 |
| `chinese` | 结巴精确模式 | 拉丁字符小写 | 无 | 中文文档 |

#### 全文搜索查询

```sql
-- 1. 任意词匹配（OR）— 最常用
SELECT * FROM articles WHERE content MATCH_ANY '数据库 分析';

-- 2. 所有词匹配（AND）
SELECT * FROM articles WHERE content MATCH_ALL '高性能 实时 分析';

-- 3. 短语匹配 — 词必须按顺序紧邻出现
SELECT * FROM articles WHERE content MATCH_PHRASE '实时数据分析';

-- 4. 短语前缀匹配 — 适合搜索框自动补全
SELECT * FROM articles WHERE title MATCH_PHRASE_PREFIX '数据库 优';

-- 5. 正则匹配（注意：匹配的是索引中的 term，不是原始文本）
SELECT * FROM articles WHERE title MATCH_REGEXP 'Star.*ocks';
-- 如果 parser="standard"，原文 "StarRocks" 会被分词为 "starrocks"（小写），
-- 此时正则需要写 'star.*ocks' 才能匹配。
-- 这不同于 SQL 的 REGEXP/RLIKE（逐行对原文全文做正则）。
```

#### BM25 相关性评分 & 排序

```sql
-- 按相关性排序的典型全文检索 query
SELECT id, title, BM25(content, '实时分析引擎') AS relevance
FROM articles
WHERE content MATCH_ANY '实时分析引擎'
ORDER BY relevance DESC
LIMIT 20;

-- 多列评分叠加（标题权重更高）
SELECT id, title,
       BM25(title, '实时分析') * 3 + BM25(content, '实时分析') AS score
FROM articles
WHERE title MATCH_ANY '实时分析' OR content MATCH_ANY '实时分析'
ORDER BY score DESC
LIMIT 20;
```

`BM25(col, query)` 返回 `DOUBLE`，表示该行对该 query 的 BM25 相关性分数。分数越高表示越相关。

**两种执行路径**:
- **持久化索引路径**（推荐）：当目标列有 GIN 索引（`imp_lib=tantivy`）且 WHERE 中有 `MATCH_*` 谓词时，`RewriteToBm25PlanRule` 将 `bm25()` 改写为虚拟列 `__bm25_score__`，在 scan 层通过持久化索引计算 segment 级 BM25 分数。IDF 基于整个 segment，跨 batch 分数可比。
- **Batch-local fallback**：无 GIN 索引或 `imp_lib != tantivy` 时，`bm25()` 作为标量函数执行，每个 batch 创建临时 tantivy 索引计算 BM25。IDF 仅基于当前 batch（4096 行），跨 batch 分数不可比。

**约束**: `MATCH_*` 谓词本身要求目标列有 GIN 索引。因此 `bm25() + MATCH` 组合在无 GIN 索引的表上不可执行。无 MATCH 时 `bm25()` 仍可独立使用（走 batch-local fallback），但结果精度有限。

```sql
-- 推荐：BM25 配合 MATCH + GIN 索引 → 持久化索引 BM25
SELECT id, BM25(content, 'query') AS score
FROM t WHERE content MATCH_ANY 'query' ORDER BY score DESC LIMIT 10;

-- 可用但精度有限：BM25 无 MATCH → batch-local fallback
SELECT id, BM25(content, 'query') AS score FROM t ORDER BY score DESC LIMIT 10;
```

#### 分词调试

```sql
SELECT TOKENIZE('StarRocks是一个高性能分析数据库', 'chinese');
-- ["starrocks", "是", "一个", "高性能", "分析", "数据库"]

SELECT TOKENIZE('full-text search engine', 'english');
-- ["full", "text", "search", "engin"]

SELECT TOKENIZE('hello world', 'standard');
-- ["hello", "world"]
```

`TOKENIZE(text, parser_name)` 返回 `ARRAY<VARCHAR>`。纯计算函数，不依赖表和索引。

---

## 3. StarRocks 现状分析

### 3.1 已有的倒排索引基础设施

| 组件 | 现状 | 说明 |
|------|------|------|
| FE 索引类型 | `IndexDef.IndexType.GIN` | DDL 语法已有 |
| FE MATCH 算子 | `MATCH`, `MATCH_ANY`, `MATCH_ALL` | 3 种已有 |
| BE 实现 | CLucene + Builtin 两种 | 通过 `imp_lib` 属性选择 |
| BE 查询路径 | `SegmentIterator::_apply_inverted_index()` → Roaring Bitmap | 已打通 |
| 分词器 | none, standard, english, chinese | 4 种已有 |

### 3.2 本次新增

| 能力 | 现状 | 本次要做 |
|------|------|---------|
| MATCH_PHRASE | BE CLucene 有 `MatchPhraseOperator`，但 FE 未暴露 | FE 暴露 + tantivy 实现 |
| MATCH_PHRASE_PREFIX | 无 | 新增 |
| MATCH_REGEXP | 无 | 新增 |
| BM25 评分 | 无 | 新增（tantivy 原生支持） |
| TOKENIZE 函数 | 无 | 新增 |
| Tantivy 引擎 | 无 | 新增 `InvertedImplementType::TANTIVY` |

### 3.3 为什么用 Tantivy 而非继续用 CLucene

| 维度 | CLucene | Tantivy |
|------|---------|---------|
| 活跃度 | 原始项目已停滞 | Quickwit 维护，高度活跃 |
| 性能 | Lucene 4.x 水平 | 约 2x 于 Lucene（官方 benchmark） |
| BM25 | 可实现但笨重 | 原生支持，API 简洁 |
| 内存安全 | 手动 C++ 管理 | Rust 保证 |
| 分词生态 | 需自行集成 | 插件化 trait（jieba, lindera 等） |
| 代价 | 原生 C++ | 需要 Rust→C FFI 层 |

CLucene 的问题不是"不能用"，而是"做 BM25 太痛苦"。Tantivy 的 BM25 是 first-class feature，这是选择它的核心原因。

> 注：BM25 公式本身并不复杂，难点在于把 BM25 作为查询引擎的一等能力稳定落地。  
> 需要在倒排执行链路中高效获取并维护 `tf/df/doc_len/avgdl/N` 等统计信息，和分词器行为严格对齐，并与 `MATCH_*` 谓词、TopK 排序、segment 级合并、性能与可观测性协同工作。  
> Tantivy 在这些能力上原生支持更完整，因此工程实现成本和维护风险显著低于在 CLucene 路径上继续扩展。

---

## 4. 整体架构

```
用户 SQL
  │
  ▼
┌────────────────────────── FE ──────────────────────────┐
│                                                        │
│  Parser: 解析 MATCH_PHRASE / BM25() / TOKENIZE 语法    │
│    ↓                                                   │
│  Analyzer: 校验列有 GIN 索引，imp_lib 兼容             │
│    ↓                                                   │
│  Optimizer: MATCH 谓词下推到 ScanNode                  │
│    ↓                                                   │
│  Planner: 生成带 inverted_index 标记的 scan plan       │
│                                                        │
└────────────────────────────────────────────────────────┘
  │ Thrift
  ▼
┌────────────────────────── BE ──────────────────────────┐
│                                                        │
│  SegmentIterator::_apply_inverted_index()               │
│    ↓                                                   │
│  TantivyInvertedReader::query(query_type, query_value) │
│    ↓                                                   │
│  Tantivy FFI (C API)                                   │
│    ↓                                                   │
│  libtantivy_ffi.a (Rust 静态库)                        │
│    ↓                                                   │
│  tantivy crate: 索引读写 / 分词 / BM25 评分            │
│                                                        │
└────────────────────────────────────────────────────────┘
```

**写入路径**（索引构建）:
```
INSERT/导入 → SegmentWriter → ColumnWriter
  → TantivyInvertedWriter::add_values()  // 每批数据传给 tantivy
  → TantivyInvertedWriter::finish()      // commit，生成索引文件到 segment 目录
```

**查询路径**（索引检索）:
```
SELECT WHERE MATCH_* → SegmentIterator::_apply_inverted_index()
  → TantivyInvertedReader::query() → FFI → tantivy search
  → Roaring Bitmap (匹配的 row_id 集合)
  → 与其他谓词的 bitmap 做交/并集 → 只读取匹配行的数据
```

**BM25 路径（持久化索引，有 GIN + imp_lib=tantivy）**:
```
SELECT BM25(col, query) WHERE col MATCH_ANY query
  → FE: RewriteToBm25PlanRule 改写 bm25() 为虚拟列 __bm25_score__
  → FE: OlapScanNode 序列化 TBm25SearchOptions 到 Thrift
  → BE: SegmentIterator::_compute_bm25_scores()
    → InvertedIndexIterator::query_bm25() → FFI → tantivy BM25 scorer
    → score_map (row_id → float score)
  → BE: _do_get_next() 中 append_vector_column 输出分数到 chunk
```

**BM25 路径（batch-local fallback，无 GIN 索引）**:
```
SELECT BM25(col, query)
  → BE scalar function (gin_functions.cpp)
  → 每 batch 创建临时 tantivy 索引 → commit → reopen → query BM25
  → 每行返回 float score（IDF 仅基于当前 batch）
```

---

## 5. 分层设计

### 5.1 Tantivy FFI 层（Rust → C）

将 tantivy 编译为 Rust 静态库，通过 `extern "C"` 暴露 C API。

**目录结构**:
```
be/src/storage/index/inverted/tantivy_ffi/
├── Cargo.toml           # tantivy + tantivy-jieba 依赖
├── src/
│   ├── lib.rs           # extern "C" 入口函数
│   ├── writer.rs        # 索引写入
│   ├── reader.rs        # 索引查询 + BM25
│   └── tokenizer.rs     # 分词器注册
├── cbindgen.toml
└── tantivy_ffi.h        # cbindgen 生成的 C 头文件
```

**核心 C API**:

```c
// ===== 写入 =====
TantivyWriter* tantivy_writer_create(const char* index_dir, const char* field_name,
                                      const char* tokenizer_name);
void tantivy_writer_add_doc(TantivyWriter* w, const char* value, uint32_t row_id);
void tantivy_writer_add_null(TantivyWriter* w, uint32_t row_id);
int  tantivy_writer_commit(TantivyWriter* w);
void tantivy_writer_destroy(TantivyWriter* w);

// ===== 查询 =====
TantivyReader* tantivy_reader_open(const char* index_dir);
void tantivy_reader_destroy(TantivyReader* r);

TantivyBitmap* tantivy_query_match_any(TantivyReader* r, const char* field, const char* query);
TantivyBitmap* tantivy_query_match_all(TantivyReader* r, const char* field, const char* query);
TantivyBitmap* tantivy_query_phrase(TantivyReader* r, const char* field, const char* query);
TantivyBitmap* tantivy_query_phrase_prefix(TantivyReader* r, const char* field, const char* query);
TantivyBitmap* tantivy_query_regexp(TantivyReader* r, const char* field, const char* pattern);

uint32_t        tantivy_bitmap_count(TantivyBitmap* b);
const uint32_t* tantivy_bitmap_row_ids(TantivyBitmap* b);
void            tantivy_bitmap_destroy(TantivyBitmap* b);

// ===== BM25 =====
TantivyScoreResult* tantivy_query_bm25(TantivyReader* r, const char* field,
                                        const char* query, int query_type, int limit);
uint32_t tantivy_score_count(TantivyScoreResult* s);
uint32_t tantivy_score_row_id(TantivyScoreResult* s, uint32_t idx);
float    tantivy_score_value(TantivyScoreResult* s, uint32_t idx);
void     tantivy_score_destroy(TantivyScoreResult* s);

// ===== 分词 =====
TantivyTokens* tantivy_tokenize(const char* text, const char* tokenizer_name);
uint32_t       tantivy_tokens_count(TantivyTokens* t);
const char*    tantivy_tokens_get(TantivyTokens* t, uint32_t idx);
void           tantivy_tokens_destroy(TantivyTokens* t);
```

### 5.2 BE 存储引擎层

**新增文件**:
```
be/src/storage/index/inverted/tantivy/
├── tantivy_plugin.h/.cpp
├── tantivy_inverted_writer.h/.cpp
└── tantivy_inverted_reader.h/.cpp
```

**枚举扩展**:
```cpp
enum class InvertedImplementType { UNKNOWN=0, CLUCENE=1, BUILTIN=2, TANTIVY=3 };
enum class InvertedIndexQueryType { ..., MATCH_PHRASE_PREFIX_QUERY=10, MATCH_REGEXP_QUERY=11 };
```

**索引文件布局**:
```
{segment_dir}/
├── {segment_file}
├── {column_uid}_tantivy/    # 每列一个独立 tantivy 索引目录
│   ├── meta.json
│   ├── {seg}.term/.pos/.store/.fast/.fieldnorm
└── ...
```

### 5.3 FE 语法层

- `MatchExpr.MatchOperator`: 新增 `MATCH_PHRASE`, `MATCH_PHRASE_PREFIX`, `MATCH_REGEXP`
- `StarRocks.g4`: 新增关键字和语法规则
- `AstBuilder.java`: 解析新 MATCH 语法 + `text_match`/`text_match_all`/`text_match_phrase` 函数 alias
- `ExprOpcodeRegistry.java`: 新增 opcode 映射
- `IndexAnalyzer.java`: tantivy imp_lib 校验
- `FunctionSet`: 注册 `BM25(VARCHAR, VARCHAR) → DOUBLE` 和 `TOKENIZE(VARCHAR, VARCHAR) → ARRAY<VARCHAR>`
- `Config.java`: `enable_experimental_gin`（默认 false）

### 5.4 Thrift

```thrift
enum TExprOpcode { ..., MATCH_PHRASE, MATCH_PHRASE_PREFIX, MATCH_REGEXP }
```

### 5.5 查询优化

- 谓词下推: 复用现有 `_apply_inverted_index()` 路径
- BM25 TopK 优化（后续版本）: `ORDER BY BM25() LIMIT K` 下推到 tantivy TopK collector
- BM25 评分粒度: V1 segment 级独立 BM25，跨 segment 分数不严格可比

---

## 6. 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 底层引擎 | Tantivy（Rust） | BM25 原生支持，性能 2x，社区活跃 |
| FFI 方式 | `extern "C"` + cbindgen | 最成熟的 Rust→C 互操作方式 |
| 索引文件布局 | 每列一个子目录 | tantivy MmapDirectory 直接可用，零适配 |
| BM25 统计粒度 | Segment 级独立 | V1 简单可行，跨 segment 近似可接受 |
| 评分接口 | `BM25(col, query)` 显式函数 | 语义清晰，支持多列评分叠加 |
| 与 CLucene 关系 | V1 共存，通过 `imp_lib` 选择；长期替代 | 渐进路线，不破坏现有功能 |
| 灰度控制 | FE Config + table property + session var 三层 | `enable_experimental_gin` 全局门控 |

---

## 7. 涉及改动的文件清单

### FE

| 文件 | 改动 |
|------|------|
| `fe/fe-parser/.../ast/expression/MatchExpr.java` | MatchOperator 新增 3 个枚举值 |
| `fe/fe-grammar/StarRocks*.g4` | 新增 MATCH_PHRASE 等关键字和语法规则 |
| `fe/fe-core/.../sql/parser/AstBuilder.java` | 解析新 MATCH 语法 + text_match alias |
| `fe/fe-core/.../sql/expression/ExprOpcodeRegistry.java` | 新增 opcode 映射 |
| `fe/fe-core/.../sql/analyzer/IndexAnalyzer.java` | tantivy imp_lib 校验 |
| `fe/fe-core/.../common/InvertedIndexParams.java` | TANTIVY 枚举 |
| `fe/fe-core/.../common/Config.java` | `enable_experimental_gin` |

### BE

| 文件 | 改动 |
|------|------|
| `be/src/storage/index/inverted/tantivy_ffi/` | **新增** Rust FFI 项目 |
| `be/src/storage/index/inverted/tantivy/` | **新增** tantivy plugin/writer/reader |
| `be/src/storage/index/inverted/inverted_index_common.h` | TANTIVY 枚举 + 新 query type |
| `be/src/storage/index/inverted/inverted_plugin_factory.cpp` | 注册 tantivy plugin |
| `be/src/storage/rowset/segment_iterator.cpp` | 新 query type 处理 |
| `be/src/exprs/bm25_function.cpp` | **新增** BM25 标量函数 |
| `be/src/exprs/tokenize_function.cpp` | **新增** TOKENIZE 标量函数 |

### Thrift

| 文件 | 改动 |
|------|------|
| `gensrc/thrift/Exprs.thrift` | 新增 MATCH_PHRASE 等 opcode |
