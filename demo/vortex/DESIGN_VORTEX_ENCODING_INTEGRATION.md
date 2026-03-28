# Stella: 新一代编码算法在 StarRocks 中的探索与实践

> Author: Zhen Fan
> Date: 2026-03-11 ~ 2026-03-28
> Status: 实验完成 + 代码清理完成。FSST 成功（EQ 0.60x baseline hot cache）；ALP/FastLanes 已删除。

---

## 0. Executive Summary

本项目验证了三种学术编码算法（ALP、FSST、FastLanes）在 StarRocks Segment 中的端到端可行性，目标是实现"压缩态计算"——在编码域直接执行谓词过滤，减少不必要的解压和物化。

**最终结论**：

| 编码 | 数据类型 | 结论 | 关键数据 |
|------|---------|------|---------|
| FSST | VARCHAR | ✅ 成功 | EQ 查询 0.60x baseline hot cache（2M rows, 15ms vs 29ms）；50M rows 0.81x（17ms vs 21ms） |
| ALP | FLOAT/DOUBLE | ❌ 证伪 → 已删除 | 文件大 1.62-2.14x，scan 更慢 |
| FastLanes | INTEGER/DECIMAL | ❌ 证伪 → 已删除 | 文件大 3.34-17.55x，scan 更慢 |

**根因一句话**：StarRocks 的 `bitshuffle+LZ4` 是整页原子压缩，ALP/FastLanes 的 FOR+bitpack 产物对 LZ4 不友好，导致最终文件更大、IO 更多。FSST 成功是因为它替换的是 DICT/PLAIN（弱基线），而非 bitshuffle（强基线）。

**代码清理状态**：ALP/FastLanes 代码已于 2026-03-28 全部删除（11 个 BE 文件 + 3 个 SQL 测试目录 + 19 个编辑文件），607 个 commit squash 为 1 个。FE+BE build 通过，FSST E2E 5/5 通过。

---

## 1. Motivation & Background

### 1.1 问题陈述

StarRocks 当前的列编码方案在以下场景存在不足：

1. **VARCHAR 列（高基数+重复模式）**：DICT 编码对高基数字符串效果不佳，退化为 PLAIN 编码。对于 URL、日志路径、分类标签等有重复子串但高基数的字符串列，缺乏有效的压缩态操作手段。
2. **编码域计算的理论价值**：当查询选择性高（< 10% 数据匹配）时，如果能在压缩态执行谓词，可以跳过大量不必要的解压工作。

### 1.2 算法来源

三种编码算法均出自 CWI Amsterdam / TU Munich 研究组，论文公开、参考实现开源：

| 算法 | 论文 | 开源参考实现 | 许可证 |
|------|------|------------|--------|
| ALP | Adaptive Lossless floating-Point (CWI 2023) | [DuckDB ALP (C++)](https://github.com/duckdb/duckdb/tree/main/src/include/duckdb/storage/compression/alp) | Apache-2.0 |
| FSST | Fast Static Symbol Table (VLDB 2020) | [cwida/fsst (C)](https://github.com/cwida/fsst) | MIT |
| FastLanes | FastLanes (VLDB 2023) | [cwida/FastLanes (C++)](https://github.com/cwida/FastLanes) | MIT |

我们的实现基于这些论文和开源参考实现，用 C++ 编写，不依赖任何外部 Rust/Vortex 组件。

### 1.3 与 DuckDB / Vortex 的关系

**Vortex** 是一个文件格式项目（类似 Parquet），将上述算法组合进了 `.vortex` 文件格式中。本项目受 Vortex 启发，但**不使用 Vortex 的任何代码、API 或文件格式**。关系类型：灵感来源（conceptual inspiration），而非技术依赖。

**DuckDB** 在自身存储引擎中独立实现了 ALP 和 FSST（C++，来自同样的论文），与 Vortex 无关。本项目对标的是 DuckDB 内部存储的做法——将论文中的编码算法集成到 StarRocks 自身的存储引擎中。

|  | DuckDB 内部存储 | **本项目（StarRocks）** |
|--|---------------|----------------------|
| **做什么** | 在自身存储中使用 ALP/FSST | 在 Segment Page 中使用 ALP/FSST/FastLanes |
| **依赖 Vortex** | 否 | 否 |
| **算法来源** | 学术论文 + C++ 实现 | 学术论文 + C++ 实现 |

---

## 2. 架构分析：为什么 DuckDB 能获益而 StarRocks 不能

这是本项目最重要的发现：**同样的编码算法，在不同存储架构下效果截然不同。**

### 2.1 DuckDB 存储架构：轻量编码 → 直接落盘

```
DuckDB 数据页写入路径:
  原始值 → 类型特化编码（ALP/FSST/FastLanes）→ 直接落盘
                                              ↑ 无通用压缩层

DuckDB 数据页读取路径:
  磁盘 → 编码数据（可直接做编码域谓词）→ 按需解码
         ↑ 编码后数据可直接访问
```

DuckDB 的编码层**就是**压缩层。ALP 将 float64 编码为更窄的整数 + bitpack，数据直接落盘，读取时可以在编码域做谓词比较，不匹配的行无需解码。

### 2.2 StarRocks 存储架构：编码 → bitshuffle → LZ4 → 落盘

```
StarRocks 数据页写入路径:
  原始值 → 编码（BIT_SHUFFLE/DICT/...）→ 通用压缩（LZ4/ZSTD/...）→ 落盘
                                        ↑ 这一层是强基线

StarRocks 数据页读取路径:
  磁盘 → LZ4 解压 → 编码数据 → 解码 → 原始值
         ↑ 必须先经过 LZ4 解压才能看到编码数据
```

StarRocks 在编码之后还有一层通用压缩（bitshuffle + LZ4）。这意味着：
- 编码后的数据**不会直接落盘**，而是被 LZ4 再压缩一次
- 读取时**必须先 LZ4 解压**才能看到编码数据，编码域计算的前提（"直接访问编码数据"）被 LZ4 层阻挡

### 2.3 核心差异对比

| 维度 | DuckDB | StarRocks |
|------|--------|-----------|
| 编码后是否有通用压缩 | **无** — 编码即最终形式 | **有** — bitshuffle + LZ4 |
| 编码后数据可直接访问 | **是** — 可做编码域谓词 | **否** — 需先 LZ4 解压 |
| 编码的作用 | 同时承担压缩+编码域计算 | 仅编码，压缩由 LZ4 承担 |
| ALP/FastLanes 的 bitpack 价值 | 减小文件体积（因为没有后续压缩） | **被 LZ4 抵消**（LZ4 已经压缩过） |
| bitshuffle 作为对手 | 不存在（无此层） | **极强基线** — 对周期型/单调型/窄值域数据效果极好 |

### 2.4 结论

ALP/FastLanes 的核心收益前提——**编码后数据可直接访问，bitpack 直接减小文件体积**——在 StarRocks 中不成立。FOR+bitpack 的产物对 LZ4 不友好（打乱了 bit plane 的规律性），反而导致 LZ4 压缩后的文件**更大**。

FSST 成功的原因恰恰是它替换的对手不同：FSST 替换的是 DICT/PLAIN（弱基线），而非 bitshuffle（强基线）。FSST 编码后的数据经 LZ4 压缩仍然更小，且编码态等值比较在 LZ4 解压后即可执行。

---

## 3. 实验过程与结论

### 3.1 ALP（FLOAT/DOUBLE）— ❌ 不可行

**编码原理**：ALP 将浮点数 `v` 编码为整数 `v_enc = round(v × 10^e)`，其中 `e`（exponent）通过采样确定。编码后的整数用 FOR+bit-packing 压缩。少量无法精确编码的值存储在 exceptions 向量中。

**实现**：完整的 `ALPPageBuilder<T>` / `ALPPageDecoder<T>` + 压缩态谓词（kEQ/kNE/kGT/kGE/kLT/kLE）。

**Benchmark 结果（50M rows）**：

| 指标 | ALP | Baseline (BIT_SHUFFLE) | 比值 |
|------|-----|----------------------|------|
| 文件大小（random DOUBLE） | 1.62x 更大 | 基准 | ❌ |
| 文件大小（ROUND(x,2) DOUBLE） | 2.14x 更大 | 基准 | ❌ |
| Full scan 时间 | 0.99x | 基准 | ≈ 持平 |

> 注：Phase 8 优化后（移除 FOR/bitpack，改为 raw encoded integers + LZ4），ALP full scan 达到 parity（14ms vs 13ms baseline）。但文件仍然大 25-28%（int64 编码值同为 8 字节 + exception 开销），整体不可行。

**根因**：
1. FOR+bitpack 的 bit 交错布局打乱了 bitshuffle 的 bit-plane 规律性，LZ4 压缩后文件反而更大
2. 即使移除 FOR+bitpack 改用 raw integers，ALP 编码值（int64）与原始 float64 同为 8 字节，加上 exception 开销，文件大 25-28%
3. 热缓存下 page cache 消除 IO 差异（4GB << 45GB RAM），ALP 的编码域计算无法抵消更大的文件体积

### 3.2 FastLanes（INTEGER/DECIMAL）— ❌ 不可行

**编码原理**：FastLanes 是 SIMD 友好的 BitPacking 方案，将整数值减去最小值后按固定 bit 宽度紧凑存储。

**实现**：完整的 `FastLanesPageBuilder<T>` / `FastLanesPageDecoder<T>` + 压缩态谓词（kEQ/kNE/kGT/kGE/kLT/kLE）。DECIMAL32/64/128 通过 `delegate_type()` 自动映射到 INT/BIGINT/LARGEINT。

**Benchmark 结果（50M rows）**：

| 指标 | FastLanes | Baseline (BIT_SHUFFLE) | 比值 |
|------|-----------|----------------------|------|
| 文件大小 | 3.34-17.55x 更大 | 基准 | ❌ |
| Full scan 时间 | ≈ 持平（优化后） | 基准 | ≈ 持平 |

> 注：Phase 8 优化后（移除 FOR/bitpack，改为 raw integers + LZ4），FastLanes full scan 达到 parity（11ms vs 13ms baseline），文件大小回到 1.0x。但此时 FastLanes 实质上退化为与 BIT_SHUFFLE 等价的方案，失去了独立存在的意义。

**根因**：同 ALP——bitshuffle+LZ4 组合基线过强。bitshuffle 将同一 bit plane 聚集到一起，LZ4 的协同压缩效果极好，尤其对周期型、单调型、窄值域整数。FOR+bitpack 产物对 LZ4 不友好，文件远大于 baseline。

### 3.3 FSST（VARCHAR）— ✅ 成功

**编码原理**：FSST 构建一个最多 255 个符号的符号表（每个符号 1-8 字节），将输入字符串中的重复子串替换为 1 字节的符号码。

```
符号表（per-column，存储在 ColumnMetaPB.fsst_symbol_table 中，同列所有 page 共享）:
  "http://"      → 0x01
  "example.com"  → 0x02
  "/api/v"       → 0x03

编码: "http://example.com/api/v2" → [0x01][0x02][0x03]"2"
解码: 查表还原

压缩态等值比较（FSST 是确定性单射）:
  WHERE url = 'http://example.com/api/v2'
  → 用同一符号表编码查询常量: [0x01][0x02][0x03]"2"
  → 直接在编码数据上做字节序列比较
```

**适用条件**：高基数但有重复子串模式（URL、日志路径、分类标签），DICT 编码已退化为 PLAIN。

#### 性能优化过程

FSST 经历了从功能正确到性能达标的完整优化过程：

| 阶段 | 优化 | EQ 性能（50M rows） | 关键发现 |
|------|------|------------------|---------|
| 初始实现 | per-page symbol table | 2.5x 慢于 baseline | `init()` 反复反序列化 symbol table 占 64% CPU |
| per-column v1 | symbol table 提升到列级 ColumnMetaPB | ~1.2x | perf 显示 `encode_flat()` 成为新热点 |
| scan-path 优化 | 8-byte prefix inline 比较 + first-byte pre-filter + 编码谓词跨页缓存 | ~0.81x | FSST 自身函数不再出现在 top 热点 |
| Legacy 移除 | 删除所有 per-page 兼容代码 | 0.81x (17ms vs 21ms) | 最终状态 |

#### 最终 Benchmark（50M rows, per-column symbol table）

| Query Type | FSST | Baseline (DICT/PLAIN) | Ratio | 结论 |
|-----------|------|----------------------|-------|------|
| EQ point query | 17ms (16-19ms) | 21ms (20-28ms) | 0.81x | FSST 快 19% |
| Full scan COUNT(*) | 6.6ms | 6.6ms | 1.0x | 持平 |

#### Post-Cleanup Verification Benchmark（2M rows, 代码清理后）

代码清理后（ALP/FastLanes 全部删除），在 FSST-only build 上重新验证：

**EQ Benchmark（rigorous, 7 runs, drop caches, disable compaction）**

| Query Type | FSST | Baseline | Hot Ratio | 结论 |
|-----------|------|----------|-----------|------|
| EQ (1/2M selectivity) | cold=358ms, hot=15ms, **median=18ms** | cold=374ms, hot=29ms, **median=30ms** | **0.60x** | FSST EQ 快 40% (hot cache) |
| Full scan COUNT(LENGTH) | cold=487ms, hot=187ms, median=191ms | cold=410ms, hot=83ms, median=86ms | 2.22x | FSST full scan 慢（decode 开销） |
| LIKE (16.7% selectivity) | cold=472ms, hot=217ms, median=219ms | cold=404ms, hot=106ms, median=108ms | 2.03x | FSST LIKE 慢（不支持编码态 LIKE） |

Data: 2M rows, avg string length 241.8 bytes, FSST size 72.9MB vs Base 73.9MB (ratio 0.987)

**Scenario Benchmark（4 patterns, 1M rows each）**

| Pattern | Avg Len | FSST Size | Base Size | Ratio | Verdict |
|---------|---------|-----------|-----------|-------|---------|
| Log lines (95% template) | 183.9B | 44.6MB | 44.6MB | 1.000x | ~same |
| JSON records (90% schema) | 327.7B | 56.1MB | 51.5MB | 1.090x | Baseline wins |
| URLs (80% shared prefix) | 190.8B | 27.0MB | 25.6MB | 1.056x | Baseline wins |
| Random hex (no patterns) | 42.9B | 28.9MB | 42.4MB | **0.682x** | **FSST wins** |

Full scan performance (LIKE query, median of 3 runs):

| Pattern | FSST | Baseline | Ratio |
|---------|------|----------|-------|
| Log | 190ms | 191ms | 1.00x |
| JSON | 243ms | 133ms | 1.83x |
| URL | 181ms | 122ms | 1.48x |

**结论**：
1. FSST EQ point query 在代码清理后性能不变，hot cache 下 0.60x baseline（40% 提速）
2. FSST 在 random hex 场景下压缩比最好（0.682x），因为 baseline 使用 PLAIN 编码
3. FSST full scan 和 LIKE 比 baseline 慢 ~1.5-2.2x，因为需要额外 decode（baseline DICT 直接二进制比较）
4. FSST 的收益窗口明确：**高基数 + EQ 查询**，其他场景不应启用

#### perf 热点收敛证明

| 优化阶段 | Top-1 热点 | 占比 | 性质 |
|---------|-----------|------|------|
| 初始实现 | `FSSTPageDecoder::init()` | ~64% | FSST 自身函数（可优化）|
| per-column 后 | `encode_flat()` | 高 | FSST 自身函数（可优化）|
| scan-path 优化后 | `_next_batch_single_eq` | 中 | 通用 scan 框架函数 |
| 50M 最终状态 | Thrift/Pipeline/RuntimeProfile | 主导 | 框架基础设施（不可优化）|

**收敛结论**：50M 规模下 `perf report` 中 FSST 相关函数（`init`、`encode_flat`、`decode`、`set_symbol_table`）均已不在 top 热点中。剩余热点为 Thrift RPC 序列化、Pipeline 调度、RuntimeProfile 统计等框架层函数，属于所有编码共用的基础设施开销。**FSST 编码的优化链路已被榨干。**

#### EQ 收益原理

FSST EQ 的提速来自 **byte-touch reduction**（数据触达量下降），属于常数项优化：
1. 编码后数据更小 → 更少 IO/内存带宽
2. 非命中行在编码域直接判等，不做 full decode
3. 编码串更短 → `memcmp` 处理更少字节

#### 收益边界

| 场景 | 是否有收益 | 说明 |
|------|----------|------|
| 高基数 + 重复子串（URL/路径/标签）+ EQ | **有** | 0.81x，编码态等值比较有效 |
| 高基数 + 重复子串 + full scan | 持平 | decode 成本 ≈ baseline |
| 低基数（DICT 命中） | N/A | 不会选中 FSST |
| 随机字符串（UUID/hash） | **无** | FSST 压缩比 > 0.7，回退 PLAIN |
| LIKE/前缀匹配 | 不支持 | FSST 不保序，编码域 LIKE 不成立 |

---

## 4. 性能优化方法论（经验沉淀）

以下方法论从 FSST 全链路优化过程中提炼，适用于任何性能优化工作。

### 4.1 用 perf 工具定位热点，不猜测

每次优化必须先 `perf record` + `perf report` 定位热点函数，再针对性修改。

- perf 显示 `FSSTPageDecoder::init()` 占 64% → 做 per-column symbol table
- perf 显示 `encode_flat()` 成为新热点 → 做谓词编码跨页缓存
- perf 显示 FSST scan 函数已接近 0% CPU → 确认已无优化空间，收口

反例：不做 profiling 而猜测"LZ4 解压应该很慢"→ 实际 LZ4 不在热点中，移除反而可能造成 IO 回归。

### 4.2 遵循已有模式（per-column 参照 DICT 模式）

StarRocks 已有成熟的 per-column 元数据模式（DICT 编码的 dictionary 每列一份，所有 page 共享）。FSST symbol table 最初设计为 per-page 是设计失误——如果一开始就遵循 DICT 模式做 per-column，可以避免整个重构。

### 4.3 理解 runtime 调用时序再写代码

`parse_page()` 先调用 `decoder.init()`，然后 `ScalarColumnIterator` 才注入 symbol table。如果不了解这个时序就在 `init()` 中强制检查 symbol table，会导致运行时必然失败。

教训：修改 reader 链路前，必须先理清 `_read_data_page()` → `parse_page()` → `init()` → `_do_init_fsst_decoder()` 的完整调用链。

### 4.4 不移除非热点组件（LZ4 分析案例）

LZ4 解压在 profiling 中 0% CPU，移除它的理论收益为零，但风险是冷缓存/S3 场景下 page 体积增大导致 IO 回归。原则：**只优化热点路径，不动非热点路径**。

### 4.5 在目标规模 benchmark

5M 行和 50M 行的瓶颈分布不同。小规模下 FSST scan 函数可能占比显著，大规模下框架开销（Thrift/Pipeline/RuntimeProfile）会主导。结论必须基于目标规模的数据。

### 4.6 收敛标准：最终热点列表 + 不可优化证明

每个性能优化收尾时，必须给出目标规模下的 `perf report` 最终热点函数列表，并证明剩余热点均属于以下不可优化类别：
- **框架/基础设施函数**：Thrift RPC 序列化、Pipeline 调度、RuntimeProfile 统计等
- **OS/库函数**：`memcpy`、`malloc`、LZ4 解压等已高度优化的系统调用
- **非本编码路径的通用 scan 函数**：`next_batch`、`_do_scan` 等所有编码共用的框架函数

只有当目标编码自身的函数不再出现在 top-N 热点中，才能认为优化链路已被榨干。

---

## 5. FSST 技术详情

### 5.1 Page Format

```
┌──────────────────────────────────────┐
│ FSST Header:                         │
│   string_count (uint32)              │
├──────────────────────────────────────┤
│ Offsets Array:                       │
│   uint32[string_count + 1]           │
├──────────────────────────────────────┤
│ Encoded Strings:                     │
│   FSST-compressed bytes             │
└──────────────────────────────────────┘
```

Symbol table 不在 page body 中存储，统一存放在 `ColumnMetaPB.fsst_symbol_table`（field 37）中，每个 VARCHAR 列独立一份，iterator 初始化时注入 decoder，同列所有 page 共享。

### 5.2 编码选择策略

```cpp
// 在 StringColumnWriter::speculate_string_encoding() 中
// 1. 低基数（distinct values < 阈值）→ DICT_ENCODING（现有，效果最好）
// 2. 高基数 + enable_fsst_encoding + FSST 压缩比 < 0.7 → FSST_ENCODING
// 3. 其他 → PLAIN_ENCODING（现有）
```

### 5.3 压缩态谓词（kEQ/kNE）

FSST 编码是确定性单射函数：`encode_S(a) = encode_S(b)` iff `a = b`。等值比较在编码空间严格正确。

**不支持有序比较**：FSST 不保序，`<`, `>`, `ORDER BY` 必须解码后执行。

实现要点：
- 谓词常量的 FSST 编码结果缓存在 `_fsst_cached_encoded_pred` 中，跨页复用
- 8-byte prefix inline 比较 + first-byte pre-filter 减少 `memcmp` 调用
- `next_batch_with_filter`：decode-then-filter 模式

### 5.4 压缩态谓词管道

引入压缩态谓词后的过滤管道：

```
Zone Map Filter → Bloom Filter → Compressed Encoding Filter → Scan
```

使用 PredicateTree visitor 模式（仿 `BloomFilterSupportChecker` / `BloomFilterEvaluator`），支持 AND+OR 复合谓词：

```cpp
// CompressedEncodingSupportChecker: AND=|=, OR=all_of
// CompressedEncodingEvaluator: AND=交集, OR=并集（逐谓词独立评估）
```

> **OR 节点关键差异**：OR 中同列多个谓词必须逐个独立评估后 union，不能打包传入（compressed encoding 是行级精确过滤，多谓词串行 `&=` 为 AND 语义，不同于 bloom filter 的 page 级 "might contain" 语义）。

可观测指标：
- `CompressedEncodingFilter`: RuntimeProfile timer
- `CompressedEncodingFilterRows`: 被过滤的行数计数器

### 5.5 关键文件清单

| 文件 | 内容 |
|------|------|
| `be/src/storage/rowset/fsst_page.h` | FSSTPageBuilder + FSSTPageDecoder (header-only) |
| `be/src/util/fsst_encoding.h` | FSST 符号表构建、编码、解码算法 (header-only) |
| `be/src/storage/rowset/column_writer.cpp` | 构建 column-level symbol table，传入 Builder，写入 ColumnMetaPB |
| `be/src/storage/rowset/scalar_column_iterator.cpp` | 从 ColumnMetaPB 加载 symbol table → 每页 set_symbol_table() |
| `be/src/storage/rowset/column_reader.cpp` | fsst_symbol_table accessor |
| `be/src/storage/rowset/segment_iterator.cpp` | CompressedEncodingSupportChecker + CompressedEncodingEvaluator |
| `be/test/storage/rowset/fsst_page_test.cpp` | per-column 模式测试（16 个用例） |

---

## 6. 总结与后续

### 6.1 达成成果

| 目标 | 状态 |
|------|------|
| FSST 编码全链路集成 (FE→Thrift→Proto→BE) | ✅ |
| FSST EQ 查询稳定优于 baseline（0.81x, 50M rows） | ✅ |
| 压缩态谓词管道 + AND/OR visitor | ✅ |
| 可观测指标 (CompressedEncodingFilterRows) | ✅ |
| 可扩展编码框架 (table property → column writer → page builder/decoder) | ✅ |
| 查询结果正确性验证（encoded vs baseline 100% 一致） | ✅ |
| DuckDB vs StarRocks 架构差异根因分析 | ✅ |
| 性能优化方法论沉淀（perf-driven, 收敛标准） | ✅ |

### 6.2 核心教训

1. **"bit_width 更小"不等于"最终文件更小"**：在 StarRocks 里真正的对手不是裸整数，而是强基线 `bitshuffle+LZ4`。对周期型、单调型、窄值域整数，它的协同压缩效果远强于预期。

2. **理解存储架构再选优化方向**：DuckDB 的编码层就是压缩层（无通用压缩），因此 ALP/FastLanes 的 bitpack 直接减小文件。StarRocks 有 bitshuffle+LZ4 强基线，bitpack 产物反而对 LZ4 不友好。同样的算法在不同架构下效果截然不同。

3. **替换弱基线才有收益**：FSST 成功是因为替换的是 DICT/PLAIN（弱基线），ALP/FastLanes 失败是因为试图替换 bitshuffle（强基线）。

4. **benchmark 的公平性比单次结果更重要**：同 schema、多分布、profile 级别的对比是必要的。不同 schema、不同 rowset 状态会把结论带偏。

### 6.3 代码清理（✅ 已完成 2026-03-28）

ALP/FastLanes 代码已全部删除，607 个 commit squash 为 1 个干净 commit。

**已删除的文件（11 个 BE + 3 个 SQL 测试目录）**：

| 文件 | 内容 |
|------|------|
| `be/src/storage/rowset/alp_page.h` | ALP PageBuilder/Decoder |
| `be/src/util/alp_encoding.h` | ALP 核心算法 |
| `be/src/storage/rowset/fastlanes_page.h` | FastLanes PageBuilder/Decoder |
| `be/src/util/fastlanes_encoding.h` | FastLanes 核心算法 |
| `be/test/util/alp_encoding_test.cpp` | ALP 编码测试 |
| `be/test/util/fastlanes_encoding_test.cpp` | FastLanes 编码测试 |
| `be/test/storage/rowset/alp_page_test.cpp` | ALP page 测试 |
| `be/test/storage/rowset/alp_predicate_diff_test.cpp` | ALP 谓词测试 |
| `be/test/storage/rowset/alp_benchmark.cpp` | ALP 性能测试 |
| `be/test/storage/rowset/alp_segment_integration_test.cpp` | ALP 集成测试 |
| `be/test/storage/rowset/fastlanes_page_test.cpp` | FastLanes page 测试 |
| `test/sql/test_vortex_alp/` | ALP SQL E2E 测试 |
| `test/sql/test_vortex_fastlanes/` | FastLanes SQL E2E 测试 |
| `test/sql/test_vortex_mixed/` | 混合编码 SQL E2E 测试 |

**已编辑的文件（19 个）**：Proto/Thrift（3）、FE Java（6）、BE C++（10）— 删除所有 ALP/FastLanes 引用，保留 FSST。

**保留的 Protobuf/Thrift 设计决策**：
- `EncodingTypePB::ALP_ENCODING` (8) 和 `FASTLANES_ENCODING` (10) 的枚举值已删除，但对应 field number 不重新分配
- Thrift field 16 (`enable_alp_encoding`) 和 18 (`enable_fastlanes_encoding`) 已删除，field number 不重新分配

**验证**：FE+BE build 通过（450s），FSST E2E 5/5 通过，EQ benchmark 性能不变（0.60x hot cache）。

---

## Appendix A: FSST 编码选择条件

| StarRocks Type | 现有默认编码 | FSST 选择条件 |
|---------------|------------|-------------|
| VARCHAR | DICT/PLAIN | `enable_fsst_encoding = true` + 高基数（DICT 阈值失败）+ FSST 压缩比 < 0.7 |
| CHAR | DICT/PLAIN | 同上 |

## Appendix B: 表属性全链路路径（FSST）

| 层 | 文件 | 修改内容 |
|---|------|---------|
| FE | `PropertyAnalyzer.java` | `PROPERTIES_ENABLE_FSST_ENCODING` 常量 |
| FE | `TableProperty.java` | 字段 + copy ctor + builder + getter |
| FE | `OlapTable.java` | getter + setter |
| FE | `OlapTableFactory.java` | 属性解析 |
| FE | `SchemaInfo.java` | 字段 + builder + `toTabletSchema()` |
| FE | `TabletTaskExecutor.java` | builder 调用 |
| Thrift | `AgentService.thrift` | `TTabletSchema` field 17 |
| Proto | `tablet_schema.proto` | `TabletSchemaPB` field 18 |
| BE | `metadata_util.cpp` | Thrift→Proto 传播（所有 overload） |
| BE | `tablet_schema.h/cpp` | 成员 + getter + proto 读写 + copy |
| BE | `column_writer.h` | `ColumnWriterOptions` 新增 field |
| BE | `segment_writer.cpp` | 从 `_tablet_schema` 传递 |
