# Vortex 深度剖析及与 StarRocks 结合的可能性

> 分析日期：2026-03-10

## 一、Vortex 概述

Vortex 是由 SpiralDB 发起、现已成为 Linux Foundation (LF AI & Data) 子项目的**下一代列式文件格式**。它的核心设计理念与传统 Parquet 有本质不同：**数据在压缩态下可以直接参与计算，而不是先解压再计算**。

官方宣称的性能指标：
- **100x** 更快的随机访问读取（vs. Parquet）
- **10-20x** 更快的扫描
- **5x** 更快的写入
- 压缩比与 Parquet 相近

项目状态：文件格式自 v0.36.0 起已稳定，保证向后兼容。

参考链接：
- [GitHub](https://github.com/spiraldb/vortex)（已迁移到 vortex-data/vortex）
- [文档](https://docs.vortex.dev/)
- [DuckDB Vortex Extension Blog](https://duckdb.org/2026/01/23/duckdb-vortex-extension)
- [性能基准](https://bench.vortex.dev)

---

## 二、相对于传统 Column 方案（Parquet）的优势

### 2.1 核心差异：压缩态计算 (Compressed Compute)

要理解 Vortex 的真正优势，需要先准确理解 Parquet 已有的 data skip 能力，避免高估差异。

#### Parquet 已有的多级过滤能力

Parquet 的压缩和过滤粒度是 **Page（~1MB）**，不是 Row Group：

```
Row Group (~128MB, ~百万行)
├── Column Chunk (Column A)
│   ├── Page 0 (~1MB, ~数万行)  ← 压缩/解压的最小粒度
│   ├── Page 1 (~1MB)
│   └── Page 2 (~1MB)
└── Column Index              ← Page 级别 min/max 统计（Parquet v2+）
```

Parquet 可以在两个级别做 data skip：
1. **Row Group 级别**：每个 Row Group 有 min/max 统计，可跳过整个 Row Group
2. **Page 级别**：通过 Column Index（Parquet v2+ 特性），每个 Page 有独立的 min/max 统计，可以跳过单个 Page

因此 Parquet 并**不需要**解压整个 Row Group——如果 Column Index 有效，只有 min/max 范围与查询条件重叠的少量 Page 会被读取和解压。

#### Vortex 的真正优势所在

差异缩小到**单个无法被跳过的最小粒度单元内部**：

```
场景：WHERE price > 99900，某个数据块 min=500, max=100000，无法被统计信息跳过

Parquet Page (~1MB, ~数万行):
  → 解压整个 Page（~1MB）
  → 逐行执行 price > 99900
  → 可能只有几十行匹配，但解压工作已全部完成

Vortex Zone (~8K 行):
  → 粒度本身更细（8K 行 vs 数万行），更多 zone 可被统计跳过
  → 对无法跳过的 zone，在 ALP 压缩态直接执行 price > 99900
  → 生成匹配行 bitmap，仅解压匹配的行
```

#### 精确的过滤粒度对比

| 层级 | Parquet | Vortex | 说明 |
|------|---------|--------|------|
| Row Group 级 (~百万行) | min/max 跳过 | - | 两者能力相当 |
| Page / Zone 级 | Column Index 跳过（~1MB / ~数万行） | ZonedLayout 跳过（~8K 行） | Vortex 粒度更细约 4-8x |
| **最小粒度内部** | **必须解压整个 Page 后过滤** | **压缩态过滤，不解压即可判断** | 这是核心架构差异 |

#### Vortex 优势大小取决于数据分布

```
情况 A — 目标值聚集（如排序列、日期列）：
  Parquet Page 级跳过已经很有效，大部分 Page 可被跳过
  → 但 Vortex 仍有 20-40% 优势（TPC-H 实测）
  → 原因：(1) Vortex Zone 粒度 8K 行 vs Parquet Page 数万行，窄范围查询跳过更精确
        (2) 无法跳过的 Zone 内部仍可压缩态过滤
        (3) 文件更小（16-33%），IO 量本身就少

情况 B — 目标值均匀分布（如随机浮点列）：
  几乎每个 Page 的 min/max 范围都覆盖目标值，Page 级跳过失效
  Parquet：每个 Page 都要解压 → 解压量 ≈ 全部数据
  Vortex：压缩态过滤仍然有效，只解压匹配行
  → Vortex 优势显著（实测 30-50%）

情况 C — 低选择性（返回大部分数据）：
  无论哪种格式，大部分数据终究要解压
  → Vortex 无优势，全表聚合可能因编码开销略慢（实测 -13%）
```

核心结论：
```
Vortex 的价值 = 更细粒度的统计跳过 + 压缩态计算 + 更小的文件体积

优势最大化条件：高选择性 + 目标值均匀分布（统计跳过失效时）+ 多谓词组合
优势最小化条件：低选择性的全表聚合（但仍有文件体积优势）

重要发现（TPC-H 实测）：即使是情况 A（有序列），Vortex 仍有 20-40% 优势，
之前预估的"10-20%"过于保守。
```

### 2.2 类型特化编码（Type-Specific Encodings）

| 编码 | 适用类型 | 核心原理 | 压缩态支持的操作 |
|------|---------|---------|----------------|
| **ALP** (Adaptive Lossless floating-Point) | Float/Double | 自适应无损浮点压缩 | `>`, `<`, `=`, 算术比较 |
| **FSST** (Fast Static Symbol Table) | String/VARCHAR | 符号表压缩，保持字典序 | 等值比较、前缀匹配、GROUP BY、ORDER BY |
| **FastLanes** | Integer/Vector | SIMD 友好的向量化编解码 | bitpacking、delta、RLE |
| **BtrBlocks** | 通用 | 级联轻量级压缩 | 高压缩比 + 快速解压 |
| **RunEnd** | 低基数列 | 游程编码 | 压缩态等值 |
| **ZStd/PCodec** | 紧凑策略 | 最大化压缩比 | 需解压 |

#### ALP 编码详解（浮点数）

- 来源论文：[ALP](https://ir.cwi.nl/pub/33334/33334.pdf)
- 自适应无损浮点压缩
- 支持压缩态算术比较（`>`, `<`, `=`）
- 适用于价格、测量值、传感器数据等列

#### FSST 编码详解（字符串）

- 来源论文：[FSST (VLDB 2020)](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf)
- 工作原理：

```
原始字符串: "http://example.com/user/123"
符号表:
  - "http://" → 0x01
  - "example.com" → 0x02
  - "user/" → 0x03

压缩后: [0x01][0x02][0x03]"123"
```

- 符号表最大 255 个符号（1 字节编码），符号长度 1-8 字节
- **保持字典序**：压缩后字符串可直接排序/分组

| 操作 | 压缩态支持 | 实现方式 |
|------|-----------|---------|
| 等值比较 `=` | 完全支持 | 直接比较编码后的字节序列 |
| 前缀匹配 `LIKE 'prefix%'` | 完全支持 | 在压缩态执行前缀比较 |
| GROUP BY | 完全支持 | 保持字典序，可直接分组 |
| ORDER BY | 完全支持 | 保持字典序，可直接排序 |
| 子串搜索 `LIKE '%pattern%'` | 需编码 | 先编码搜索模式，再匹配 |

最佳数据模式：重复子串（URLs、文件路径）、半结构化文本、分类字符串（`category_001`）、固定格式日志。

**FSST 局限性**：符号表固定 255 个符号（1 字节编码），对以下场景效果有限：
- **高基数无规律字符串**：UUID、随机 hash、用户自由文本等缺乏重复子串，符号表命中率低，压缩率接近 1:1
- **极短字符串**：1-2 字节的字符串没有可提取的子串模式
- **子串搜索 `LIKE '%pattern%'`**：需要先将搜索模式编码为符号序列，如果 pattern 跨越符号边界则需回退到解压比较

#### FastLanes 编码详解（向量/GPU）

- 来源论文：[FastLanes (VLDB 2023)](https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf)
- SIMD 友好解码
- 支持内存到 GPU 直传
- 设计用于 ML 嵌入向量

### 2.3 级联压缩 (Cascading Compression)

Vortex 支持**编码嵌套**，例如：
```
原始 Float64 → ALP 编码 → FastLanes BitPacking → 存储
```
这在 Parquet 中不存在——Parquet 的编码方案是固定、不可组合的。

### 2.4 可扩展架构

Vortex 模仿 Apache DataFusion 的可扩展设计：
- **可插拔编码系统**：可以自定义新的编码方式
- **可插拔类型系统**：Logical Type 与 Physical Encoding 严格分离
- **可插拔 Layout 策略**：Writer 决定数据如何组织，而不是格式规范硬编码
- **可插拔压缩策略**：BtrBlocks（性能优先） vs. Compact（压缩比优先）

### 2.5 零拷贝 Arrow 兼容

Vortex 的内存模型基于 Apache Arrow，实现零拷贝互转，这使得它可以无缝集成到任何 Arrow 生态的系统中。

### 2.6 优势与代价总结

| 维度 | Parquet | Vortex |
|------|---------|--------|
| 谓词执行 | 必须先解压 | 压缩态执行 |
| 编码方案 | 固定（Plain/Dict/RLE/Delta） | 可插拔、可嵌套 |
| 文件组织 | 固定 Row Group → Column Chunk → Page | Writer 自定义 Layout 树 |
| 浮点压缩 | 通用压缩 | ALP 类型特化 |
| 字符串压缩 | Dict + 通用压缩 | FSST 保持字典序 |
| GPU 支持 | 无 | FastLanes 直通 |
| 扩展性 | 需修改规范 | 插件式扩展 |
| **写入速度** | **更快** | **慢 1.8-2.6x**（见下文） |

#### 写入性能代价

Vortex 的类型特化编码（ALP、FSST）在写入时需要额外的分析和编码步骤，导致写入速度显著慢于 Parquet。TPC-H SF50 实测数据：

| 表 | Parquet 写入 | Vortex 写入 | 倍率 |
|---|---|---|---|
| lineitem (300M 行, 最大表) | 76.6s | 198.2s | **2.6x 慢** |
| orders (75M 行) | 16.8s | 29.7s | **1.8x 慢** |
| partsupp (40M 行) | 10.7s | 18.2s | **1.7x 慢** |

**对 StarRocks 的影响**：
- **实时写入 (Realtime Ingestion)**：Compaction 过程中需要重新编码数据。Vortex 编码的列会使 Compaction 耗时增加 1.7-2.6x，可能影响写入吞吐量。
- **批量导入 (Bulk Load)**：影响较小，导入任务本身是 IO 密集型，编码开销可被并行化摊平。
- **缓解策略**：仅对读密集列使用 Vortex 编码，写密集列保持原生编码。或在 Compaction 时异步编码。

---

## 三、Vortex 文件的 Segment 结构

### 3.1 文件整体结构

```
┌──────────────────────────────────────────────────┐
│  Magic Number: 'VTXF' (4 bytes)                  │
├──────────────────────────────────────────────────┤
│                                                  │
│  Segments (Binary Data Blocks)                   │
│  ┌────────────────────────────────┐              │
│  │ Segment 0: Column A, Chunk 0  │              │
│  ├────────────────────────────────┤              │
│  │ Segment 1: Column A, Chunk 1  │              │
│  ├────────────────────────────────┤              │
│  │ Segment 2: Column B, Chunk 0  │              │
│  ├────────────────────────────────┤              │
│  │ ...                           │              │
│  └────────────────────────────────┘              │
│  (optional inter-segment padding)                │
│                                                  │
├──────────────────────────────────────────────────┤
│  Postscript Data                                 │
│  ┌────────────────────────────────────────────┐  │
│  │ 1. DType Segment (逻辑 Schema)             │  │
│  │ 2. Layout Segment (Layout 树的根节点)       │  │
│  │ 3. Statistics Segment (文件级统计信息)       │  │
│  │ 4. Footer Segment (Segment Map + 配置)      │  │
│  └────────────────────────────────────────────┘  │
├──────────────────────────────────────────────────┤
│  Version Tag (u16)                               │
│  Postscript Length (u16)                         │
│  Magic Number: 'VTXF' (4 bytes)                  │
└──────────────────────────────────────────────────┘
```

### 3.2 Layout 树（核心创新）

默认的 Layout 策略形成如下树结构：

```
StructLayout (按列分区)
├── Column A
│   └── ZonedLayout (每 8K 行存储 pruning 统计)
│       └── ChunkedLayout (每 chunk ≈ 2MB 未压缩)
│           └── CompressorLayout (应用压缩策略)
│               └── BufferedLayout (合并到 ≤ 1MB 压缩 chunks)
│                   └── FlatLayout (序列化单个 Array)
├── Column B
│   └── ZonedLayout
│       └── ChunkedLayout
│           └── ...
└── Column C
    └── ...
```

**内置 Layout 类型**：

| Layout | 作用 | 类比 Parquet |
|--------|------|------------|
| **StructLayout** | 按列分区，列裁剪 | Column Chunks |
| **ZonedLayout** | 每 8K 行存储 min/max 统计，用于 Zone Pruning | Row Group Statistics |
| **ChunkedLayout** | 按 2MB 未压缩数据切分 chunk | Pages |
| **CompressorLayout** | 选择并应用最佳编码 | Page Encoding |
| **BufferedLayout** | 聚合小 chunks 以优化 I/O（≤ 1MB） | - (Parquet 无此层) |
| **FlatLayout** | 持有单个序列化的 Vortex Array | - |
| **DictionaryLayout** | 共享字典，子 Layout 存储索引 | Dictionary Encoding |

**关键设计决策**：Layout 策略由 Writer 决定，不是格式规范硬编码。用户可以自定义 Layout 来模拟 Parquet Row Group 或其他任意组织方式。

### 3.3 Segment 的含义

在 Vortex 中，**Segment** 是一个**惰性加载的二进制数据块**，由 `SegmentId(u32)` 标识。它是 Layout 树叶子节点实际存储的数据。

```rust
struct SegmentId(u32);  // 简单的 u32 标识符
```

Segment 不是一个独立的组织单位（不同于 StarRocks 的 Segment），而更类似于一个**可寻址的 byte range**。

Footer 中的 **Segment Map**（字典编码）记录每个 SegmentId 对应的：
- 文件内 byte offset
- 压缩方案
- 加密方案
- 对齐要求

### 3.4 与 StarRocks Segment 的术语对比

| 概念 | StarRocks | Vortex |
|------|-----------|--------|
| 文件单位 | Segment File（一个独立的数据文件） | Vortex File（.vortex 文件） |
| 列存储 | ColumnReader/ColumnWriter per column | StructLayout 按列分区 |
| 数据块 | Page（在 Column 内分页） | Segment（可寻址 byte range） |
| 统计信息 | Zone Map Index | ZonedLayout（每 8K 行） |
| 索引 | Short Key / Bloom Filter / Bitmap | Layout 树本身 + Zone 统计 |
| 元数据 | SegmentFooterPB (Protobuf) | Postscript (FlatBuffers) |

### 3.5 与 Parquet 的结构对比

```
Parquet:                          Vortex:
┌─────────────────┐              ┌─────────────────┐
│ Magic: PAR1      │              │ Magic: VTXF      │
├─────────────────┤              ├─────────────────┤
│ Row Group 0      │              │ Segments         │
│  ├─ Col A Page  │              │  (任意 byte 块)   │
│  ├─ Col B Page  │              │  由 Layout 树     │
│  └─ Col C Page  │              │  组织和引用       │
├─────────────────┤              ├─────────────────┤
│ Row Group 1      │              │ Postscript       │
│  ├─ Col A Page  │              │  ├─ DType        │
│  └─ ...         │              │  ├─ Layout Root  │
├─────────────────┤              │  ├─ Statistics   │
│ Footer           │              │  └─ Footer       │
│  ├─ Schema      │              ├─────────────────┤
│  ├─ Row Group   │              │ Version + Length  │
│  │   Metadata   │              │ Magic: VTXF      │
│  └─ Col Metadata│              └─────────────────┘
├─────────────────┤
│ Footer Length    │              关键区别：
│ Magic: PAR1      │              - Layout 是树结构，不是固定层级
└─────────────────┘              - Writer 决定组织方式
                                 - 支持嵌套编码
```

---

## 四、与 StarRocks 结合的创新点

### 4.1 存算分离架构中的网络 IO 优化

StarRocks 的 shared-data 模式中，数据存储在 S3/OSS 上：

```
当前 StarRocks (自有格式):
  S3 → 传输完整 Segment → BE 解压 → 过滤 → 结果

引入 Vortex 后:
  S3 → 传输压缩 Segment → BE 压缩态过滤 → 仅解压匹配数据 → 结果

对高选择性查询，IO 减少 50-90%
```

### 4.2 向量检索 / AI 工作负载

StarRocks 已有 AI Functions。结合 Vortex 的 FastLanes 编码：
- Embedding 向量可以用 FastLanes 编码存储
- 支持 S3 → GPU 显存直通（通过 RDMA/GDS）
- 省去 CPU 中转环节

```
传统路径：S3 → CPU 内存 → 解压 → GPU 显存
Vortex + FastLanes：S3 → GPU 显存（RDMA/GDS 直通）→ GPU 端解压
```

### 4.3 冷数据分层查询加速

StarRocks 支持数据分层（热/温/冷）。冷数据存储在对象存储上：
- 使用 Vortex 编码存储冷数据
- 冷查询无需预热缓存，压缩态计算直接减少 IO
- Zone Map 统计信息实现高效 pruning

### 4.4 外表查询（Data Lake）

StarRocks 的 Connector 框架已支持读取 Parquet/ORC：
- 新增 Vortex Connector，支持读取 `.vortex` 文件
- 利用 Vortex 的谓词下推能力在压缩态过滤
- DuckDB 已有 Vortex Extension 可参考

### 4.5 收益传导效应（TPC-H 实测发现）

TPC-H SF50 测试揭示了一个此前未充分预期的现象：**Vortex 在 Scan 阶段减少的数据量会传导到下游 Join/Sort/Agg 算子，使整个查询 pipeline 受益**。

```
传统理解（错误）：
  Scan 加速 → 但 Join 是瓶颈 → 整体收益有限

实际情况（TPC-H 实测）：
  Scan 压缩态过滤 → 进入 Join 的数据量减少 50-90%
  → Hash Table 更小 → 更少的 probe → Join 也加速
  → Sort 数据量减少 → 排序也加速
  → 整个 Pipeline 加速 20-48%
```

典型例证：
- **Q2**（5 表 Join）：原以为 Join 是瓶颈，实际加速 **1.95x**。原因是 `p_type LIKE '%BRASS'` + `r_name='EUROPE'` 等字符串谓词在 Scan 阶段就通过 FSST 压缩态过滤，大幅减少了进入 Join 的行数。
- **Q19**（复合 OR 谓词）：加速 **3.05x**。多组字符串等值匹配是 FSST 的最优场景。

### 4.6 创新点总结

| 场景 | Vortex 优势 | StarRocks 应用 | TPC-H 实测收益 |
|------|------------|----------------|---------------|
| 存算分离 | 减少网络传输 | 谓词下推到存储层 | 文件小 16% |
| 冷数据查询 | 压缩态计算 | 减少从 S3 的读取量 | 查询快 20-67% |
| 向量检索 | FastLanes 编码 | AI/ML 工作负载 | 待测试 |
| 点查/范围查 | 最小化解压 | 高并发查询场景 | 快 25-34% |
| Data Lake | 新一代文件格式 | Connector 生态扩展 | - |
| **Join 密集查询** | **收益传导** | **多表关联分析** | **快 20-48%** |

---

## 五、混合 Segment + Vortex 结构的可行性分析

**提案：在 StarRocks 的 Segment 中，针对特定 DataType 的列使用 Vortex 的编码和 API，而非整表存储为 Vortex 文件。**

### 5.1 理论上完全可行，原因如下

#### (1) StarRocks Segment 已经是按列独立存储的

StarRocks 的 Segment 结构：
```
Segment File:
├── Column 0 Data (encoding: dict/plain/bitshuffle/...)
├── Column 1 Data (encoding: dict/plain/bitshuffle/...)
├── Column 2 Data (encoding: ...)
├── ...
├── Short Key Index
├── Column Indexes (ordinal, zone map, bitmap, bloom filter)
└── Segment Footer (SegmentFooterPB)
```

每个列的数据是**独立编码、独立存储**的。`ColumnReader`/`ColumnWriter` 是独立的接口。这意味着你可以**在列级别**替换编码方案，而不影响其他列。

#### (2) Vortex 的编码是可独立使用的

Vortex 的架构设计中，逻辑层和物理层严格分离。它的编码（ALP、FSST、FastLanes）是独立的 Rust crate：

```
encodings/
├── alp/        # 可独立使用
├── fsst/       # 可独立使用
├── fastlanes/  # 可独立使用 (bitpacked, delta, rle)
├── runend/     # 可独立使用
├── zstd/       # 可独立使用
├── bytebool/
├── datetime-parts/
├── decimal-byte-parts/
├── pco/        # PCodec
├── sequence/
├── sparse/
└── zigzag/
```

这些编码不依赖 Vortex 文件格式本身，可以被提取出来在任意系统中使用。

#### (3) 具体集成方案

```
StarRocks Segment (混合结构):
├── Column "id" (BIGINT)         → StarRocks 原生编码 (bitshuffle)
├── Column "name" (VARCHAR)      → Vortex FSST 编码    ← 替换
├── Column "price" (DOUBLE)      → Vortex ALP 编码     ← 替换
├── Column "status" (INT)        → StarRocks 原生编码 (dict)
├── Column "embed" (ARRAY<FLOAT>) → Vortex FastLanes   ← 替换
├── Short Key Index              → StarRocks 原生
├── Zone Map Index               → StarRocks 原生 (可结合 Vortex ZonedLayout 统计)
└── Segment Footer               → 扩展 SegmentFooterPB，增加 vortex_encoding 字段
```

#### (4) 实现路径

**Step 1：编码层集成**（C++ binding）
```cpp
// 新增 VortexColumnEncoder / VortexColumnDecoder
// 将 Vortex 的 ALP/FSST/FastLanes 编码通过 C API 或 FFI 暴露给 StarRocks BE

class VortexALPColumnWriter : public ColumnWriter {
    // 使用 Vortex ALP 编码写入 Float/Double 列
    Status append(const Column& column) override;
    Status finish() override;
};

class VortexALPColumnReader : public ColumnReader {
    // 支持压缩态谓词计算
    Status evaluate_predicate(const Predicate& pred, roaring::Roaring* result);
    // 惰性解压
    Status read_page(const PagePointer& pp, Column* column);
};
```

**Step 2：谓词下推到编码层**
```cpp
// 当前 StarRocks 的谓词执行流程：
//   ColumnReader::read_page() → 解压 → Chunk → 执行 Predicate

// 混合后的流程（Vortex 列）：
//   VortexColumnReader::evaluate_predicate_compressed() → bitmap
//   → 仅对匹配行调用 read_page() 解压
```

**Step 3：元数据扩展**
```protobuf
// 在 SegmentFooterPB 中扩展
message ColumnMetaPB {
    // 现有字段...
    optional VortexEncodingType vortex_encoding = 100;  // ALP, FSST, FASTLANES
    optional bytes vortex_encoding_metadata = 101;       // 编码特定元数据（如 FSST 符号表）
}
```

### 5.2 关键技术挑战

| 挑战 | 难度 | 说明 |
|------|------|------|
| **C++/Rust FFI** | 中 | Vortex 是 Rust 实现，需要通过 FFI 桥接到 StarRocks C++ BE。可选方案：(a) 通过 `cbindgen` 导出 C API；(b) 用 C++ 重新实现核心编码算法（ALP/FSST 论文公开，算法不复杂） |
| **内存模型对齐** | 中 | Vortex 基于 Arrow 内存布局，StarRocks 使用自己的 Column/Chunk 抽象。需要转换层 |
| **压缩态谓词接口** | 高 | 需要设计新的 ColumnReader 接口支持压缩态谓词计算 |
| **Schema Evolution** | 低 | 编码变更可以通过列级别的 schema change 处理 |
| **向后兼容** | 低 | 新编码只用于新写入的 Segment，旧 Segment 不受影响 |

### 5.3 为什么混合方案比全量替换更合理

1. **渐进式采用**：不需要迁移存量数据，新写入的列可以逐步使用 Vortex 编码
2. **按需优化**：只有适合的列才使用 Vortex 编码（Float 用 ALP，String 用 FSST），其他列保持现有高效编码
3. **保持 StarRocks 存储引擎优势**：Short Key Index、Bloom Filter、Bitmap Index 等能力不受影响
4. **降低风险**：Segment 的整体结构不变，只是编码层的替换，回退简单
5. **StarRocks 的 Column 抽象已经支持**：`ColumnReader`/`ColumnWriter` 是接口化的，天然支持插入新的编码实现

### 5.4 最适合用 Vortex 编码的 StarRocks 列类型

| StarRocks 类型 | 推荐 Vortex 编码 | 收益 |
|---------------|-----------------|------|
| DOUBLE/FLOAT | ALP | 压缩态浮点比较，高选择性查询快 20-40% |
| VARCHAR (有重复模式) | FSST | 压缩态字符串等值/前缀匹配 |
| ARRAY\<FLOAT\> (Embedding) | FastLanes | GPU 直通，SIMD 友好解码 |
| BIGINT (高基数) | FastLanes BitPacking | SIMD 友好的整数压缩 |

**不推荐使用 Vortex 编码的列**：低基数 INT（StarRocks 原生 Dict 编码已经很高效）、BOOLEAN。

**待评估的列类型**：DATE/DATETIME — 此前认为"原生编码已经足够好"，但 TPC-H 实测表明日期范围过滤是许多查询加速的关键贡献者（Q3/Q6/Q10/Q12 均包含日期谓词且加速 27-40%）。Vortex 更细的 Zone 粒度（8K 行）对时间序列数据的窄范围查询可能有额外收益，需进一步隔离测试。

---

## 六、Benchmark 验证结果

我们进行了两轮测试验证，详细数据见独立报告，此处仅列出关键结论。

- **TPC-H SF50 标准评测**（主要参考）：[`tpch/TPCH_BENCHMARK.md`](./tpch/TPCH_BENCHMARK.md)
- **自造数据微基准**（辅助参考）：[`simple/BENCHMARK_REPORT.md`](./simple/BENCHMARK_REPORT.md)

测试环境：macOS Darwin 22.6.0, Apple M2 Pro (10 核), 32 GB RAM, DuckDB v1.5.0 + Vortex Extension。

### 关键结论

1. **TPC-H 几何平均快 38%**（21/22 条查询 Vortex 胜出），超过 Vortex 官方公布的 18%（[bench.vortex.dev](https://bench.vortex.dev)，SF100）
2. **文件更小**：TPC-H 数据 Vortex 比 Parquet 小 16%；自造数据（浮点+字符串密集）小 33%
3. **字符串过滤是最大收益点**：包含多字符串谓词的查询一致加速 1.3-3.0x（FSST 编码），最佳 Q19 达 3.05x
4. **收益可传导到 Join/Sort/Agg**：Scan 阶段减少的数据量间接加速整个 pipeline（见 4.5 节），Join 密集查询也有 1.3-2.0x 加速
5. **日期谓词也有显著收益**：包含日期过滤的查询（Q3/Q6/Q10/Q12）加速 27-40%，此前低估
6. **退化场景**：EXISTS 相关子查询（Q4, -16%）、全表 count（-59%）、全表聚合（-13%）
7. **写入代价**：Vortex 写入慢 1.7-2.6x，需在集成时权衡读写比

---

## 七、总结

| 维度 | 结论 |
|------|------|
| **Vortex 核心价值** | 压缩态计算 + 更小文件 + 更细统计粒度 |
| **TPC-H SF50 实测** | 几何平均快 **38%**，21/22 条查询胜出 |
| **最大收益场景** | 字符串过滤（FSST, 最高 3x） > 浮点过滤（ALP, 1.3-1.8x） > Join 密集（收益传导, 1.3-2.0x） |
| **最大代价** | 写入慢 1.7-2.6x，全表聚合无优势 |
| **混合集成可行性** | **完全可行** — StarRocks 按列独立编码存储，Vortex 编码可独立使用 |
| **推荐路径** | 列级别编码替换，优先 FSST（字符串）和 ALP（浮点），通过 C++ 重新实现核心算法 |
| **最大挑战** | 压缩态谓词下推接口设计 + Compaction 写入性能影响 |

混合方案是比全量替换更务实的选择——它允许你在保持 StarRocks 存储引擎核心优势的同时，针对特定数据类型引入 Vortex 的先进编码能力。

---

## 附录：学术论文参考

| 论文 | 关联编码 | 链接 |
|------|---------|------|
| ALP | 浮点压缩 | https://ir.cwi.nl/pub/33334/33334.pdf |
| FSST (VLDB 2020) | 字符串压缩 | https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf |
| FastLanes (VLDB 2023) | 整数压缩 | https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf |
| BtrBlocks | 级联压缩 | https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf |
| G-ALP | 广义浮点压缩 | https://dl.acm.org/doi/pdf/10.1145/3736227.3736242 |
| Procella | YouTube 数据系统 | https://dl.acm.org/citation.cfm?id=3360438 |
