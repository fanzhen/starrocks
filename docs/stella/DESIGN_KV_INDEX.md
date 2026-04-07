# Stella KV Index：面向 Paimon 外表向量检索行捞回的行存索引

## 1. 动机与背景

### 1.1 核心场景：向量检索后的行捞回瓶颈

#### 1.1.1 Paimon Global Index：已解决"定位"，未解决"取值"

Paimon (master/1.5) 已内置一套 Global Index 体系，支持在 Append Table 上建立多种检索索引：

| 索引类型 | 底层实现 | 用途 |
|---------|---------|------|
| **BTree Index** | SST 文件上的 B-tree | 标量列等值/范围查询 |
| **Vector Index** | DiskANN | 向量近邻搜索 |
| **Full-Text Index** | Tantivy | 全文检索 |

前提条件：表必须开启 `bucket = -1`（unaware-bucket）、`row-tracking.enabled = true`（全局唯一 `_ROW_ID`）、`global-index.enabled = true`。

所有 Global Index 的查询流程是统一的：**索引返回 `GlobalIndexResult`（匹配的 `_ROW_ID` 集合）→ 引擎用 `_ROW_ID` 回列存数据文件取行数据**。

```
Global Index 查询流程:
  输入: 查询谓词 (name='foo', vector_search, fulltext_search)
  ①  索引检索 → _ROW_ID 集合                    ← 高效（locate 已解决）
  ②  用 _ROW_ID 回列存数据文件取行数据              ← 慢（fetch 未解决）
```

定位（locate）已经高效，但**取值（fetch）仍然走列存路径**——这就是瓶颈所在。

#### 1.1.2 行捞回瓶颈：列存格式对点查的结构性低效

以向量检索为例，典型工作流是：

```
用户查询: SELECT id, name, payload FROM docs WHERE vector_search(embedding, query_vec, top_k=100)

执行流程:
  1. 调用 Paimon SDK 的 Vector Index (DiskANN) → 返回 top-100 的 _ROW_ID 列表
  2. 根据 _ROW_ID 列表"捞回"对应行的完整数据 ← 瓶颈在这里
  3. 应用 Filter 过滤
  4. 执行 Projection 输出
```

**步骤 2 的"行捞回"是当前的核心瓶颈**。Global Index 高效地返回了 `_ROW_ID` 列表，但将这些 `_ROW_ID` 转化为实际行数据时，必须走列存路径：

```
捞回 100 个 _ROW_ID 对应的 3 列数据（当前列存路径）:
  - 不知道 _ROW_ID 分布在哪些数据文件中 → 可能扫描多个 Parquet 文件
  - 每个文件: 读 Footer → 定位 RowGroup → 读 3 个 ColumnChunk → 解压 → 解码
  - 每个 ColumnChunk ~MB 级，但只需要其中 ~几十行
  - 总 I/O: 数十 MB（对象存储远程读取），实际需要: ~KB 级
```

这不仅是向量检索的问题——**它是 BTree 索引查询、全文检索等所有 Global Index 查询流程共有的"最后一公里"瓶颈**。

瓶颈的根因不在于实现不够优化，而在于**列存格式与点查访问模式的结构性错配**：取 1 行 N 列需要 N 次 Column Chunk 级 I/O（各 ~MB 级），解压、解码整个 Chunk 后只提取 1 个值。这是列存为批量扫描优化的必然代价——它不可能在保持列式布局的同时高效服务单行随机访问。

#### 1.1.3 解法思路：预物化行存索引

既然列存对点查存在结构性低效，自然的解法是：**将高频点查涉及的列，预先按 `_ROW_ID` 组织为行存格式，使得一次键查找即可返回完整行数据**。

具体而言，如果我们维护一个从 `_ROW_ID` 到 `{col1, col2, ...}` 的有序键值映射，那么"行捞回"就从列存多文件扫描简化为一次键值查找——单次 block I/O + 简单反序列化。Paimon 的 `_ROW_ID`（Row Tracking 提供的全局唯一行标识符）是天然的 key：不可变、有序、全局唯一。

这就是本设计提出的 **KV Index**——一个关联到 Paimon Append Table 的预物化行存索引，与 Global Index 形成互补：

```
Global Index 负责定位（locate）:  查询谓词 → _ROW_ID 集合
KV Index 负责取值（fetch）:       _ROW_ID → 行数据 {col1, col2, ...}

当前:   VectorIndex → _ROW_IDs → [列存扫描: 多文件 Parquet I/O] → Filter → Project
                                  ~~~~~~~~ 瓶颈 ~~~~~~~~

有 KV:  VectorIndex → _ROW_IDs → [KV MultiGet: SSTable block I/O] → Filter → Project
                                  ~~~~~~~~ 快 10-50x ~~~~~~~~
```

**设计边界**：本设计**仅服务于 Paimon Global Index 行捞回场景**——即上游 Global Index（VectorSearch / BTree / FullText）返回 `_ROW_ID` 集合后，用 KV Index 加速取行数据。不支持独立的 `_ROW_ID` 点查（`WHERE _ROW_ID = X`）或业务键点查（`WHERE user_id = X`）——这些场景走列存路径。聚焦单一场景使版本管理和优化器集成大幅简化（详见 2.5 节）。

### 1.2 Paimon Append Table 与 Row Tracking

#### 1.2.1 Paimon 表类型

Paimon 支持两类表：

- **Primary Key Table**：有主键，支持 upsert/delete，基于 LSM-tree 的 merge-on-read
- **Append Table（无主键表）**：仅追加写入，不支持按行更新/删除。开启 Row Tracking 后获得全局行标识（`_ROW_ID`）和版本追踪能力，但**不改变 Append-only 语义**——行数据一旦写入不可变

本设计针对 **Append Table + Row Tracking** 场景。

#### 1.2.2 Row Tracking 机制

Paimon Append Table 开启 `'row-tracking.enabled' = 'true'` 后，表 schema 自动增加两个隐藏列：


| 隐藏列                | 类型     | 说明                                                                                         |
| ------------------ | ------ | ------------------------------------------------------------------------------------------ |
| `_ROW_ID`          | BIGINT | 全局唯一的行标识符。每行在写入时由 committer 延迟分配，一旦分配永不改变。即使行因 compaction 从一个文件移动到另一个文件，`_ROW_ID` 也会被复制保留。 |
| `_SEQUENCE_NUMBER` | BIGINT | 行的版本号，实际值等于该行所属 snapshot 的 snapshot-id。用于跟踪行的版本变化。                                         |


Row Tracking 的关键规则：

1. 读取 Row Tracking 表时，`_ROW_ID` 和 `_SEQUENCE_NUMBER` 保证 **NOT NULL**。
2. 首次追加记录时，`_ROW_ID` 和 `_SEQUENCE_NUMBER` 不实际写入数据文件，而是由 committer **延迟分配**（lazy assigned）。
3. 读取时，先从数据文件读取这两个字段；若为 NULL，则从 `DataFileMeta` 回退到延迟分配的值。
4. 当行因 compaction 从一个文件移到另一个文件时，**行数据（用户列值）不变**，`_ROW_ID` 必须复制到目标文件。`_SEQUENCE_NUMBER` 在文件搬迁时可能被重置为 NULL（由后续读取回退到新的 snapshot-id），但这仅影响元数据追踪，不影响用户列值。**Append Table 没有 update 语义——行一旦写入，value 不可变。**

**重要约束**：Row Tracking **仅支持 unaware-bucket append table**，不允许定义 `bucket` 和 `bucket-key`（即必须 `bucket = -1`）。因此 `_ROW_ID` 是**表级全局唯一**的，不存在 bucket 维度。Paimon Global Index 同样要求 `bucket = -1`，其索引文件按 `_ROW_ID` range 分片组织（`_ROW_RANGE_START / _ROW_RANGE_END`），是表级概念而非分区级或 bucket 级。

```sql
-- Paimon 建表示例（Flink SQL）
CREATE TABLE events (
    event_id BIGINT,
    user_id INT,
    event_type STRING,
    payload STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH ('row-tracking.enabled' = 'true');

-- 读取时可查询隐藏列
SELECT event_id, user_id, _ROW_ID, _SEQUENCE_NUMBER FROM events;
-- 结果示例:
-- +----------+---------+--------+------------------+
-- | event_id | user_id | _ROW_ID| _SEQUENCE_NUMBER |
-- +----------+---------+--------+------------------+
-- |     1001 |      42 |      0 |                1 |
-- |     1002 |      43 |      1 |                1 |
-- +----------+---------+--------+------------------+
```

#### 1.2.3 为什么 `_ROW_ID` 是理想的 KV Index Key

`_ROW_ID` 作为 KV 索引 key 的优势：

1. **全局唯一**：Paimon 保证 `_ROW_ID` 在整张表范围内唯一，天然适合作为 KV 的 key。
2. **不可变**：一旦分配，`_ROW_ID` 永不改变（即使 compaction 移动了行），索引不会因 compaction 失效。
3. **系统管理**：由 Paimon 自动分配和维护，用户无需手动指定唯一键。
4. **BIGINT 类型**：定长 8 字节，编码简单，排序高效。

> **未来扩展**：后续版本可支持用户指定业务唯一键（如 `event_id`）作为 key，需配合独立点查路径。v1 仅使用 `_ROW_ID`。

### 1.3 Paimon Snapshot 版本模型

理解 Paimon 的 Snapshot 机制对 KV 索引的一致性设计至关重要。

#### 1.3.1 Snapshot 结构

Paimon 的每次提交（commit）生成一个 Snapshot 文件，版本号从 1 开始连续递增：

```
warehouse/
└── default.db/
    └── my_table/
        ├── snapshot/
        │   ├── EARLIEST          # 最早 snapshot 的提示文件
        │   ├── LATEST            # 最新 snapshot 的提示文件
        │   ├── snapshot-1        # JSON 格式
        │   ├── snapshot-2
        │   └── snapshot-3
        ├── manifest/             # Manifest 文件（Avro 格式）
        └── data/                 # 数据文件（Parquet/ORC）
```

#### 1.3.2 Snapshot 文件内容（JSON）

每个 Snapshot 文件包含：


| 字段                  | 说明                                                                                                                                           |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                | Snapshot ID，与文件名一致                                                                                                                           |
| `schemaId`          | 该提交对应的 schema 版本                                                                                                                             |
| `baseManifestList`  | 记录前序 snapshot 所有变更的 manifest list                                                                                                            |
| `deltaManifestList` | 记录本次 snapshot 新增变更的 manifest list                                                                                                            |
| `commitKind`        | 变更类型：`APPEND` / `COMPACT` / `OVERWRITE` / `ANALYZE`。注意：Paimon 的删除语义不通过独立的 `commitKind` 表达，而是通过 Manifest 中的文件级 DELETE 记录或 Deletion Vector 实现。 |
| `timeMillis`        | 提交时间戳                                                                                                                                        |
| `totalRecordCount`  | 本次 snapshot 涉及的总记录数                                                                                                                          |


#### 1.3.3 Manifest 层次

```
Snapshot → Manifest List → Manifest File(s) → DataFileMeta(s)
```

- **Manifest List**：包含多个 Manifest 文件的元信息列表（Avro 格式）
- **Manifest File**：包含多个数据文件的元信息，每条记录标记为 `ADD` 或 `DELETE`（文件级别的增删）
- **DataFileMeta**：单个数据文件的元信息，包含文件名、大小、行数、key 统计、min/max sequence number 等

#### 1.3.4 数据一致性保证

- 写入时**抢占**（preempt）下一个 snapshot-id；snapshot 文件成功写入后，该提交**立即可见**
- Manifest 中的文件变更是有序的，同一个文件可能被多次 ADD 或 DELETE，读取时取最后一个版本
- 这种设计使 compaction 产生的文件替换（DELETE 旧文件 + ADD 新文件）成为轻量级原子操作

### 1.4 列存 vs 行存：点查场景的量化分析

1.1 中提出了"列存对点查存在结构性低效"的论断，本节给出量化分析。

#### 1.4.1 列存点查的完整读取路径

当 StarRocks 通过 Paimon Global Index 返回的 `_ROW_ID` 集合回列存取行数据时，实际读取路径为：

```
Paimon Append Table:
  1. 获取目标 snapshot → 解析 manifest list → 获取数据文件列表
  2. 对每个候选 Parquet/ORC 数据文件:
     a. 读取 Footer → 获取 Row Group 元信息
     b. Row Group 级 min/max 统计 → 粗粒度裁剪（对 _ROW_ID 可能有效）
     c. 对每个候选 Row Group 的每个请求列:
        - 从对象存储读取 Column Chunk（通常 ~MB 级别）
        - 解压（Snappy/Zstd/LZ4）
        - 解码（Dictionary / RLE / Delta / ...）
        - 物化列值
     d. 应用 _ROW_ID 谓词过滤
  3. 跨文件汇总结果
```

对于一个查 2 列、返回 1 行的点查，根本性的开销是：

| 步骤 | 开销 | 问题本质 |
|------|------|---------|
| 文件/页读取 | 2 次 Column Chunk 远程读取（各 ~MB 级） | 只需要 ~几十字节，却读取了 ~MB 级数据 |
| 解压 | 2 次整块解压 | 必须解压整个 Column Chunk 才能提取一个值 |
| 编码解码 | 2 次整块解码 | 必须解码整个页才能定位到目标行 |
| 多文件扫描 | O(文件数) 倍乘 | 不知道 `_ROW_ID=X` 在哪个文件中，可能需要扫描多个文件 |

**核心低效：读 2 个值共 ~100 字节，却需要从对象存储中读取并解压 ~MB 级的列式数据页，且可能需要检查多个文件。**

#### 1.4.2 行存 KV 为什么在点查场景一定赢

从第一性原理出发，行存在点查场景有三个根本性优势：

**1. I/O 放大比**

- 列存：读 N 列需要 N 次 Column Chunk I/O。每个 Chunk 在 MB 量级，总量 = N × ChunkSize。以 2 列点查为例，需从对象存储远程读取数 MB 数据。
- 行存 KV：一次键查找返回所有请求列的打包值。总量 = key_size + value_size ≈ 几百字节。
- **放大降低：数量级差距**（对象存储场景下，列存读取 ~MB 级 vs KV 读取 ~KB 级）。

**2. 计算放大**

- 列存：解压 + 解码一整个 Column Chunk（数千个值）来提取 1 个值。
- 行存 KV：反序列化一条打包的行记录。
- **计算降低：数量级差距/列**。

**3. 查找复杂度**

- 列存：不知道目标行在哪个文件，可能需要检查多个文件的 Row Group 统计。
- 行存 KV：bloom filter 检查 + 单个 data block 读取。
- **期望复杂度：从 O(文件数 × log(N)) 降到 O(SSTable 数)**。前提条件：SSTable 层级受控（定期 compaction 合并为少量文件）、bloom filter 和 index block 已缓存到本地 block cache。在最理想情况下（单 SSTable + 缓存命中），为 O(1)。

行存 KV 索引消除了列存点查的整个流水线：一次 SSTable Get(`_ROW_ID`) 返回预组装好的行，单次 block I/O + 简单反序列化即可完成。

### 1.5 StarRocks 中的现有基础

StarRocks 已有的可复用基础设施：


| 组件                                     | 用途                       | 与 KV Index 的关系                                 |
| -------------------------------------- | ------------------------ | ---------------------------------------------- |
| `storage/sstable/`                     | 自研 SSTable 库（源自 LevelDB） | KV Index 的文件格式基础                               |
| `LakePersistentIndex`                  | 存算分离主键索引                 | 已验证 SSTable 文件可存放在对象存储并被 CN 并发读取               |
| `PersistentIndexSstable`               | SSTable 文件抽象             | 支持 MultiGet、bloom filter、`RandomAccessFile` 访问 |
| `PaimonMetadata`                       | Paimon connector 元数据     | 已支持 snapshot 读取、`DataSplit`、`DataFileMeta` 解析  |
| `ConnectorScanNode` / `HiveDataSource` | 外表扫描执行                   | Paimon 表的读取路径                                  |


**关键发现：StarRocks 的 `storage/sstable/` 库是一个可以独立于 RocksDB 使用的 SSTable 实现**，支持：

- 写入有序 KV 对（`TableBuilder::Add`）
- 点查（`Table::MultiGet`）
- 范围迭代（`Table::NewIterator`）
- Bloom filter（`FilterPolicy`）
- 通过 `RandomAccessFile` 抽象访问（本地文件或对象存储均可）

### 1.6 业界参考


| 系统           | 特性                                                  | 说明                                                       |
| ------------ | --------------------------------------------------- | -------------------------------------------------------- |
| Apache Doris | Row Store / Row Cache                               | 内存缓存完整行，`"store_row_column" = "true"`                    |
| ClickHouse   | 无内置行缓存                                              | 依赖主键稀疏索引 + mark 级跳跃                                      |
| TiDB (TiKV)  | 原生 KV 存储                                            | RocksDB 行存作为主存储格式                                        |
| DuckDB       | 无点查优化                                               | 纯分析型，无行存索引                                               |
| Paimon 自身    | Row Tracking + Global Index (BTree/Vector/FullText) | 提供全局 `_ROW_ID` 和多种检索索引，但索引输出 `_ROW_ID` 后仍需回列存取行，无预物化行存加速 |


Doris 方案存储完整行，浪费空间。我们方案更灵活——用户选择 value 列，控制空间与灵活性权衡。

## 2. 设计

### 2.1 核心概念

**KV Index** 是一个关联到 Paimon Append Table 的**预物化行存索引**，**专为 Global Index 行捞回场景设计**。它将 Paimon 的 `_ROW_ID` 映射到一组选定的列值，以行式格式存储为 SSTable 文件。核心价值是加速 Paimon Global Index（向量检索、BTree、全文索引）返回 `_ROW_ID` 后的**行捞回**步骤——从列存多文件扫描降为 SSTable MultiGet。

```
┌─────────────────────────────────────────────────────────────────┐
│              Paimon Append Table (Row Tracking)                  │
│                                                                  │
│  ┌───────────────────────────┐  ┌────────────────────────────┐  │
│  │   列存数据文件              │  │   KV Index                 │  │
│  │   (Parquet/ORC)           │  │   (SSTable 文件)            │  │
│  │                           │  │                            │  │
│  │  RowGroup[col1] [col2]    │  │  SSTable:                  │  │
│  │  RowGroup[col1] [col2]    │  │   _ROW_ID=0 → {v1,v2}    │  │
│  │  ...                      │  │   _ROW_ID=1 → {v1,v2}    │  │
│  └───────────────────────────┘  └────────────────────────────┘  │
│         ↓ 对象存储                     ↓ 对象存储 + CN 本地缓存    │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心设计决策

#### 2.2.1 索引归属：为什么 KV Index 必须是 StarRocks 内部索引

这是最高层级的架构决策，决定了后续所有设计选择的边界。

**根本原因：KV Index 与 Paimon Global Index 不在同一个抽象层**

Paimon Global Index 解决的是**检索**问题——给定查询谓词，返回匹配的 `_ROW_ID` 集合。所有已有 Global Index 类型（BTree / Vector / FullText）共享统一接口：`GlobalIndexReader` → `GlobalIndexResult`（`RoaringNavigableMap64`，即 `_ROW_ID` 位图）。

KV Index 解决的是**物化**问题——给定 `_ROW_ID`，返回预组装好的行数据 `{col1, col2, ...}`。它的输入是 Global Index 的输出，它的输出是行数据而非位图。这不是同一类索引，不存在公共接口。

Paimon 虽然有完整的 `GlobalIndexerFactory` SPI 插件机制（`ServiceLoader` 加载，已有 4 个实现），技术上可以注册新 index type。但 `GlobalIndexResult` 只能返回 `_ROW_ID` 位图——**接口本身不支持"返回物化行数据"的语义**，这是 API 层面的根本限制，不是实现层面的缺失。

**StarRocks 持有的优势**

即使 Paimon 未来扩展了接口，引擎内部持有在性能和集成深度上仍有显著优势：

| 能力 | 引擎内部持有 | Paimon 持有 |
|------|-----------|-----------|
| **Plan Rewrite** | FE 本地元数据，零额外开销 | 需额外 SDK 调用获取索引元数据 |
| **Block Cache** | SSTable 4KB block 直接进入 CN cache | 需适配引擎 cache 接口，或退化为 OS page cache |
| **读取路径** | C++ `MultiGet`，进程内完成 | 经 Paimon Java API，增加 JNI 调用开销 |
| **版本判定** | FE plan 阶段本地完成 | 需与 Paimon 侧交互获取版本信息 |

**Tradeoff 与演进路径**

StarRocks 持有的代价是**自建生命周期管理**：需要自行跟踪 Paimon Snapshot 版本、自行清理过期索引文件。这是确定的工程成本，通过版本绑定 + 后台维护线程 + GC 策略可控地解决（详见 2.5、2.6 节）。

核心原则：**KV Index 的读写路径和生命周期均由 StarRocks 完全控制**。SSTable + Manifest 存储在对象存储 `.starrocks_kv_index/` 目录下，StarRocks 自建版本绑定、可用性判定和过期清理。这是性能优势和工程简洁性的来源——不依赖外部系统的生命周期语义，避免引入额外的集成复杂度。

#### 2.2.2 存储格式：SSTable 文件

KV Index 需要一个面向有序键值点查的存储格式。候选方案有三个：

**Parquet/ORC 不可行**——列式格式与 KV 点查是反模式。取 1 行 N 列需要 N 次 Column Chunk I/O（各 ~MB 级），而 SSTable 只需 1 次 DataBlock 读取（~4KB）。连 Paimon 自身的 BTree Global Index 都选择 SST 文件而非 Parquet。

**RocksDB 不可行**——本地 LSM-tree 需要 WAL + MANIFEST，无法运行在对象存储上；多 CN 不能并发打开同一实例（write lock）；外表没有 tablet 概念。

**选型：StarRocks 自研 `storage/sstable/` 库**——源自 LevelDB 的独立 SSTable 实现，`LakePersistentIndex` 已验证可用于存算分离场景。不可变文件，直接存放对象存储，任意 CN 并发读取无锁冲突。

#### 2.2.3 用户选定的 value 列

只有在索引定义中指定的列才会存储在 KV 的 value 中。控制存储开销，用户可以只索引高频点查的列。

#### 2.2.4 Key 选择

- **默认且唯一方式（v1）**：使用 Paimon Row Tracking 的 `_ROW_ID`（要求表已开启 `row-tracking.enabled=true`）。这与 Global Index 行捞回场景天然匹配——Global Index 输出 `_ROW_ID` 集合，KV Index 以 `_ROW_ID` 为 key。
- **自定义 key（未来扩展）**：用户指定业务逻辑上的唯一键列。当前设计仅服务 Global Index 行捞回（输出固定为 `_ROW_ID`），自定义 key 需要配合独立点查路径才有触发入口，暂不纳入 v1 范围。

#### 2.2.5 按分区组织 + 全局路由索引

KV Index 的 SSTable 文件按照 Paimon 表的分区粒度组织。每个分区的 KV 索引独立构建、独立存储、独立更新。

**`_ROW_ID` 的分区路由问题**：Global Index 返回的 `_ROW_ID` 集合中，无法仅靠 `_ROW_ID` 值推导出该行属于哪个分区。为解决此问题，KV Index Manifest 中维护一个**全局路由索引**：

```
全局路由索引（per partition）:
  partition "dt=2026-01":  _ROW_ID range [0, 999999],    bloom filter
  partition "dt=2026-02":  _ROW_ID range [1000000, 3499999], bloom filter
  partition "dt=2026-03":  _ROW_ID range [3500000, 3999999], bloom filter
```

路由策略（按优先级）：

1. **查询同时带有分区谓词**（如 `WHERE dt = '2026-01' AND vector_search(...)`）：上游 Global Index 已限定分区，直接定位到目标分区，O(1)。
2. **仅有 `_ROW_ID` 集合**：先用 min/max range 裁剪候选分区，再用 bloom filter 进一步过滤，通常可缩小到 1 个分区。
3. **降级场景**：若 `_ROW_ID` 分配不连续（多 committer 并发写入导致 range 重叠），可能需要 fan-out 到多个候选分区。此时查询复杂度为 O(候选分区数)，而非 O(1)。Manifest 中记录 `_ROW_ID` 的分布质量指标（如 range 重叠率），当 fan-out 候选分区数超过阈值时，FE 优化器直接禁用 KV 索引，避免高 fan-out 误用。

**Bloom filter 存储形态**（分层策略）：

- **FE Catalog 中仅存轻量摘要**：per-partition 的 `key_min`、`key_max`、`row_count`、bloom filter 文件路径。FE 内存占用 = O(分区数 × 固定字节)，与数据量无关。
- **Bloom filter bitset 存对象存储**：每个分区的 bloom filter 序列化为独立文件（或与 SSTable 共存于同一目录），路径记录在 Manifest 中。单个 bloom 大小约 10 bits/key（100 万 key ≈ 1.2MB）。
- **CN 按需加载 bloom**：查询时 CN 根据 min/max 初筛后的候选分区，按需从对象存储加载对应的 bloom filter，缓存在本地 block cache 中。
- **FE 元数据上限**：单表 Manifest 总大小（不含 bloom bitset）不超过可配置阈值（如 100MB）。超过时拒绝创建新索引并提示用户清理旧版本。
- **降级**：当分区数过多（如 > 10000）导致即使 min/max 也无法有效裁剪时，FE 优化器直接禁用 KV 索引，避免高 fan-out。


### 2.3 SQL 语法

```sql
-- 在 Paimon 表上创建 KV 索引（v1: 固定使用 _ROW_ID 作为 key）
CREATE INDEX idx_name ON paimon_catalog.db.table_name
    USING KV
    VALUE (value_col1, value_col2, ...)
    [PROPERTIES ("prop_key" = "prop_value", ...)];

-- 示例
CREATE INDEX idx_events_kv ON paimon_catalog.db.events
    USING KV
    VALUE (user_id, event_type, payload);

-- 删除 KV 索引
DROP INDEX idx_events_kv ON paimon_catalog.db.events;
```

语义说明：

- v1 固定使用 `_ROW_ID` 作为 key（要求表已开启 Row Tracking），与 Global Index 输出的 `_ROW_ID` 集合天然匹配
- **v1 每张表仅支持一个 KV 索引**。重复 `CREATE INDEX ... USING KV` 会报错
- 索引元信息（Manifest）存储在对象存储中（与 SSTable 文件共存），FE 仅缓存轻量摘要，Paimon 系统无感知

### 2.4 KV 编码格式

**Key 编码**（v1: `_ROW_ID` 模式）：

- BIGINT 的 big-endian 8 字节编码（保证字节序与数值序一致）

> **未来扩展**：自定义 key 模式可复用 `PrimaryKeyEncoder` 的编码逻辑，将多列编码为可比较的字节序列。

**Value 编码**：简单行式序列化：

```
Value = [null_bitmap][col_1_data][col_2_data]...[col_N_data]

null_bitmap: ceil(N/8) 字节，bit i = 1 表示第 i 列为 NULL
col_i_data:
  - 定长类型 (INT/BIGINT/DOUBLE/...): 原始字节，原生宽度
  - VARCHAR/STRING: 4 字节长度前缀 + 原始字节
  - DECIMAL: 底层整数表示的原始字节
```

故意保持简单——不做额外压缩/编码。单条 value 通常 < 1KB，SSTable 在 block 级别做 LZ4 压缩。

### 2.5 索引版本管理

KV 索引是 Paimon 表某一时刻的行数据快照。核心问题是：**查询时索引可能滞后于 Paimon 最新 Snapshot，如何保证正确性？**

#### 2.5.1 版本绑定

每次索引构建产生一个版本记录：

- **`index_build_version`**：构建代次（递增）
- **`base_snapshot_id`**：该构建覆盖到的 Paimon Snapshot ID

不需要每个 Snapshot 都触发构建。查询时 FE 取最新的 `index_build_version`，比较 `base_snapshot_id` 与 `query_snapshot_id` 的关系。

#### 2.5.2 版本匹配：为什么索引滞后不影响正确性

KV 索引的主要使用场景是 **Global Index 行捞回**：上游 Paimon Global Index（VectorSearch / BTree / FullText）基于 `query_snapshot` 执行检索，返回当前有效的 `_ROW_ID` 集合，再用 KV Index 取行数据。

在这条路径上，即使索引滞后，也不存在正确性问题：

1. **上游已过滤无效行**：Global Index 返回的 `_ROW_ID` 集合基于 `query_snapshot`，已排除被删除的行。KV 索引中的过期条目不会被访问。
2. **行不可变**：Append Table 中已写入的行，value 永远不变。KV 索引中已有条目的 value 永远正确。
3. **`_ROW_ID` 不复用**：单调递增分配，删除后不会被新行占用。不存在"旧 value 被错误关联到新行"的风险。

因此，版本匹配规则非常简单：

> **`query_snapshot_id >= base_snapshot_id` → 索引可用。**

索引未覆盖的新增行（APPEND 后产生），由上游 Global Index 感知，回退列存扫描。若滞后过大（超过可配置阈值），放弃使用索引，整个查询回退列存。

COMPACT 对 KV 索引完全透明：Paimon 的 COMPACT 只做文件合并（Manifest 中 DELETE 旧文件 + ADD 新文件），行数据和 `_ROW_ID` 不变，KV 索引按 `_ROW_ID` 索引与底层文件位置无关。

#### 2.5.3 增量构建

由于行不可变 + `_ROW_ID` 不复用，增量构建只需处理新增行：

```
当前: index_build_version=2, base_snapshot_id=7
目标: Paimon 最新 snapshot-10

1. 收集 snapshot 8→10 中 APPEND 新增的数据文件（跳过 COMPACT）
2. 扫描新增文件，提取 (_ROW_ID, value_cols)，生成增量 SSTable
3. 与已有 SSTable 合并为新版本
4. 记录 index_build_version=3, base_snapshot_id=10
5. 更新对象存储 Manifest，刷新 FE 摘要缓存

特殊情况: 中间存在 OVERWRITE → 受影响分区全量重建（旧条目成为死数据，需清理）
```

#### 2.5.4 索引元信息存储

Manifest 存储在**对象存储**中（`.starrocks_kv_index/` 目录下），包含版本记录、SSTable 文件列表、分区级 key range 等。FE 仅缓存轻量摘要（`base_snapshot_id`、`key_min/key_max`），不持久化到 FE image/journal。FE 重启时从对象存储重新加载。

### 2.6 后台维护线程

KV 索引需要后台线程执行三类维护任务，保持索引的时效性和空间效率：

#### 2.6.1 增量构建（Build）

当 Paimon 表产生新的 APPEND Snapshot 时，后台线程检测到 `latest_snapshot_id > base_snapshot_id`，自动触发增量构建：

- 收集新增 Snapshot 中 `commitKind=APPEND` 的 DataFileMeta
- 提取新增行的 `(_ROW_ID, value_cols)`，生成增量 SSTable
- 与已有 SSTable 合并，更新 `index_build_version` 和 `base_snapshot_id`
- 触发策略：定期检查（可配置间隔，如 5 分钟）或达到新增行数阈值时触发

#### 2.6.2 过期条目清理（Purge）

当 Paimon 表发生行级删除（Deletion Vector）或 OVERWRITE 时，KV 索引中对应的 `_ROW_ID` 条目成为死数据。虽然在 Global Index 行捞回路径中这些过期条目不会被访问（上游已过滤），但它们仍占用存储空间。后台线程定期清理：

- 读取 Deletion Vector 获取被删除的具体 `_ROW_ID` 集合
- 对于 OVERWRITE Snapshot，识别受影响分区中被替换的 `_ROW_ID` 范围
- 重写 SSTable 文件，移除已删除的 `_ROW_ID` 条目
- **注意**：仅处理导致行实际删除的操作（Deletion Vector / OVERWRITE），**不处理 COMPACT 的文件级 DELETE**——COMPACT 只做文件合并替换，行数据和 `_ROW_ID` 不变（与 2.5.2 一致）
- 触发策略：当死条目比例超过可配置阈值（如 20%）时触发清理

#### 2.6.3 SSTable Compaction

增量构建会产生多个小 SSTable 文件，查询时需要 fan-out 到多个文件。后台线程定期合并小文件：

- 当单分区内 SSTable 文件数超过阈值（如 10 个）时触发
- 多路归并合并为少量大文件（≤ 256MB/文件）
- 合并过程中顺带清理已删除的 `_ROW_ID` 条目

### 2.7 存储架构

#### 2.7.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│ FE                                                                  │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ SQL Parser    │→ │ Analyzer         │→ │ Optimizer            │  │
│  │ (KV IndexDef) │  │ (校验 key/value) │  │ (行捞回路由+版本匹配) │  │
│  └──────────────┘  └──────────────────┘  └──────────────────────┘  │
│                                                 │                    │
│                              ┌──────────────────┤                    │
│                              ↓                  ↓                    │
│                    KV Index Manifest     Paimon Snapshot 版本        │
│                    (对象存储, FE 缓存摘要) (PaimonMetadata)           │
│                                                 ↓ TExecPlan          │
├─────────────────────────────────────────────────────────────────────┤
│ CN (计算节点)                                                        │
│                                                                     │
│  ┌────────────────────┐      ┌───────────────────────────────┐     │
│  │ KVIndexScanNode     │─────→│ KVIndexReader                 │     │
│  │ (exec/scan)         │      │ (storage/kv_index_reader.h)   │     │
│  └────────────────────┘      └───────────────────────────────┘     │
│                                          │                          │
│                                 ┌────────┴────────┐                │
│                                 ↓                  ↓                │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐     │
│  │ 本地 Block Cache     │  │ 对象存储 (S3/OSS/HDFS)          │     │
│  │ (SSTable 数据块缓存) │  │ KV SSTable 文件                 │     │
│  └─────────────────────┘  └─────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 2.7.2 SSTable 文件在对象存储上的组织

```
s3://warehouse/paimon_db/events/.starrocks_kv_index/
├── idx_events_kv/                          # 索引名
│   ├── v1_snap3/                           # Version 1, Snapshot 3
│   │   ├── dt=2026-01/
│   │   │   └── kv_00001.sst
│   │   └── dt=2026-02/
│   │       └── kv_00001.sst
│   ├── v2_snap7/                           # Version 2, Snapshot 7
│   │   ├── dt=2026-01/
│   │   │   └── kv_00001.sst               # 可能与 v1 相同（未变化的分区）
│   │   ├── dt=2026-02/
│   │   │   ├── kv_00001.sst
│   │   │   └── kv_00002.sst
│   │   └── dt=2026-03/
│   │       └── kv_00001.sst
│   └── manifest.json                       # 全局 Manifest
```

#### 2.7.3 多 CN 并发查询 SSTable 的分发策略

SSTable 文件是**不可变的只读文件**，天然支持多 CN 并发读取无锁冲突。核心问题是：**FE 如何将 SSTable 查询任务分发到多个 CN，以实现并行行捞回？**

**问题分析：SSTable 有序性与拆分**

SSTable 文件内部 key 是全局有序的，但这**不妨碍多 CN 并发查询**。原因：行捞回场景是 **MultiGet**（给定 `_ROW_ID` 集合，查找对应值），不是范围扫描。每个 `_ROW_ID` 的查找是独立的点查操作，不依赖 key 的连续性。因此拆分维度不是"切分 SSTable 文件的 key range"，而是**切分待查找的 `_ROW_ID` 集合**。

**分发模型：按 `_ROW_ID` 集合分片**

```
上游 Global Index 返回 _ROW_ID 集合: {5, 23, 47, 102, 198, 301, 455, 789, 1024, 1500}

FE 分发策略:
  1. 分区路由: 根据 Manifest 中 per-partition key_min/key_max + bloom filter,
     将 _ROW_ID 分组到目标分区
       partition "dt=2026-01": {5, 23, 47, 102, 198}
       partition "dt=2026-02": {301, 455, 789, 1024, 1500}

  2. CN 分配: 每个分区的 _ROW_ID 子集分配给一个 CN
       CN-1: partition "dt=2026-01" → MultiGet({5, 23, 47, 102, 198})
       CN-2: partition "dt=2026-02" → MultiGet({301, 455, 789, 1024, 1500})

  3. 同分区内 SSTable fan-out: 若分区有多个 SSTable 文件,
     CN 内部对所有 SSTable 执行 MultiGet (bloom filter 快速跳过不包含目标 key 的文件)
```

**三级并行度**

| 级别 | 并行维度 | 分发者 | 说明 |
|------|---------|--------|------|
| **L1: 分区级** | 不同分区的 SSTable 分发到不同 CN | FE | 分区间完全独立，无数据重叠 |
| **L2: `_ROW_ID` 子集级** | 同一分区内，将 `_ROW_ID` 集合拆分到多个 CN | FE | 每个 CN 对同一组 SSTable 文件执行不同 key 的 MultiGet，互不干扰 |
| **L3: SSTable 文件级** | 单 CN 内，对分区中的多个 SSTable 文件并发 MultiGet | CN | bloom filter 快速排除不含目标 key 的文件，减少无效 I/O |

**与 RocksDB LSM Level 的区别：三级可全并发**

RocksDB 的 LSM Level 是版本分层（新 Level shadow 旧 Level），同一 key 可能存在多个 Level，**必须从新到旧串行查找**。KV Index 的三级并行度与此本质不同——由于 Paimon Append Table 的 `_ROW_ID` 不可变 + 不复用，**同分区内 SSTable 文件 key 不重叠**，不存在 shadow 语义。因此 L3 可对所有文件并发 MultiGet，bloom filter 直接定位唯一命中的文件，无需逐层搜索。

**MultiGet 的批量局部性优化**

MultiGet 并非对每个 `_ROW_ID` 独立执行一次 Get。当输入上万个 `_ROW_ID` 时，实现上利用 SSTable 的有序性做批量优化：

1. **排序归并**：将待查找的 `_ROW_ID` 集合排序后，对 SSTable 的 index block 做单次顺序扫描，定位所有目标 data block。落在同一 data block（~4KB）内的多个 key **共享一次 block 读取**。
2. **I/O 合并**：排序后相邻的 `_ROW_ID` 大概率命中相邻的 data block，对象存储可合并为连续范围读取（range read），减少 I/O 次数。
3. **bloom filter 批量检查**：对多文件场景，先用 bloom filter 批量过滤，仅对命中的文件执行排序归并查找。

因此 MultiGet 10K 个 `_ROW_ID` 的开销远小于 10K 次独立 Get——核心收益来自排序后的 data block 共享和 I/O 合并。

**L1 分区级并行**是主要并行度来源。**L2 `_ROW_ID` 子集级**在单分区查询量极大时启用：FE 将 `_ROW_ID` 列表分片到多个 CN，每个 CN 对同一组 SSTable 执行各自子集的 MultiGet（不可变文件，并发无冲突）。

**CN 分配策略**

- **Block Cache 亲和性**：优先将同一分区的查询分配到同一 CN，利用 SSTable block cache 热度
- **负载均衡**：亲和 CN 负载过高时 Round-Robin 选择其他 CN
- **降级**：CN 不可用时自动切换（SSTable 在对象存储上，任意 CN 可访问，仅损失 cache 热度）

### 2.8 数据流

#### 2.8.1 首次构建（Full Build）

```
CREATE INDEX 触发:
  1. FE 获取 Paimon 表的 latest snapshot id (如 snapshot-5)
  2. FE 下发构建任务到 CN
  3. CN 对每个分区:
     a. 使用 Paimon ReadBuilder 读取 snapshot-5 的数据
     b. 提取 (_ROW_ID, value_col1, value_col2, ...)
     c. 按 _ROW_ID 排序
     d. 使用 sstable::TableBuilder 生成 SSTable 文件
     e. 上传到对象存储
  4. FE 记录:
     - index_build_version = 1
     - base_snapshot_id = 5
     - 各分区的 SSTable 文件列表、全局路由索引（key_min/key_max）、bloom filter 文件路径
```

#### 2.8.2 增量构建（Incremental Build）

```
定期/手动触发（流程与 2.5.3 增量构建一致）:
  1. FE 获取 Paimon 最新 snapshot id (如 snapshot-12)
  2. FE 读取当前 KV Index 最新构建版本:
     - index_build_version = 1, base_snapshot_id = 5
  3. 构建策略判定（与 2.5.3 一致）:
     - 遍历 snapshot-6 到 snapshot-12 的 commitKind
     - 全部为 APPEND/COMPACT → 增量构建（只处理新增数据）
     - 存在 OVERWRITE → 受影响分区需全量重建（旧条目已失效）
  4. FE 通过 Paimon API 获取 snapshot 5→12 的增量变更:
     - 仅收集 commitKind=APPEND 新增的 DataFileMeta
     - 忽略 COMPACT 变更（不影响行内容，_ROW_ID 不变）
  5. CN 对增量文件:
     a. 读取新增数据文件中的 (_ROW_ID, value_cols)
     b. 生成增量 SSTable 文件
     c. 与已有 SSTable 合并（或保持多个 SSTable 后续 compact）
  6. FE 记录:
     - index_build_version = 2
     - base_snapshot_id = 12
     - 更新全局路由索引（key_min/key_max/bloom filter）
```

#### 2.8.3 读取路径（Global Index 行捞回）

```
FE 查询规划:
  1. 上游 Global Index 算子（VectorSearch / BTree / FullText）基于 query_snapshot
     执行检索，返回 _ROW_ID 集合
  2. 确定 query_snapshot_id（由 ConnectorTableVersion 决定）
  3. 版本可用性判定:
     - query_snapshot_id >= base_snapshot_id → ✓ 可用
     - query_snapshot_id < base_snapshot_id → ✗ 禁用，回退列存
     - 索引滞后过大（差异行数超阈值）→ ✗ 禁用，回退列存
  4. 分区路由:
     - 检查全局路由索引: key_min/key_max + bloom filter 匹配目标分区
  5. 生成 KVIndexScanNode:
     - KV 索引命中的 _ROW_ID → KVIndexScanNode
     - 未命中的 _ROW_ID（索引滞后未收录）→ ConnectorScanNode 回退列存

CN 执行:
  KVIndexScanNode::get_next(chunk)
    ├── 根据上游 _ROW_ID 集合 + 全局路由索引确定目标分区的 SSTable 文件
    ├── 批量编码 _ROW_ID → SSTable key
    ├── 在 SSTable 文件上执行 MultiGet
    ├── 反序列化 value → 行数据，追加到输出 chunk
    └── 返回 chunk
```

### 2.9 优化器集成：索引发现、应用与执行计划改写

KV Index 与优化器的集成是本设计的核心——优化器负责发现可用索引、判断适用性、改写执行计划。

#### 2.9.1 索引发现（Index Discovery）

FE 优化器在生成执行计划时，查询 Catalog 中目标 Paimon 表上注册的所有 KV 索引：

```
输入: 目标表 paimon_catalog.db.events
输出: [
  {name: "idx_events_kv", key: [_ROW_ID], value: [user_id, event_type, payload],
   latest_build_version: 2, base_snapshot_id: 7}
]
```

#### 2.9.2 索引适用性判定（Index Applicability）

优化器检查以下条件，**全部满足**时才将扫描路由到 KV 索引：

1. **表定义了 KV 索引**
2. **Key 集合来自上游 Global Index**：上游算子已产出 `_ROW_ID` 集合（如 `PaimonVectorSearchNode`、`PaimonBTreeIndexNode`、`PaimonFullTextSearchNode` 输出的 `_ROW_ID` 列表）
3. **Value 列覆盖**：Filter 引用列 + Project 输出列（排除 key 列）都被 KV 索引 value 列覆盖
4. **基本查询模式**：没有聚合、GROUP BY、JOIN 或 ORDER BY
5. **规模可控**：估算的点查数量 < 可配置阈值（如 1000）
6. **版本可用性**：`query_snapshot_id >= base_snapshot_id`；差异行数超过阈值 → 禁用

#### 2.9.3 执行计划改写（Plan Transformation）

**场景 1：向量检索 + 行捞回（主要场景）**

```
改写前（列存捞回）:
  PaimonVectorSearchNode(_ROW_IDs)
    → ConnectorScanNode(读列存 Parquet, 按 _ROW_ID 过滤)
      → FilterNode(应用用户 WHERE 条件)
        → ProjectNode(输出列)

改写后（KV 捞回）:
  PaimonVectorSearchNode(_ROW_IDs)
    → KVIndexScanNode(SSTable MultiGet, 直接取行数据)
      → FilterNode(应用用户 WHERE 条件)
        → ProjectNode(输出列)

改写条件: KV Index 的 value 列覆盖了 Filter + Project 所需的全部列
```

**场景 2：BTree/FullText 索引 + 行捞回**

```
改写前:
  PaimonBTreeIndexNode(name='foo' → _ROW_IDs)
    → ConnectorScanNode(读列存, 按 _ROW_ID 过滤)

改写后:
  PaimonBTreeIndexNode(name='foo' → _ROW_IDs)
    → KVIndexScanNode(SSTable MultiGet)

原理与向量检索相同: 所有 Global Index 的输出都是 _ROW_ID 集合,
KV Index 统一加速"_ROW_ID → 行数据"这一步
```

#### 2.9.4 Filter 下推到 KV Index

当 KV Index 的 value 列覆盖了 Filter 条件中的列时，Filter 可以在 KV 取值后立即应用，避免不必要的列传递：

```
SELECT id, name FROM docs
WHERE vector_search(embedding, query_vec, top_k=1000)
  AND category = 'tech'
  AND score > 0.5

执行计划:
  VectorSearch → _ROW_IDs (1000个)
    → KVIndexScan(取 id, name, category, score)
      → Filter(category='tech' AND score>0.5)  ← 在 KV 结果上直接过滤
        → Project(id, name)

前提: KV Index value 列包含 category 和 score
```

#### 2.9.5 索引选择策略

v1 每张表仅支持一个 KV 索引（key 固定为 `_ROW_ID`）。优化器判定逻辑：

1. **Value 列覆盖**：索引 value 列覆盖 Filter + Project 所需全部列 → 使用 KV 索引
2. **版本可用**：`query_snapshot_id >= base_snapshot_id` 且差异行数未超阈值
3. **不满足则回退**：ConnectorScanNode 列存扫描

## 3. ASR 分析（Availability / Scalability / Reliability）

### 3.1 Availability（可用性）

**设计目标**：KV Index 不可用时，查询自动降级到列存路径，**零业务中断**。


| 场景                                             | 行为                                                            | 影响                               |
| ---------------------------------------------- | ------------------------------------------------------------- | -------------------------------- |
| KV Index 尚未构建                                  | 优化器不改写计划，走 ConnectorScanNode 列存路径                             | 无影响，等同无索引状态                      |
| 索引版本滞后（`base_snapshot_id < query_snapshot_id`） | 直接可用（上游 Global Index 已过滤无效 `_ROW_ID`），新增行回退列存 | 查询正确性不受影响              |
| 索引版本失效（OVERWRITE / Deletion Vector）            | **查询仍可用**（上游已过滤无效 `_ROW_ID`，过期条目不会被访问）；**构建需重建**受影响分区（2.5.3），过期条目由后台线程清理（2.6.2） | 查询无影响；构建开销增加    |
| SSTable 文件损坏或对象存储不可达                           | CN 读取 SSTable 失败 → 返回 error → FE 可选择 retry 其他 CN 或回退列存        | 需实现 fallback 逻辑                   |
| Manifest 文件损坏                                  | FE 无法解析 Manifest → 视为索引不存在，回退列存                               | 完全降级，需手动 `DROP INDEX` + 重建       |
| FE 摘要缓存过期或丢失（FE 重启）                            | FE 启动时从对象存储重新加载 Manifest → 重建摘要缓存                             | 短暂不可用（加载期间），之后恢复                 |
| 索引构建过程中有查询                                     | 查询使用旧版本索引（或无索引），构建完成后原子切换新版本                                  | 构建期间不影响查询可用性                     |


**关键设计约束**：

- **KV Index 是加速索引，不是数据源**。列存数据始终是 ground truth，KV Index 是可丢弃的物化视图。任何异常场景的最终兜底都是回退列存。
- **版本判定简单**：仅服务于 Global Index 行捞回场景，上游已过滤无效 `_ROW_ID`，`query_snapshot_id >= base_snapshot_id` 即可用。
- **后台维护保持空间效率**：后台线程负责增量构建、过期条目清理和 SSTable Compaction（详见 2.6 节）。
- **索引不可用不阻塞 DDL/DML**：Paimon 表的 APPEND/COMPACT/OVERWRITE 操作不受 KV Index 存在与否的影响。

### 3.2 Scalability（可扩展性）

**设计目标**：KV Index 的构建和查询性能随数据量和集群规模**线性扩展**。

#### 数据规模扩展


| 维度          | 扩展策略                                          | 瓶颈分析                                                    |
| ----------- | --------------------------------------------- | ------------------------------------------------------- |
| 行数增长        | SSTable 按文件大小阈值分割（≤256MB），单分区可包含多个 SSTable 文件 | MultiGet 需 fan-out 到多个 SSTable → bloom filter 过滤减少无效读取  |
| 分区数增长       | 每个分区独立构建、独立存储、独立查询                            | FE 摘要缓存 = O(分区数 × 固定字节)；分区数 > 10000 时 FE 禁用索引避免 fan-out |
| Value 列宽度增长 | SSTable block 级 LZ4 压缩                        | value 过大（>1KB/行）时 block 膨胀，点查 I/O 增大。建议仅索引高频查询列         |
| 并发查询增长      | SSTable 不可变文件，无锁并发读；CN 本地 block cache 吸收热点    | 冷查询全部打到对象存储时，受对象存储 IOPS/带宽限制                            |


#### 构建规模扩展


| 维度                 | 扩展策略                                     | 限制                                    |
| ------------------ | ---------------------------------------- | ------------------------------------- |
| 全量构建               | 多 CN 并行构建，按分区分发任务                        | 单分区数据必须在单 CN 内排序（v1）；超大分区可能受单 CN 内存限制 |
| 增量构建               | 仅读取 APPEND 新增的 DataFileMeta，生成增量 SSTable | 增量 SSTable 累积过多时需 compaction 合并       |
| SSTable Compaction | 多个小 SSTable 合并为大 SSTable，减少查询时 fan-out   | Compaction 期间占用 CN 计算资源和对象存储带宽        |


#### 扩展性边界（已知限制）

- **单表索引大小**：受对象存储容量限制（实际无上限），但 FE Manifest 摘要缓存有可配置上限（如 100MB）
- **`_ROW_ID` 路由退化**：多 committer 并发写入导致 `_ROW_ID` range 重叠时，分区路由从 O(1) 退化为 O(候选分区数)。Manifest 记录 range 重叠率，超过阈值时 FE 禁用索引
- **跨分区行捞回**：Global Index 返回的 `_ROW_ID` 集合涉及 K 个分区时，需 K 次独立 SSTable 查询。分区级并行可缓解但不消除
- **Value 列变更**：索引定义的 value 列如发生 schema evolution（如列类型变更），需全量重建索引

### 3.3 Reliability（可靠性）

**设计目标**：KV Index 在任何故障场景下**不导致数据错误**，最差降级到列存路径。

#### 数据正确性保证


| 保证        | 机制                                                                                   | 验证方式                          |
| --------- | ------------------------------------------------------------------------------------ | ----------------------------- |
| **读写一致性** | 索引版本严格绑定 Paimon Snapshot（`index_build_version` ↔ `base_snapshot_id`），`query_snapshot_id >= base_snapshot_id` 即可用 | 构建后 full scan 列存 vs KV 索引交叉比对 |
| **无幻读**   | `query_snapshot_id > base_snapshot_id` 的差异行回退列存扫描，不从 KV 索引返回不存在的 key                 | 增量构建后 diff 验证                 |
| **无脏读**   | 上游 Global Index 基于 `query_snapshot` 过滤无效行，KV 索引中的过期条目不会被访问              | 模拟 OVERWRITE 后验证 KV 索引仍正确工作 |
| **构建幂等**  | 同一 `base_snapshot_id` 的构建产出确定性结果（相同 key+value → 相同 SSTable 内容）                       | 重复构建 diff 验证                  |
| **原子切换**  | 新版本 Manifest 写入对象存储成功后才对查询可见；写入失败不影响旧版本                                              | 模拟写入中断验证旧版本仍可用                |


#### 故障恢复


| 故障场景                  | 恢复策略                                                   | RPO / RTO                         |
| --------------------- | ------------------------------------------------------ | --------------------------------- |
| CN 构建中途 crash         | 构建任务失败，已上传的部分 SSTable 为孤儿文件 → 定期 GC 清理                 | RPO=0（列存数据不受影响），RTO=重新触发构建        |
| FE crash（摘要缓存丢失）      | FE 重启后从对象存储加载 Manifest 重建摘要缓存                          | RPO=0，RTO=FE 启动时间 + Manifest 加载时间 |
| 对象存储部分不可用             | SSTable 读取失败 → 查询回退列存                                  | RPO=0，RTO=对象存储恢复时间                |
| Manifest 写入部分成功       | Manifest 采用不可变对象 + 指针切换：将新版本写为独立对象（单对象 PUT 原子性由对象存储保证），成功后更新 `latest` 指针指向新对象 | RPO=0，不产生半写 Manifest              |
| Paimon Snapshot 被意外删除 | 索引版本的 `base_snapshot_id` 指向不存在的 Snapshot → 索引标记无效，回退列存 | RPO=0，需重建索引                       |


#### 数据持久性

- **SSTable 文件持久性**：依赖对象存储（S3/OSS/HDFS）的持久性保证（通常 11 个 9）
- **Manifest 持久性**：存储在对象存储中，与 SSTable 共享相同持久性级别
- **FE 摘要缓存**：非持久化，可从对象存储完整恢复，不是单点
- **索引可重建性**：KV Index 可随时从 Paimon 列存数据完整重建（`DROP INDEX` + `CREATE INDEX`），列存数据是唯一 source of truth

#### 一致性模型

```
KV Index 一致性 = Paimon Snapshot Isolation 的子集

                          Paimon 视角       KV Index（Global Index 行捞回）
                          ──────────       ──────────────────────────
Snapshot N (已索引)         完整可见          KV 索引可用 ✓
Snapshot N+1 (APPEND)     完整可见          索引可用，新增 key 回退列存
Snapshot N+2 (OVERWRITE)  完整可见          查询仍可用（上游已过滤无效 _ROW_ID），受影响分区需重建

不变式: KV Index 返回的每一行数据，在 query_snapshot 的列存视角中一定存在且值相同。
保证机制: 上游 Global Index 基于 query_snapshot 执行检索，仅返回当前有效的 _ROW_ID。
KV 索引中的过期条目不会被访问，仅占空间（由后台维护线程清理，见 2.6.2）。
差异行（新增行）不在索引中，只影响性能（回退列存），不影响正确性。
```


