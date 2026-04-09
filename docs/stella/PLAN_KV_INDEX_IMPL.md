# KV Index 实施计划

> 基于设计文档 `DESIGN_KV_INDEX.md`，本文档为 KV Index 的分阶段实施计划。
> 每个 Stage 必须 E2E 可测、完整交付，代码写完 ≠ 完成。

## Stage 总览

| Stage | 名称 | 核心交付物 | E2E 验证方式 | 依赖 |
|-------|------|---------|-----------|------|
| 0 | 基础设施搭建 | 远程 Docker 编译部署 + Paimon catalog + 测试数据 | `SELECT * FROM paimon_catalog.test_db.test_kv LIMIT 5` 返回 5 行 | 无 |
| 1 | BE KV Index Core | KVIndexWriter + KVIndexReader + UT | `run-be-ut.sh kv_index_test` 全 PASS | Stage 0 |
| 2 | FE DDL + 元数据 | Parser + Analyzer + Thrift/Proto | `CREATE/DROP INDEX ... USING KV` + `SHOW INDEX` | Stage 0 |
| 3 | Full Build 路径 | CN 构建任务 + Manifest | `CREATE INDEX` 触发构建 → 验证 SST 文件存在 | Stage 1, 2 |
| 4 | Read 路径集成 | FE Java SSTable Reader + ADMIN SHOW KV_INDEX_DATA | SQL 查询返回 SSTable 全部数据 | Stage 3 |
| 5 | 后台维护 | 增量 Build + Purge + Compaction | 追加数据后自动增量构建 + 查询验证 | Stage 3 |
| 6 | 性能优化 | MultiGet 批量优化 + block cache | perf 火焰图 + benchmark 对比 | Stage 4 |

```
依赖关系:
  Stage 0 ← 所有 Stage 依赖
  Stage 1 ← Stage 3, 4
  Stage 2 ← Stage 3, 4
  Stage 3 ← Stage 4, 5
  Stage 1 与 Stage 2 可并行
  Stage 5 与 Stage 4 可并行
  Stage 6 最后执行
```

---

## Stage 0: 基础设施搭建

### 目标

在远程机器上搭建 Docker 编译/部署环境，配置 Paimon catalog，准备测试数据。

### 步骤

1. **SSH 连接远程机器**
   ```bash
   ssh -i ~/.ssh/my_ecs.pem root@8.217.233.254
   ```

2. **拉取 dev-env Docker 镜像**
   ```bash
   docker pull starrocks/dev-env-ubuntu:latest
   ```

3. **Docker 容器内编译**
   ```bash
   docker run -it --name sr-build \
     -v /root/starrocks:/build \
     -v /root/.m2:/root/.m2 \
     starrocks/dev-env-ubuntu:latest bash
   cd /build && ./build.sh --be -j8 && ./build.sh --fe
   ```

4. **部署 FE + BE（单节点）**
   ```bash
   cd output/fe && bin/start_fe.sh --daemon
   cd ../be && bin/start_be.sh --daemon
   mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';"
   ```

5. **安装 Flink + Paimon**
   ```bash
   wget https://archive.apache.org/dist/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
   tar xzf flink-1.18.1-bin-scala_2.12.tgz
   wget -P flink-1.18.1/lib/ \
     https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/1.0/paimon-flink-1.18-1.0.jar
   cd flink-1.18.1 && ./bin/start-cluster.sh
   ```

6. **Flink SQL 创建 Paimon 表 + 写入测试数据**
   ```sql
   CREATE CATALOG paimon WITH ('type'='paimon', 'warehouse'='file:///tmp/paimon_warehouse');
   USE CATALOG paimon;
   CREATE DATABASE test_db;
   USE test_db;
   CREATE TABLE test_kv (
       id BIGINT, name STRING, score DOUBLE, category STRING
   ) WITH ('bucket'='-1', 'row-tracking.enabled'='true');
   INSERT INTO test_kv VALUES (1,'alice',0.95,'tech'),(2,'bob',0.82,'science'),(3,'carol',0.71,'tech');
   INSERT INTO test_kv VALUES (4,'dave',0.63,'art'),(5,'eve',0.99,'tech');
   ```

7. **StarRocks 侧配置 Paimon Catalog**
   ```sql
   CREATE EXTERNAL CATALOG paimon_catalog PROPERTIES (
       "type"="paimon",
       "paimon.catalog.type"="filesystem",
       "paimon.catalog.warehouse"="file:///tmp/paimon_warehouse"
   );
   ```

### E2E 验证

```sql
SELECT * FROM paimon_catalog.test_db.test_kv LIMIT 5;
-- 期望: 返回 5 行测试数据
```

### 状态: ✅ 完成 (2026-04-07)

- FE+BE 部署: 容器内 `--network host` + `priority_networks = 127.0.0.1/32`
- Flink 1.18.1 + Paimon 0.8.2: `SET 'execution.runtime-mode' = 'batch'` + 4 slots
- Paimon catalog + test_kv 表: 2 snapshots, 5 行数据
- E2E: `SELECT * FROM paimon_catalog.test_db.test_kv ORDER BY id` 返回 5 行 ✓

---

## Stage 1: BE KV Index Core Library + Unit Test

### 目标

实现 SSTable-based KV index 的写入和读取核心库，通过 UT 验证正确性。

### 新增文件

#### 1.1 `be/src/storage/kv_index/kv_index_writer.h` + `.cpp`

```cpp
class KVIndexWriter {
public:
    // value_schema: value 列的 Schema (不含 key 列, num_key_fields=0)
    KVIndexWriter(const Schema& value_schema, WritableFile* file);
    ~KVIndexWriter();

    // keys: Int64Column (_ROW_ID), 必须严格递增
    // value_chunk: 对应 value 列的 Chunk
    Status add_chunk(const Column& keys, const Chunk& value_chunk);
    Status finish();
    uint64_t file_size() const;
    std::pair<Slice, Slice> key_range() const;
};
```

实现要点:
- **Key 编码**: `encoding_utils::encode_integral<int64_t>` — 8 字节 big-endian，保序
- **Value 编码**: `RowStoreEncoderSimple::encode_columns_to_full_row_column` — null bitmap + 列数据
- **SSTable 配置**: bloom filter 10 bits/key, Snappy 压缩, 4KB block
- **Key 顺序校验**: `add_chunk` 内逐行检查 strictly increasing，违反则返回 `InvalidArgument`

#### 1.2 `be/src/storage/kv_index/kv_index_reader.h` + `.cpp`

```cpp
class KVIndexReader {
public:
    ~KVIndexReader();
    static StatusOr<std::unique_ptr<KVIndexReader>> open(
        const Schema& value_schema, RandomAccessFile* file, uint64_t file_size);

    // 批量查询: found_mask[i]=true 表示 keys[i] 找到
    // 返回 Chunk: 找到的行为实际值, 未找到的行为 NULL
    StatusOr<ChunkUniquePtr> multi_get(
        const std::vector<int64_t>& keys, std::vector<bool>* found_mask);
};
```

实现要点:
- **批量查询**: 对 keys 排序后顺序 Seek SSTable iterator (利用 locality)
- **Value 解码**: `RowStoreEncoderSimple::decode_columns_from_full_row_column`
- **输出 Chunk**: NullableColumn, 未找到的 key 对应行全部为 NULL
- **结果定位**: 排序后的查找结果通过 `Column::update_rows` 放回原始位置

#### 1.3 `be/test/storage/kv_index/kv_index_test.cpp`

测试用例:

| 用例名 | 描述 | 验证内容 |
|-------|------|---------|
| `WriteReadRoundTrip` | 1000 行 (INT, VARCHAR, DOUBLE), multi_get 全部读回 | 值完全匹配 |
| `KeyNotFound` | 查询不存在的 key | found_mask=false, value=NULL |
| `MixedFoundAndNotFound` | 混合存在/不存在的 key | 各自正确 |
| `NullValueHandling` | 部分行某些列为 NULL | round-trip NULL 保持 |
| `LargeBatch` | 100K 行, random multi_get 100 个 key | 正确性 |
| `KeyOrdering` | 乱序 key 被拒绝 | 返回 InvalidArgument |
| `DuplicateKeys` | 重复 key 被拒绝 | 返回 InvalidArgument |
| `MultipleAddChunks` | 3 批次 add_chunk | 跨批次连续性正确 |
| `NegativeKeys` | 负数 key | big-endian 保序正确 |
| `EmptyMultiGet` | 空 key 列表 | 返回 0 行 chunk |

测试模式 (仿 `persistent_index_sstable_test.cpp`):
```cpp
class KVIndexTest : public ::testing::Test {
    static void SetUpTestCase() { CHECK_OK(fs::create_directories(kTestDir)); }
    static void TearDownTestCase() { (void)fs::remove_all(kTestDir); }
    constexpr static const char* kTestDir = "./kv_index_test";
};
```

### 修改文件

| 文件 | 修改内容 |
|------|---------|
| `be/src/storage/CMakeLists.txt` | STORAGE_FILES 添加 `kv_index/kv_index_writer.cpp`, `kv_index/kv_index_reader.cpp` |
| `be/test/CMakeLists.txt` | 添加 `./storage/kv_index/kv_index_test.cpp` |

### 可复用组件

| 组件 | 文件 | 用途 |
|------|------|------|
| `sstable::TableBuilder` | `be/src/storage/sstable/table_builder.h` | 写入有序 KV 到 SSTable |
| `sstable::Table::Open` + `NewIterator` | `be/src/storage/sstable/table.h` | 打开 SSTable + 点查 |
| `sstable::NewBloomFilterPolicy(10)` | `be/src/storage/sstable/filter_policy.h` | Bloom filter |
| `encoding_utils::encode_integral<int64_t>` | `be/src/storage/primary_key_encoder.h` | Big-endian key 编码 |
| `RowStoreEncoderSimple` | `be/src/storage/row_store_encoder_simple.h` | Value 序列化/反序列化 |
| `ChunkHelper::new_chunk` | `be/src/storage/chunk_helper.h` | 创建输出 Chunk |
| `ChunkHelper::column_from_field` | `be/src/storage/chunk_helper.h` | 创建临时解码列 |
| `fs::new_writable_file / new_random_access_file` | `be/src/fs/fs.h` | 文件 I/O |

### E2E 验证

```bash
# 远程机器 Docker 容器内 (注意: 测试编入 starrocks_dw_test 二进制, 非独立 target)
docker exec sr-dev bash -c 'cd /build && ./run-be-ut.sh \
  --build-target starrocks_dw_test \
  --module starrocks_dw_test \
  --gtest_filter "KVIndexTest.*" \
  --without-java-ext -j8'
# 期望: All 11 tests passed
```

### 验证流程

```
本地编码完成 → git commit → git push (fanzhen remote)
→ 远程: git fetch + git checkout FETCH_HEAD
→ Docker 内 run-be-ut.sh --build-target starrocks_dw_test --gtest_filter "KVIndexTest.*"
→ 全部 PASS → Stage 1 complete
→ 失败 → 本地修 bug → 新 commit → 回到 push
```

### 状态: ✅ 完成 (2026-04-08)

**E2E 结果 (远程 8.217.233.254, Docker sr-dev, ASAN build):**
```
[==========] Running 11 tests from 1 test suite.
[  PASSED  ] 11 tests. (1613 ms total)
  KVIndexTest.WriteReadRoundTrip    (242 ms)
  KVIndexTest.KeyNotFound           (2 ms)
  KVIndexTest.MixedFoundAndNotFound (2 ms)
  KVIndexTest.NullValueHandling     (3 ms)
  KVIndexTest.LargeBatch            (1354 ms)
  KVIndexTest.KeyOrdering           (0 ms)
  KVIndexTest.DuplicateKeys         (0 ms)
  KVIndexTest.MultipleAddChunks     (4 ms)
  KVIndexTest.NegativeKeys          (3 ms)
  KVIndexTest.EmptyMultiGet         (1 ms)
  KVIndexTest.CrossBatchKeyOrdering (1 ms)
```

**修复的 bugs:**
1. `ColumnPtr` = `ImmutPtr<const Column>` (COW 模式): 必须用 `chunk->mutable_columns()` 获取 `MutableColumnPtr` 才能修改列数据
2. `NullableColumn::null_column()->get_data()` 返回 `ImmContainer` (不可变 span): 改用 `null_column_data()` 获取可变引用
3. `TableBuilder::Abandon()` 断言 `!closed`: `KVIndexWriter` 析构函数需跳过已 `finish()` 的 builder (增加 `_finished` 标记)
4. 测试目录残留文件: `SetUpTestCase` 需先 `remove_all` 再 `create_directories`
5. `ASSIGN_OR_ABORT`/`ASSERT_OK` 宏需从 `base/testutil/assert.h` 引入
6. `Int64Column::create()` 返回 `MutPtr` 不是 `shared_ptr`: 用 `ColumnPtr` 作为 `make_keys()` 返回类型

---

## Stage 2: FE DDL + 元数据传播

### 目标

支持 `CREATE INDEX ... USING KV` / `DROP INDEX` SQL 语法，元数据正确存储和展示。

### 修改文件

#### 2.1 FE Parser 层

| 文件 | 修改内容 |
|------|---------|
| `fe/fe-parser/src/main/java/com/starrocks/sql/ast/IndexDef.java` | `IndexType` 枚举新增 `KV("KV")` |
| `fe/fe-grammar/StarRocks.g4` | CREATE INDEX 语法支持 `USING KV VALUE (col_list)` 子句 |

#### 2.2 Thrift / Proto

| 文件 | 修改内容 |
|------|---------|
| `gensrc/thrift/Descriptors.thrift` | `TIndexType` 枚举新增 `KV` |

#### 2.3 FE Analyzer

| 文件 | 修改内容 |
|------|---------|
| `fe/fe-core/.../sql/analyzer/` 相关 | KV 索引校验: 目标表是 Paimon 外表 + Row Tracking 已开启 + value 列存在且类型受支持 + v1 每表仅一个 KV 索引 |

#### 2.4 FE Catalog

| 文件 | 修改内容 |
|------|---------|
| `fe/fe-core/.../catalog/Index.java` | 支持 KV 索引的元数据存储 (value 列列表) |
| `fe/fe-core/.../connector/paimon/PaimonMetadata.java` | 注册 KV 索引元数据管理 |

### E2E 验证

```sql
CREATE INDEX idx_test_kv ON paimon_catalog.test_db.test_kv
    USING KV VALUE (name, score, category);
SHOW INDEX FROM paimon_catalog.test_db.test_kv;
-- 期望: 显示 idx_test_kv, type=KV, columns=[name, score, category]

DROP INDEX idx_test_kv ON paimon_catalog.test_db.test_kv;
SHOW INDEX FROM paimon_catalog.test_db.test_kv;
-- 期望: 空
```

### 验证流程

```
本地编码完成 → git commit → git push
→ 远程: git pull + Docker 内 build FE+BE → 部署 → mysql 验证
→ 全部通过 → Stage 2 complete
```

### 状态: ✅ 完成 (2026-04-08)

**E2E 结果 (远程 8.217.233.254, Docker sr-dev):**
- `CREATE INDEX idx_test_kv ON paimon_catalog.test_db.test_kv (name, score, category) USING KV` — 成功 ✓
- `SHOW INDEX` — 显示 `idx_test_kv, KV` ✓
- `DROP INDEX` — 清空索引元数据 ✓

**实现方式 (v1 简化):**
- 语法: `CREATE INDEX ... USING KV` (无 VALUE 子句, value 列直接在列名列表中指定)
- IndexType 枚举新增 KV
- 元数据: 内存态 `KVIndexMetadataManager` (FE 重启后丢失)
- 无 Analyzer 校验 (不检查 Paimon 表类型/Row Tracking)

---

## Stage 3: Full Build 路径

### 目标

CREATE INDEX 触发首次全量构建：读取 Paimon 数据 → 生成 SSTable → 写入存储 → 更新 Manifest。

### 新增文件

#### 3.1 `be/src/storage/kv_index/kv_index_build_task.h` + `.cpp`

构建任务:
- 输入: Paimon 表的 snapshot_id, 分区列表, value 列 Schema, 输出路径
- 流程: PaimonReader 读取数据 → 按 `_ROW_ID` 排序 → KVIndexWriter 写 SSTable
- 输出: SSTable 文件列表, key range, 文件大小

#### 3.2 `be/src/storage/kv_index/kv_index_manifest.h` + `.cpp`

Manifest 管理:
- 版本记录: `index_build_version`, `base_snapshot_id`
- 分区级 SSTable 文件列表
- 全局路由索引: per-partition `key_min/key_max`
- JSON 序列化/反序列化
- 存储路径: `<warehouse>/<table>/.starrocks_kv_index/<index_name>/manifest.json`

### 修改文件

| 文件 | 修改内容 |
|------|---------|
| FE 构建任务调度 | `CREATE INDEX` 完成元数据注册后，异步下发构建任务到 CN |
| FE 状态管理 | 构建状态 (BUILDING / READY / FAILED) |
| `be/src/storage/CMakeLists.txt` | 新增源文件 |

### E2E 验证

```sql
CREATE INDEX idx_test_kv ON paimon_catalog.test_db.test_kv
    USING KV VALUE (name, score);
SHOW INDEX FROM paimon_catalog.test_db.test_kv;
-- 期望: status=READY, base_snapshot_id=N

-- 验证 SSTable 文件存在:
-- ls <warehouse>/<table>/.starrocks_kv_index/idx_test_kv/
```

### 状态: ✅ 完成 (2026-04-09)

**实现方式 (v1: FE 侧 Java 构建):**
- FE 异步线程池 (2 daemon threads) 通过 Paimon Java API 读取数据
- 纯 Java LevelDB SSTable 写入器 (`KVIndexSSTWriter.java`, ~500 行)
  - 二进制兼容 BE `sstable::Table::Open` (prefix-compressed blocks, CRC32C, Snappy, bloom filter)
  - Key: `encode_integral<int64_t>` (XOR sign bit + big-endian)
  - Value: `RowStoreEncoderSimple` 格式 (header + null bitmap + offsets + column data)
- 文件存储: `<warehouse>/.starrocks_kv_index/<indexName>/data/kv_00001.sst` + `manifest.json`
- 构建状态: PENDING → BUILDING → READY/FAILED, 通过 `SHOW INDEX` 展示

**E2E 结果 (远程 8.217.233.254, Docker sr-dev):**
- CREATE INDEX 立即返回 (非阻塞) ✓
- SHOW INDEX 显示 `KV (READY, snapshot=2, rows=5)` ✓
- SSTable 文件存在 (353 bytes) ✓
- manifest.json 正确 (baseSnapshotId=2, rowCount=5, 3 columns) ✓
- DROP INDEX + 重建 — 正常工作 ✓

**新增文件:**
- `KVIndexBuildExecutor.java` — 异步构建执行器
- `KVIndexSSTWriter.java` — Java LevelDB SSTable 写入器

**修改文件:**
- `KVIndexMetadataManager.java` — 重构为 KVIndexMeta + BuildState
- `PaimonMetadata.java` — CREATE INDEX 触发异步构建
- `ShowExecutor.java` — SHOW INDEX 展示构建状态
- `GlobalStateMgr.java` — 注册 KVIndexBuildExecutor

**修复的 bugs:**
1. Checkstyle: try-with-resources 续行缩进 13→16 spaces
2. SnapshotManager API 不兼容: 改用 `paimonTable.latestSnapshot()`
3. 重复 import: `InternalRow`

---

## Stage 4: Read 路径集成

### 目标

验证 SSTable 文件可被正确读取，提供 SQL 可达的 lookup 接口。方案: FE Java Reader + `ADMIN SHOW KV_INDEX_DATA` SQL 语句。

### 新增文件

| 文件 | 说明 |
|------|------|
| `KVIndexSSTReader.java` | Java LevelDB SSTable 读取器 (KVIndexSSTWriter 的逆操作) |
| `AdminShowKVIndexDataStmt.java` | `ADMIN SHOW KV_INDEX_DATA FROM catalog.db.table INDEX name` AST 节点 |

### 修改文件

| 文件 | 修改内容 |
|------|---------|
| `StarRocksLex.g4` | 新增 `KV_INDEX_DATA` lexer token |
| `StarRocks.g4` | 新增 `adminShowKVIndexDataStatement` 规则 + nonReserved |
| `AstVisitor.java` | 新增 `visitAdminShowKVIndexDataStatement` 方法 |
| `AstBuilder.java` | Parser → AST (支持 1/2/3 part qualifiedName) |
| `AdminStmtAnalyzer.java` | 基础校验 (index name required) |
| `Analyzer.java` | 委托到 AdminStmtAnalyzer |
| `AuthorizerStmtVisitor.java` | OPERATE 权限校验 |
| `ShowExecutor.java` | 核心执行: KVIndexSSTReader 读取 → 动态列 ShowResultSet |
| `ShowResultMetaFactory.java` | 占位元数据 (实际动态列在 ShowExecutor 构建) |
| `RedirectStatus.java` | NO_FORWARD (本地读) |
| `KVIndexMetadataManager.java` | KVIndexMeta 新增 `columnNames`/`columnTypes` 字段 |
| `KVIndexBuildExecutor.java` | 构建完成时回写列名/列类型到 KVIndexMeta |

### E2E 验证

```sql
-- 1. 确认索引 READY
SHOW INDEX FROM paimon_catalog.test_db.test_kv;
-- 期望: KV (READY, snapshot=2, rows=5)

-- 2. 查询 KV 索引数据
ADMIN SHOW KV_INDEX_DATA FROM paimon_catalog.test_db.test_kv INDEX idx_test_kv;
-- 期望: 返回 5 行 (ROW_ID, name, score, category)

-- 3. 交叉验证
SELECT name, score, category FROM paimon_catalog.test_db.test_kv ORDER BY name;
-- 期望: 结果一致

-- 4. DROP + 重建
DROP INDEX idx_test_kv ON paimon_catalog.test_db.test_kv;
CREATE INDEX idx_test_kv ON paimon_catalog.test_db.test_kv (name, score, category) USING KV;
ADMIN SHOW KV_INDEX_DATA FROM paimon_catalog.test_db.test_kv INDEX idx_test_kv;
-- 期望: 同样返回 5 行
```

### 状态: ✅ 完成 (2026-04-09)

**E2E 结果 (远程 8.217.233.254, Docker sr-dev):**
- `SHOW INDEX` 显示 `KV (READY, snapshot=2, rows=5)` ✓
- `ADMIN SHOW KV_INDEX_DATA` 返回 5 行 (ROW_ID 0-4, name/score/category 正确) ✓
- 交叉验证: KV 索引数据与列存 SELECT 完全一致 ✓
- DROP + 重建后再次查询 — 正常返回 5 行 ✓

**修复的 bugs:**
1. `KVIndexSSTWriter.finish()`: `writeRawBlock()` 写 filter/metaindex block 时覆盖 `pendingHandleOffset/Size`，导致最后一个 data block 的 index entry 指向 metaindex block。修复: 写非数据 block 前保存 pending handle

---

## Stage 5: 后台维护

### 目标

实现增量构建、过期条目清理 (Purge)、SSTable Compaction 三个后台维护任务。

### 新增文件

#### 5.1 `be/src/storage/kv_index/kv_index_maintenance.h` + `.cpp`

后台维护线程:
- **增量 Build**: 检测 `latest_snapshot > base_snapshot` → 收集 APPEND 增量 → 生成增量 SSTable → 合并 Manifest
- **Purge**: 检测 Deletion Vector / OVERWRITE → 重写 SSTable 移除死条目
- **SSTable Compaction**: 文件数超阈值 → 多路归并为大文件

#### 5.2 FE 维护调度器

- 定期检查 (可配置间隔, 如 5 分钟)
- 增量行数阈值触发
- 死条目比例阈值触发 Purge

### E2E 验证

```sql
-- 1. Stage 3 已完成
-- 2. 向 Paimon 表追加新数据 (通过 Flink)
-- 3. 等待/手动触发增量构建
SHOW INDEX FROM paimon_catalog.test_db.test_kv;
-- base_snapshot_id 应更新到最新 snapshot

-- 4. 查询新增数据，验证覆盖
SELECT * FROM TABLE(kv_index_lookup(..., ARRAY[新增_ROW_ID]));
```

### 状态: 未开始

---

## Stage 6: 性能优化

### 目标

通过 perf 定位热点，优化 MultiGet 批量性能、block cache 利用率。

### 优化方向

1. **MultiGet 排序归并**: 排序 `_ROW_ID` → 共享 data block 读取 → I/O 合并
2. **Block Cache**: SSTable 4KB block 进入 CN 本地 cache
3. **Value 编码优化**: 评估是否需要比 RowStoreEncoderSimple 更紧凑的编码

### E2E 验证

```bash
# perf 采集
perf record -g -p <be_pid> -- sleep 30
perf report --stdio

# benchmark: KV Index 路径 vs 列存路径的 query latency
```

### 状态: 未开始

---

## 通用规范

### 验证流程 (每个 Stage)

```
1. 本地编码完成
2. git commit → git push
3. 远程: git pull → Docker 内编译 (BE: -j8, FE)
4. 部署 (重启 FE/BE)
5. E2E 验证 (UT 或 mysql 查询)
6. 全部通过 → Stage complete
7. 失败 → 本地修 bug → 新 commit → 回到 step 2
```

### 编译部署速查

```bash
# BE 编译 (-j8 安全, 不要 -j12 避免 OOM)
cd /build && ./build.sh --be -j8

# FE 编译
cd /build && ./build.sh --fe

# 单测快速迭代
./run-be-ut.sh --build-target kv_index_test --module kv_index_test --without-java-ext -j8

# 特定测试用例
./run-be-ut.sh --build-target kv_index_test --module kv_index_test \
  --gtest_filter "KVIndexTest.WriteReadRoundTrip" --without-java-ext

# 修改 Proto/Thrift 后需 clean 重编
./build.sh --fe --clean
```

### 远程机器信息

- **地址**: 8.217.233.254 (root, SSH key `~/.ssh/my_ecs.pem`)
- **规格**: 24 核 x86_64, 45GB RAM
- **BE 并行度**: `-j8` (不要 `-j12`, OOM 风险)
- **OS**: Alibaba Cloud Linux 3 (podman 代替 docker, 已安装 `podman-docker` 兼容层)
- **JDK**: OpenJDK 17 (FE 要求 JDK 17+)
- **Git 源**: `https://github.com/fanzhen/starrocks.git` (fanzhen fork)

### 容器内部署关键 Skill

**问题**: 宿主机 GLIBC 2.32, 编译产物需要 GLIBC 2.33+, 因此必须在容器内运行 FE+BE。

```bash
# 1. 创建容器 (--network host 确保端口直通)
docker run -d --name sr-dev --network host \
  -v /root/starrocks:/build \
  -v /root/.m2:/root/.m2 \
  -v /tmp/paimon_warehouse:/tmp/paimon_warehouse \
  starrocks/dev-env-ubuntu:latest sleep infinity

# 2. BE 配置: priority_networks 必须匹配 ADD BACKEND 的 IP
#    否则 BE 会 resolve hostname → 192.168.x.x, 与 FE 注册的 127.0.0.1 不匹配
#    错误: "FE saved address not match backend address"
echo 'priority_networks = 127.0.0.1/32' >> /build/output/be/conf/be.conf
#    注意: 必须在独立行, 不要 cat >> 导致与上一行拼接

# 3. FE 配置: 同样设置 priority_networks
echo 'priority_networks = 127.0.0.1/32' >> /build/output/fe/conf/fe.conf

# 4. 启动 FE (容器内, 后台)
docker exec -d sr-dev bash -c \
  'export JAVA_HOME=/lib/jvm/java-17-openjdk && cd /build/output/fe && bin/start_fe.sh --daemon'
# 等待 ~25 秒 FE 完全启动

# 5. 启动 BE (容器内, 后台)
docker exec -d sr-dev bash -c 'cd /build/output/be && bin/start_be.sh --daemon'
# 等待 ~10 秒

# 6. 注册 BE (宿主机 mysql)
mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';"

# 7. 验证
mysql -h 127.0.0.1 -P 9030 -u root -e 'SHOW BACKENDS\G'
# 期望: Alive: true, StatusCode: OK

# 8. 单节点需设置 replication_num=1
# CREATE TABLE ... PROPERTIES("replication_num"="1");
```

**重启 FE+BE 流程** (代码更新后):
```bash
# 停容器 → 清旧进程 → 重启容器 → 启动 FE/BE → 不需要重新 ADD BACKEND (除非清了 FE meta)
docker stop sr-dev; docker rm sr-dev
# 如果需要 clean start, 删除 meta:
rm -rf /root/starrocks/output/fe/meta /root/starrocks/output/be/storage
# 重新创建容器 + 启动
```

### Flink + Paimon 测试数据 Skill

```bash
# Paimon connector: 0.8.2 (1.0 不存在), 需要 flink-shaded-hadoop
cd /root/flink-1.18.1/lib/
wget 'https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.2/paimon-flink-1.18-0.8.2.jar'
wget 'https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar'

# Flink 配置: 增加 slots (默认 1 slot, 多 job 会排队卡住)
sed -i 's/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: 4/' conf/flink-conf.yaml

# 关键: batch 模式 INSERT (默认 streaming 模式 INSERT 不会自动结束)
SET 'execution.runtime-mode' = 'batch';

# Aliyun 镜像下载 Flink (Apache 源极慢):
wget 'https://mirrors.aliyun.com/apache/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz'
```

### BE Unit Test Skill

```bash
# UT 测试文件编入 starrocks_dw_test 二进制 (不是独立 target)
# 通过 --gtest_filter 选择特定测试
docker exec sr-dev bash -c 'cd /build && ./run-be-ut.sh \
  --build-target starrocks_dw_test \
  --module starrocks_dw_test \
  --gtest_filter "KVIndexTest.*" \
  --without-java-ext -j8'

# StarRocks Column COW 模式:
# - ColumnPtr = ImmutPtr<Column> = RCPtr<const Column> → operator-> 返回 const Column*
# - MutableColumnPtr = MutPtr<Column> = RCPtr<Column> → operator-> 返回 Column*
# - 修改 Chunk 中的列: auto mcols = chunk->mutable_columns(); mcols[i]->append_datum(...)
# - mutable_columns() 不破坏 chunk 内部 _columns (共享底层对象, refcount==1 时无拷贝)
# - NullableColumn 可变 null 数据: nullable->null_column_data() (非 null_column()->get_data())

# TableBuilder 生命周期:
# - Finish() 后 closed=true, Abandon() 断言 !closed
# - Writer 析构需跳过已 Finish 的 builder (用 _finished 标记)

# 测试宏来源:
# - ASSIGN_OR_ABORT, ASSERT_OK, EXPECT_OK → #include "base/testutil/assert.h"
# - CHECK_OK 不可用于测试代码

# SetUpTestCase 清理:
# (void)fs::remove_all(kTestDir) 后再 fs::create_directories(kTestDir)
# 防止上次残留文件导致 "File exists" 错误
```
