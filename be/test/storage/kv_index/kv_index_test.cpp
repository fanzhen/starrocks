// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <random>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/kv_index/kv_index_reader.h"
#include "storage/kv_index/kv_index_writer.h"

namespace starrocks {

class KVIndexTest : public ::testing::Test {
public:
    static void SetUpTestCase() { ASSERT_OK(fs::create_directories(kTestDir)); }
    static void TearDownTestCase() { (void)fs::remove_all(kTestDir); }

protected:
    constexpr static const char* kTestDir = "./kv_index_test";

    static Schema make_value_schema() {
        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "col_int", LogicalType::TYPE_INT, true));
        fields.emplace_back(std::make_shared<Field>(1, "col_str", LogicalType::TYPE_VARCHAR, true));
        fields.emplace_back(std::make_shared<Field>(2, "col_dbl", LogicalType::TYPE_DOUBLE, true));
        return Schema(std::move(fields));
    }

    static ColumnPtr make_keys(const std::vector<int64_t>& vals) {
        auto col = Int64Column::create();
        col->append_numbers(vals.data(), vals.size() * sizeof(int64_t));
        return col;
    }

    static ChunkUniquePtr make_value_chunk(const std::vector<int32_t>& ints, const std::vector<std::string>& strs,
                                            const std::vector<double>& dbls,
                                            const std::vector<bool>& int_nulls = {},
                                            const std::vector<bool>& str_nulls = {},
                                            const std::vector<bool>& dbl_nulls = {}) {
        size_t n = ints.size();
        auto schema = make_value_schema();
        auto chunk = ChunkHelper::new_chunk(schema, n);

        for (size_t i = 0; i < n; i++) {
            if (!int_nulls.empty() && int_nulls[i]) {
                chunk->get_column_by_index(0)->append_nulls(1);
            } else {
                chunk->get_column_by_index(0)->append_datum(Datum(ints[i]));
            }
        }

        for (size_t i = 0; i < n; i++) {
            if (!str_nulls.empty() && str_nulls[i]) {
                chunk->get_column_by_index(1)->append_nulls(1);
            } else {
                chunk->get_column_by_index(1)->append_datum(Datum(Slice(strs[i])));
            }
        }

        for (size_t i = 0; i < n; i++) {
            if (!dbl_nulls.empty() && dbl_nulls[i]) {
                chunk->get_column_by_index(2)->append_nulls(1);
            } else {
                chunk->get_column_by_index(2)->append_datum(Datum(dbls[i]));
            }
        }

        return chunk;
    }

    struct SstInfo {
        std::string path;
        uint64_t file_size;
    };

    SstInfo write_sst(const std::string& name, const Schema& schema, const Column& keys,
                       const Chunk& value_chunk) {
        std::string path = std::string(kTestDir) + "/" + name;
        ASSIGN_OR_ABORT(auto file, fs::new_writable_file(path));
        KVIndexWriter writer(schema, file.get());
        EXPECT_OK(writer.add_chunk(keys, value_chunk));
        EXPECT_OK(writer.finish());
        uint64_t fsize = writer.file_size();
        file->close();
        return {path, fsize};
    }
};

TEST_F(KVIndexTest, WriteReadRoundTrip) {
    auto schema = make_value_schema();
    const int N = 1000;

    std::vector<int64_t> key_vals;
    std::vector<int32_t> int_vals;
    std::vector<std::string> str_vals;
    std::vector<double> dbl_vals;
    for (int i = 0; i < N; i++) {
        key_vals.push_back(i * 10);
        int_vals.push_back(i * 100);
        str_vals.push_back(fmt::format("value_{}", i));
        dbl_vals.push_back(i * 0.5);
    }

    auto keys = make_keys(key_vals);
    auto value_chunk = make_value_chunk(int_vals, str_vals, dbl_vals);
    auto sst = write_sst("round_trip.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(key_vals, &found_mask));

    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(found_mask[i]) << "key " << key_vals[i] << " not found";
    }
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(result->get_column_by_index(0)->get(i).get_int32(), int_vals[i]);
        ASSERT_EQ(result->get_column_by_index(1)->get(i).get_slice().to_string(), str_vals[i]);
        ASSERT_DOUBLE_EQ(result->get_column_by_index(2)->get(i).get_double(), dbl_vals[i]);
    }
}

TEST_F(KVIndexTest, KeyNotFound) {
    auto schema = make_value_schema();
    auto keys = make_keys({10, 20, 30});
    auto value_chunk = make_value_chunk({1, 2, 3}, {"a", "b", "c"}, {0.1, 0.2, 0.3});
    auto sst = write_sst("not_found.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::vector<int64_t> query_keys = {5, 15, 25, 100};
    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    for (size_t i = 0; i < query_keys.size(); i++) {
        ASSERT_FALSE(found_mask[i]) << "key " << query_keys[i] << " should not be found";
    }
    ASSERT_EQ(result->num_rows(), query_keys.size());
}

TEST_F(KVIndexTest, MixedFoundAndNotFound) {
    auto schema = make_value_schema();
    auto keys = make_keys({10, 20, 30, 40, 50});
    auto value_chunk = make_value_chunk({1, 2, 3, 4, 5}, {"a", "b", "c", "d", "e"}, {0.1, 0.2, 0.3, 0.4, 0.5});
    auto sst = write_sst("mixed.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::vector<int64_t> query_keys = {50, 15, 10, 99, 30};
    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    ASSERT_TRUE(found_mask[0]);
    ASSERT_EQ(result->get_column_by_index(0)->get(0).get_int32(), 5);
    ASSERT_EQ(result->get_column_by_index(1)->get(0).get_slice().to_string(), "e");

    ASSERT_FALSE(found_mask[1]);

    ASSERT_TRUE(found_mask[2]);
    ASSERT_EQ(result->get_column_by_index(0)->get(2).get_int32(), 1);

    ASSERT_FALSE(found_mask[3]);

    ASSERT_TRUE(found_mask[4]);
    ASSERT_EQ(result->get_column_by_index(0)->get(4).get_int32(), 3);
}

TEST_F(KVIndexTest, NullValueHandling) {
    auto schema = make_value_schema();
    auto keys = make_keys({1, 2, 3, 4});

    std::vector<bool> int_nulls = {false, true, false, false};
    std::vector<bool> str_nulls = {false, false, true, false};
    std::vector<bool> dbl_nulls = {false, false, false, true};
    auto value_chunk = make_value_chunk({10, 0, 30, 40}, {"aa", "", "", "dd"}, {1.1, 2.2, 3.3, 0.0}, int_nulls,
                                        str_nulls, dbl_nulls);
    auto sst = write_sst("null_values.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::vector<int64_t> query_keys = {1, 2, 3, 4};
    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(found_mask[i]);
    }

    ASSERT_FALSE(result->get_column_by_index(0)->is_null(0));
    ASSERT_EQ(result->get_column_by_index(0)->get(0).get_int32(), 10);
    ASSERT_TRUE(result->get_column_by_index(0)->is_null(1));
    ASSERT_TRUE(result->get_column_by_index(1)->is_null(2));
    ASSERT_TRUE(result->get_column_by_index(2)->is_null(3));
}

TEST_F(KVIndexTest, LargeBatch) {
    auto schema = make_value_schema();
    const int N = 100000;

    std::vector<int64_t> key_vals;
    std::vector<int32_t> int_vals;
    std::vector<std::string> str_vals;
    std::vector<double> dbl_vals;
    for (int i = 0; i < N; i++) {
        key_vals.push_back(i);
        int_vals.push_back(i);
        str_vals.push_back(fmt::format("v{}", i));
        dbl_vals.push_back(i * 0.01);
    }

    auto keys = make_keys(key_vals);
    auto value_chunk = make_value_chunk(int_vals, str_vals, dbl_vals);
    auto sst = write_sst("large_batch.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::mt19937 rng(42);
    std::vector<int64_t> query_keys;
    for (int i = 0; i < 100; i++) {
        query_keys.push_back(rng() % N);
    }

    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    for (size_t i = 0; i < query_keys.size(); i++) {
        ASSERT_TRUE(found_mask[i]) << "key " << query_keys[i] << " should be found";
        int64_t k = query_keys[i];
        ASSERT_EQ(result->get_column_by_index(0)->get(i).get_int32(), static_cast<int32_t>(k));
    }
}

TEST_F(KVIndexTest, KeyOrdering) {
    auto schema = make_value_schema();
    auto keys = make_keys({10, 5});
    auto value_chunk = make_value_chunk({1, 2}, {"a", "b"}, {0.1, 0.2});

    std::string path = std::string(kTestDir) + "/ordering.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(path));
    KVIndexWriter writer(schema, file.get());
    auto st = writer.add_chunk(*keys, *value_chunk);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
}

TEST_F(KVIndexTest, DuplicateKeys) {
    auto schema = make_value_schema();
    auto keys = make_keys({10, 10});
    auto value_chunk = make_value_chunk({1, 2}, {"a", "b"}, {0.1, 0.2});

    std::string path = std::string(kTestDir) + "/duplicate.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(path));
    KVIndexWriter writer(schema, file.get());
    auto st = writer.add_chunk(*keys, *value_chunk);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
}

TEST_F(KVIndexTest, MultipleAddChunks) {
    auto schema = make_value_schema();
    std::string path = std::string(kTestDir) + "/multi_chunk.sst";
    ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(path));
    KVIndexWriter writer(schema, wfile.get());

    for (int batch = 0; batch < 3; batch++) {
        std::vector<int64_t> k;
        std::vector<int32_t> iv;
        std::vector<std::string> sv;
        std::vector<double> dv;
        for (int i = batch * 10; i < (batch + 1) * 10; i++) {
            k.push_back(i);
            iv.push_back(i * 10);
            sv.push_back(fmt::format("batch{}_{}", batch, i));
            dv.push_back(i * 1.0);
        }
        auto keys = make_keys(k);
        auto chunk = make_value_chunk(iv, sv, dv);
        ASSERT_OK(writer.add_chunk(*keys, *chunk));
    }

    ASSERT_OK(writer.finish());
    uint64_t fsize = writer.file_size();
    wfile->close();

    ASSIGN_OR_ABORT(auto rfile, fs::new_random_access_file(path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, rfile.get(), fsize));

    std::vector<int64_t> query_keys;
    for (int i = 0; i < 30; i++) {
        query_keys.push_back(i);
    }
    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    for (int i = 0; i < 30; i++) {
        ASSERT_TRUE(found_mask[i]) << "key " << i << " not found";
        ASSERT_EQ(result->get_column_by_index(0)->get(i).get_int32(), i * 10);
    }
}

TEST_F(KVIndexTest, NegativeKeys) {
    auto schema = make_value_schema();
    auto keys = make_keys({-100, -50, -1, 0, 1, 50, 100});
    auto value_chunk = make_value_chunk({-100, -50, -1, 0, 1, 50, 100},
                                        {"neg100", "neg50", "neg1", "zero", "pos1", "pos50", "pos100"},
                                        {-10.0, -5.0, -0.1, 0.0, 0.1, 5.0, 10.0});
    auto sst = write_sst("negative_keys.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::vector<int64_t> query_keys = {-100, -1, 0, 100};
    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    ASSERT_TRUE(found_mask[0]);
    ASSERT_EQ(result->get_column_by_index(0)->get(0).get_int32(), -100);
    ASSERT_EQ(result->get_column_by_index(1)->get(0).get_slice().to_string(), "neg100");

    ASSERT_TRUE(found_mask[1]);
    ASSERT_EQ(result->get_column_by_index(0)->get(1).get_int32(), -1);

    ASSERT_TRUE(found_mask[2]);
    ASSERT_EQ(result->get_column_by_index(0)->get(2).get_int32(), 0);

    ASSERT_TRUE(found_mask[3]);
    ASSERT_EQ(result->get_column_by_index(0)->get(3).get_int32(), 100);
}

TEST_F(KVIndexTest, EmptyMultiGet) {
    auto schema = make_value_schema();
    auto keys = make_keys({1, 2, 3});
    auto value_chunk = make_value_chunk({10, 20, 30}, {"a", "b", "c"}, {0.1, 0.2, 0.3});
    auto sst = write_sst("empty_get.sst", schema, *keys, *value_chunk);

    ASSIGN_OR_ABORT(auto file, fs::new_random_access_file(sst.path));
    ASSIGN_OR_ABORT(auto reader, KVIndexReader::open(schema, file.get(), sst.file_size));

    std::vector<int64_t> query_keys = {};
    std::vector<bool> found_mask;
    ASSIGN_OR_ABORT(auto result, reader->multi_get(query_keys, &found_mask));

    ASSERT_EQ(result->num_rows(), 0);
    ASSERT_TRUE(found_mask.empty());
}

TEST_F(KVIndexTest, CrossBatchKeyOrdering) {
    auto schema = make_value_schema();
    std::string path = std::string(kTestDir) + "/cross_batch_order.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(path));
    KVIndexWriter writer(schema, file.get());

    auto keys1 = make_keys({10, 20});
    auto chunk1 = make_value_chunk({1, 2}, {"a", "b"}, {0.1, 0.2});
    ASSERT_OK(writer.add_chunk(*keys1, *chunk1));

    auto keys2 = make_keys({15});
    auto chunk2 = make_value_chunk({3}, {"c"}, {0.3});
    auto st = writer.add_chunk(*keys2, *chunk2);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
}

} // namespace starrocks
