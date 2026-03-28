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

#include "storage/rowset/fsst_page.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "types/type_info.h"

namespace starrocks {

class FSSTPageTest : public testing::Test {
public:
    // Build a column-level symbol table from the input strings.
    static fsst_detail::SymbolTable build_symbol_table(const std::vector<std::string>& input) {
        std::vector<std::pair<const uint8_t*, size_t>> string_ptrs;
        string_ptrs.reserve(input.size());
        for (const auto& s : input) {
            string_ptrs.emplace_back(reinterpret_cast<const uint8_t*>(s.data()), s.size());
        }
        return fsst_detail::build_symbol_table(string_ptrs);
    }

    // Build an FSST page from input strings using the given symbol table.
    static OwnedSlice build_page(const std::vector<std::string>& input,
                                 const fsst_detail::SymbolTable& st) {
        PageBuilderOptions opts;
        opts.data_page_size = 256 * 1024;
        FSSTPageBuilder<TYPE_VARCHAR> builder(opts);
        builder.set_symbol_table(&st);

        std::vector<Slice> slices;
        slices.reserve(input.size());
        for (const auto& s : input) {
            slices.emplace_back(s.data(), s.size());
        }
        builder.add(reinterpret_cast<const uint8_t*>(slices.data()), slices.size());
        return builder.finish()->build();
    }

    void test_round_trip(const std::vector<std::string>& input) {
        auto st = build_symbol_table(input);
        OwnedSlice page = build_page(input, st);

        FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
        decoder.set_symbol_table(&st);
        ASSERT_TRUE(decoder.init().ok());
        ASSERT_EQ(0, decoder.current_index());
        ASSERT_EQ(input.size(), decoder.count());

        auto column = BinaryColumn::create();
        size_t size_to_fetch = input.size();
        ASSERT_TRUE(decoder.next_batch(&size_to_fetch, column.get()).ok());
        ASSERT_EQ(input.size(), size_to_fetch);

        for (size_t i = 0; i < input.size(); ++i) {
            ASSERT_EQ(input[i], column->get_slice(i).to_string()) << "index=" << i;
        }
    }
};

// Test basic roundtrip with URL-like strings
TEST_F(FSSTPageTest, BasicRoundTrip) {
    std::vector<std::string> urls;
    for (int i = 0; i < 100; ++i) {
        urls.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }
    test_round_trip(urls);
}

// Test roundtrip with empty strings
TEST_F(FSSTPageTest, EmptyStrings) {
    std::vector<std::string> input = {"", "hello", "", "world", ""};
    test_round_trip(input);
}

// Test roundtrip with single string
TEST_F(FSSTPageTest, SingleString) {
    std::vector<std::string> input = {"http://example.com/test"};
    test_round_trip(input);
}

// Test roundtrip with high-cardinality strings
TEST_F(FSSTPageTest, HighCardinality) {
    std::vector<std::string> input;
    for (int i = 0; i < 500; ++i) {
        input.push_back("path/to/resource/" + std::to_string(i) + "/detail?key=" + std::to_string(i * 7));
    }
    test_round_trip(input);
}

// Test compressed predicate evaluation: kEQ
TEST_F(FSSTPageTest, CompressedPredicateEQ) {
    std::vector<std::string> input;
    for (int i = 0; i < 100; ++i) {
        input.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    // Create EQ predicate for a known value
    std::string target = "http://example.com/api/v5/user/5";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_eq_predicate(type_info, 0, Slice(target.data(), target.size())));

    SparseRange<> row_ranges;
    ASSERT_TRUE(decoder.evaluate_predicate_compressed(pred.get(), &row_ranges).ok());

    // Should find exactly one match at index 5
    ASSERT_EQ(1, row_ranges.span_size()) << "Should find exactly 1 match";

    // Verify via decode that the matching row is correct
    auto column = BinaryColumn::create();
    SparseRangeIterator<> iter = row_ranges.new_iterator();
    while (iter.has_more()) {
        Range<> r = iter.next(1);
        decoder.seek_to_position_in_page(r.begin());
        size_t n = 1;
        decoder.next_batch(&n, column.get());
    }
    ASSERT_EQ(target, column->get_slice(0).to_string());
}

// Test compressed predicate: kNE
TEST_F(FSSTPageTest, CompressedPredicateNE) {
    std::vector<std::string> input = {"aaa", "bbb", "aaa", "ccc", "aaa"};

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    std::string target = "aaa";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_ne_predicate(type_info, 0, Slice(target.data(), target.size())));

    SparseRange<> row_ranges;
    ASSERT_TRUE(decoder.evaluate_predicate_compressed(pred.get(), &row_ranges).ok());

    // "aaa" appears at indices 0, 2, 4 -> NE should match indices 1, 3
    ASSERT_EQ(2, row_ranges.span_size()) << "NE should match 2 rows";
}

// Test compressed predicate: non-equality (kGT) returns all rows
TEST_F(FSSTPageTest, CompressedPredicateGTReturnsAll) {
    std::vector<std::string> input = {"aaa", "bbb", "ccc"};

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    std::string target = "bbb";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_gt_predicate(type_info, 0, Slice(target.data(), target.size())));

    SparseRange<> row_ranges;
    ASSERT_TRUE(decoder.evaluate_predicate_compressed(pred.get(), &row_ranges).ok());

    // GT is not supported by FSST compressed eval, should return all rows
    ASSERT_EQ(3, row_ranges.span_size()) << "Unsupported predicate should return all rows";
}

// Test next_batch_with_filter with EQ predicate (Level 1: encoded-space evaluation)
TEST_F(FSSTPageTest, NextBatchWithFilterEQ) {
    std::vector<std::string> input;
    for (int i = 0; i < 200; ++i) {
        input.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    // Search for a specific value
    std::string target = "http://example.com/api/v5/user/5";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_eq_predicate(type_info, 0, Slice(target.data(), target.size())));

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    auto column = BinaryColumn::create();
    SparseRange<> range;
    range.add(Range<>(0, 200));

    std::vector<uint8_t> selection(200, 0);
    std::vector<uint16_t> selected_idx(200, 0);
    std::vector<const ColumnPredicate*> preds = {pred.get()};

    ASSERT_TRUE(decoder.next_batch_with_filter(column.get(), range, preds, nullptr, selection.data(),
                                               selected_idx.data())
                        .ok());

    // Should find exactly 1 match
    ASSERT_EQ(1, column->size());
    ASSERT_EQ(target, column->get_slice(0).to_string());
}

// Test next_batch_with_filter with NE predicate (Level 1: encoded-space evaluation)
TEST_F(FSSTPageTest, NextBatchWithFilterNE) {
    std::vector<std::string> input = {"aaa", "bbb", "aaa", "ccc", "aaa"};

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    std::string target = "aaa";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_ne_predicate(type_info, 0, Slice(target.data(), target.size())));

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    auto column = BinaryColumn::create();
    SparseRange<> range;
    range.add(Range<>(0, 5));

    std::vector<uint8_t> selection(5, 0);
    std::vector<uint16_t> selected_idx(5, 0);
    std::vector<const ColumnPredicate*> preds = {pred.get()};

    ASSERT_TRUE(
            decoder.next_batch_with_filter(column.get(), range, preds, nullptr, selection.data(), selected_idx.data())
                    .ok());

    // "aaa" at 0,2,4 → NE matches "bbb" at 1 and "ccc" at 3
    ASSERT_EQ(2, column->size());
    ASSERT_EQ("bbb", column->get_slice(0).to_string());
    ASSERT_EQ("ccc", column->get_slice(1).to_string());
}

// Test lazy decode: init() does NOT decode, next_batch() triggers decode
TEST_F(FSSTPageTest, LazyDecode) {
    std::vector<std::string> input;
    for (int i = 0; i < 50; ++i) {
        input.push_back("path/to/resource/" + std::to_string(i));
    }

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    // init() should succeed without decoding
    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(50, decoder.count());

    // Read a partial range — should trigger lazy decode and return correct results
    decoder.seek_to_position_in_page(10);
    auto column = BinaryColumn::create();
    size_t n = 5;
    ASSERT_TRUE(decoder.next_batch(&n, column.get()).ok());
    ASSERT_EQ(5, n);

    for (size_t i = 0; i < 5; ++i) {
        ASSERT_EQ(input[10 + i], column->get_slice(i).to_string());
    }
}

// Differential test: compressed predicate vs decode-then-compare
TEST_F(FSSTPageTest, DifferentialTestEQ) {
    std::vector<std::string> input;
    for (int i = 0; i < 200; ++i) {
        input.push_back("http://example.com/api/v" + std::to_string(i % 20) + "/user/" + std::to_string(i));
    }

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    // Test EQ for each string
    auto type_info = get_type_info(TYPE_VARCHAR);
    for (const auto& target : input) {
        FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
        decoder.set_symbol_table(&st);
        ASSERT_TRUE(decoder.init().ok());

        auto pred = std::unique_ptr<ColumnPredicate>(
                new_column_eq_predicate(type_info, 0, Slice(target.data(), target.size())));

        SparseRange<> row_ranges;
        ASSERT_TRUE(decoder.evaluate_predicate_compressed(pred.get(), &row_ranges).ok());

        // Count matches via standard comparison
        int expected_matches = 0;
        for (const auto& s : input) {
            if (s == target) expected_matches++;
        }

        ASSERT_EQ(expected_matches, row_ranges.span_size())
                << "Mismatch for target: " << target;
    }
}

// Test EQ on nullable column: NULL rows should be filtered out
TEST_F(FSSTPageTest, NextBatchWithFilterEQNullable) {
    std::vector<std::string> input = {"aaa", "bbb", "aaa", "ccc", "aaa"};

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    // EQ predicate for "aaa"
    std::string target = "aaa";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_eq_predicate(type_info, 0, Slice(target.data(), target.size())));

    auto column = BinaryColumn::create();
    SparseRange<> range;
    range.add(Range<>(0, 5));

    // Mark rows 0 and 4 as NULL (both are "aaa" in the data)
    std::vector<uint8_t> null_data = {1, 0, 0, 0, 1};
    std::vector<uint8_t> selection(5, 0);
    std::vector<uint16_t> selected_idx(5, 0);
    std::vector<const ColumnPredicate*> preds = {pred.get()};

    ASSERT_TRUE(decoder.next_batch_with_filter(column.get(), range, preds, null_data.data(), selection.data(),
                                               selected_idx.data())
                        .ok());

    // "aaa" at indices 0, 2, 4. Indices 0 and 4 are NULL → only index 2 matches.
    ASSERT_EQ(1, column->size());
    ASSERT_EQ("aaa", column->get_slice(0).to_string());
}

// Test mixed predicates (EQ + GT) — should fall back to lazy full decode path
TEST_F(FSSTPageTest, NextBatchWithFilterMixedPredicates) {
    std::vector<std::string> input;
    for (int i = 0; i < 100; ++i) {
        input.push_back("item_" + std::to_string(i));
    }

    auto st = build_symbol_table(input);
    OwnedSlice page = build_page(input, st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&st);
    ASSERT_TRUE(decoder.init().ok());

    // Mix of EQ and GT predicates → forces fallback to decode path
    auto type_info = get_type_info(TYPE_VARCHAR);
    std::string target_eq = "item_50";
    std::string target_gt = "item_3";
    auto pred_eq = std::unique_ptr<ColumnPredicate>(
            new_column_eq_predicate(type_info, 0, Slice(target_eq.data(), target_eq.size())));
    auto pred_gt = std::unique_ptr<ColumnPredicate>(
            new_column_gt_predicate(type_info, 0, Slice(target_gt.data(), target_gt.size())));

    auto column = BinaryColumn::create();
    SparseRange<> range;
    range.add(Range<>(0, 100));

    std::vector<uint8_t> selection(100, 0);
    std::vector<uint16_t> selected_idx(100, 0);
    std::vector<const ColumnPredicate*> preds = {pred_eq.get(), pred_gt.get()};

    ASSERT_TRUE(decoder.next_batch_with_filter(column.get(), range, preds, nullptr, selection.data(),
                                               selected_idx.data())
                        .ok());

    // EQ "item_50" AND GT "item_3" → only "item_50" passes both
    // (lexicographic: "item_50" > "item_3" is true)
    ASSERT_EQ(1, column->size());
    ASSERT_EQ("item_50", column->get_slice(0).to_string());
}

// Test per-column symbol table: header stores only string_count.
TEST_F(FSSTPageTest, PerColumnSymbolTable) {
    std::vector<std::string> input;
    for (int i = 0; i < 100; ++i) {
        input.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }

    auto col_st = build_symbol_table(input);
    OwnedSlice page = build_page(input, col_st);

    // Verify page header stores string_count only
    const auto* p = reinterpret_cast<const uint8_t*>(page.slice().data);
    uint32_t string_count = decode_fixed32_le(p);
    ASSERT_EQ(100, string_count);

    // Decode with shared symbol table
    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&col_st);
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(100, decoder.count());

    auto column = BinaryColumn::create();
    size_t n = 100;
    ASSERT_TRUE(decoder.next_batch(&n, column.get()).ok());
    ASSERT_EQ(100, n);

    for (size_t i = 0; i < input.size(); ++i) {
        ASSERT_EQ(input[i], column->get_slice(i).to_string()) << "index=" << i;
    }
}

// Test per-column mode: symbol table can be set after init() (before decode).
// This matches the actual runtime flow where parse_page() calls init() first,
// then ScalarColumnIterator sets the symbol table.
TEST_F(FSSTPageTest, PerColumnSymbolTableSetAfterInit) {
    std::vector<std::string> input = {"hello", "world"};

    auto col_st = build_symbol_table(input);
    OwnedSlice page = build_page(input, col_st);

    // init() succeeds without symbol table (only parses header + offsets)
    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(2, decoder.count());

    // Set symbol table after init, then decode successfully
    decoder.set_symbol_table(&col_st);
    auto column = BinaryColumn::create();
    size_t n = 2;
    ASSERT_TRUE(decoder.next_batch(&n, column.get()).ok());
    ASSERT_EQ(2, n);
    ASSERT_EQ("hello", column->get_slice(0).to_string());
    ASSERT_EQ("world", column->get_slice(1).to_string());
}

// Test per-column mode: compressed predicate evaluation works with shared table
TEST_F(FSSTPageTest, PerColumnCompressedPredicate) {
    std::vector<std::string> input;
    for (int i = 0; i < 100; ++i) {
        input.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }

    auto col_st = build_symbol_table(input);
    OwnedSlice page = build_page(input, col_st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&col_st);
    ASSERT_TRUE(decoder.init().ok());

    // EQ predicate
    std::string target = "http://example.com/api/v5/user/5";
    auto type_info = get_type_info(TYPE_VARCHAR);
    auto pred = std::unique_ptr<ColumnPredicate>(
            new_column_eq_predicate(type_info, 0, Slice(target.data(), target.size())));

    SparseRange<> row_ranges;
    ASSERT_TRUE(decoder.evaluate_predicate_compressed(pred.get(), &row_ranges).ok());
    ASSERT_EQ(1, row_ranges.span_size());
}

// Test two per-column pages built from same input produce identical decode results.
TEST_F(FSSTPageTest, PerColumnConsistency) {
    std::vector<std::string> input;
    for (int i = 0; i < 50; ++i) {
        input.push_back("path/to/resource/" + std::to_string(i) + "/detail?key=" + std::to_string(i * 7));
    }

    auto col_st = build_symbol_table(input);

    std::vector<Slice> slices;
    for (const auto& s : input) {
        slices.emplace_back(s.data(), s.size());
    }

    // Build two pages with same symbol table
    PageBuilderOptions opts;
    opts.data_page_size = 256 * 1024;

    FSSTPageBuilder<TYPE_VARCHAR> builder_a(opts);
    builder_a.set_symbol_table(&col_st);
    builder_a.add(reinterpret_cast<const uint8_t*>(slices.data()), slices.size());
    OwnedSlice page_a = builder_a.finish()->build();

    FSSTPageBuilder<TYPE_VARCHAR> builder_b(opts);
    builder_b.set_symbol_table(&col_st);
    builder_b.add(reinterpret_cast<const uint8_t*>(slices.data()), slices.size());
    OwnedSlice page_b = builder_b.finish()->build();

    // Decode both and compare
    FSSTPageDecoder<TYPE_VARCHAR> decoder_a(page_a.slice());
    decoder_a.set_symbol_table(&col_st);
    ASSERT_TRUE(decoder_a.init().ok());
    auto col_a = BinaryColumn::create();
    size_t n_a = 50;
    ASSERT_TRUE(decoder_a.next_batch(&n_a, col_a.get()).ok());

    FSSTPageDecoder<TYPE_VARCHAR> decoder_b(page_b.slice());
    decoder_b.set_symbol_table(&col_st);
    ASSERT_TRUE(decoder_b.init().ok());
    auto col_b = BinaryColumn::create();
    size_t n_b = 50;
    ASSERT_TRUE(decoder_b.next_batch(&n_b, col_b.get()).ok());

    ASSERT_EQ(n_a, n_b);
    for (size_t i = 0; i < n_a; ++i) {
        ASSERT_EQ(col_a->get_slice(i).to_string(), col_b->get_slice(i).to_string()) << "index=" << i;
    }
}

// Test serialization/deserialization of symbol table (simulates column metadata round-trip)
TEST_F(FSSTPageTest, SymbolTableSerializeRoundTrip) {
    std::vector<std::string> input;
    for (int i = 0; i < 100; ++i) {
        input.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }

    // Build symbol table
    auto original_st = build_symbol_table(input);

    // Serialize and deserialize (simulates ColumnMetaPB storage)
    std::string serialized = original_st.serialize();
    ASSERT_GT(serialized.size(), 0);

    fsst_detail::SymbolTable restored_st;
    size_t consumed = restored_st.deserialize(reinterpret_cast<const uint8_t*>(serialized.data()), serialized.size());
    ASSERT_EQ(consumed, serialized.size());

    // Build page with original, decode with restored — should produce identical results
    OwnedSlice page = build_page(input, original_st);

    FSSTPageDecoder<TYPE_VARCHAR> decoder(page.slice());
    decoder.set_symbol_table(&restored_st);
    ASSERT_TRUE(decoder.init().ok());

    auto column = BinaryColumn::create();
    size_t n = 100;
    ASSERT_TRUE(decoder.next_batch(&n, column.get()).ok());
    ASSERT_EQ(100, n);

    for (size_t i = 0; i < input.size(); ++i) {
        ASSERT_EQ(input[i], column->get_slice(i).to_string()) << "index=" << i;
    }
}

} // namespace starrocks
