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

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "roaring/roaring.hh"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_plugin_factory.h"
#include "storage/index/inverted/tantivy/tantivy_inverted_reader.h"
#include "storage/index/inverted/tantivy/tantivy_inverted_writer.h"
#include "storage/index/inverted/tantivy/tantivy_plugin.h"
#include "storage/olap_define.h"
#include "storage/rowset/options.h"
#include "storage/tablet_index.h"
#include "types/type_info.h"

namespace starrocks {

class TantivyInvertedIndexTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path().string() + "/tantivy_test_" + std::to_string(getpid());
        std::filesystem::create_directories(_test_dir);

        _tablet_index = std::make_shared<TabletIndex>();
        _tablet_index->add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);
        _tablet_index->add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD);
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    Status write_test_data(const std::vector<std::string>& docs) {
        std::string index_dir = _test_dir + "/index";
        TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);

        std::unique_ptr<InvertedWriter> writer;
        RETURN_IF_ERROR(TantivyInvertedWriter::create(typeinfo, "content", index_dir, _tablet_index.get(), &writer));
        RETURN_IF_ERROR(writer->init());

        std::vector<Slice> slices;
        slices.reserve(docs.size());
        for (const auto& doc : docs) {
            slices.emplace_back(doc);
        }
        writer->add_values(slices.data(), slices.size());

        return writer->finish(nullptr, nullptr);
    }

    Status write_test_data_with_nulls(const std::vector<std::string>& docs, const std::vector<bool>& is_null) {
        std::string index_dir = _test_dir + "/index";
        TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);

        std::unique_ptr<InvertedWriter> writer;
        RETURN_IF_ERROR(TantivyInvertedWriter::create(typeinfo, "content", index_dir, _tablet_index.get(), &writer));
        RETURN_IF_ERROR(writer->init());

        size_t doc_idx = 0;
        for (size_t i = 0; i < is_null.size(); ++i) {
            if (is_null[i]) {
                writer->add_nulls(1);
            } else {
                Slice slice(docs[doc_idx]);
                writer->add_values(&slice, 1);
                ++doc_idx;
            }
        }

        return writer->finish(nullptr, nullptr);
    }

    std::string _test_dir;
    std::shared_ptr<TabletIndex> _tablet_index;
};

TEST_F(TantivyInvertedIndexTest, WriterBasicTest) {
    std::vector<std::string> docs;
    docs.reserve(100);
    for (int i = 0; i < 100; i++) {
        docs.push_back("document number " + std::to_string(i) + " with some text content");
    }

    ASSERT_OK(write_test_data(docs));

    // Verify index directory exists
    std::string index_dir = _test_dir + "/index";
    ASSERT_TRUE(std::filesystem::exists(index_dir));
    ASSERT_TRUE(std::filesystem::is_directory(index_dir));
}

TEST_F(TantivyInvertedIndexTest, ReaderMatchAnyTest) {
    // Write test data with specific keywords
    std::vector<std::string> docs = {
            "the quick brown fox jumps over the lazy dog", // 0
            "a fast brown cat sits on the mat",            // 1
            "the dog barks at the moon",                   // 2
            "a quick rabbit runs through the field",       // 3
            "the lazy cat sleeps all day",                 // 4
    };
    ASSERT_OK(write_test_data(docs));

    // Query for "quick" OR "cat" (MATCH_ANY)
    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    Slice query_slice("quick cat");
    roaring::Roaring result;
    ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::MATCH_ANY_QUERY, &result));

    // Should match: doc 0 (quick), doc 1 (cat), doc 3 (quick), doc 4 (cat)
    ASSERT_TRUE(result.contains(0));
    ASSERT_TRUE(result.contains(1));
    ASSERT_FALSE(result.contains(2));
    ASSERT_TRUE(result.contains(3));
    ASSERT_TRUE(result.contains(4));
}

TEST_F(TantivyInvertedIndexTest, ReaderMatchAllTest) {
    std::vector<std::string> docs = {
            "the quick brown fox jumps over the lazy dog", // 0
            "a fast brown cat sits on the mat",            // 1
            "the dog barks at the moon",                   // 2
            "brown dog plays in the park",                 // 3
            "the lazy cat sleeps all day",                 // 4
    };
    ASSERT_OK(write_test_data(docs));

    // Query for "brown" AND "dog" (MATCH_ALL)
    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    Slice query_slice("brown dog");
    roaring::Roaring result;
    ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::MATCH_ALL_QUERY, &result));

    // Should match: doc 0 (brown + dog), doc 3 (brown + dog)
    ASSERT_TRUE(result.contains(0));
    ASSERT_FALSE(result.contains(1));
    ASSERT_FALSE(result.contains(2));
    ASSERT_TRUE(result.contains(3));
    ASSERT_FALSE(result.contains(4));
}

TEST_F(TantivyInvertedIndexTest, ReaderPhraseTest) {
    std::vector<std::string> docs = {
            "the quick brown fox jumps over the lazy dog", // 0
            "brown fox is a type of animal",               // 1
            "the fox is brown colored",                    // 2
            "a quick brown fox ran away",                  // 3
    };
    ASSERT_OK(write_test_data(docs));

    // Query for exact phrase "brown fox"
    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    Slice query_slice("brown fox");
    roaring::Roaring result;
    ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &result));

    // Should match: doc 0 ("brown fox"), doc 1 ("brown fox"), doc 3 ("brown fox")
    // Doc 2 has "fox is brown" - not the phrase "brown fox"
    ASSERT_TRUE(result.contains(0));
    ASSERT_TRUE(result.contains(1));
    ASSERT_FALSE(result.contains(2));
    ASSERT_TRUE(result.contains(3));
}

TEST_F(TantivyInvertedIndexTest, ReaderPhrasePrefixTest) {
    std::vector<std::string> docs = {
            "the quick brown fox jumps",     // 0
            "brown foxes are common",        // 1
            "the fox is brown",              // 2
            "a quick brown foxhound ran",    // 3
    };
    ASSERT_OK(write_test_data(docs));

    // Query for phrase prefix "brown fox" - should match "brown fox*"
    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    Slice query_slice("brown fox");
    roaring::Roaring result;
    ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                            &result));

    // Should match docs where "brown" is followed by a word starting with "fox"
    // Doc 0: "brown fox" - yes
    // Doc 1: "brown foxes" - yes (foxes starts with fox)
    // Doc 2: "fox is brown" - no (wrong order)
    // Doc 3: "brown foxhound" - yes (foxhound starts with fox)
    ASSERT_TRUE(result.contains(0));
    ASSERT_TRUE(result.contains(1));
    ASSERT_FALSE(result.contains(2));
    ASSERT_TRUE(result.contains(3));
}

TEST_F(TantivyInvertedIndexTest, ReaderRegexpTest) {
    std::vector<std::string> docs = {
            "error code 404 not found",   // 0
            "warning code 200 success",   // 1
            "error code 500 server fail", // 2
            "info code 301 redirect",     // 3
    };
    ASSERT_OK(write_test_data(docs));

    // Query for regexp matching "error"
    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    Slice query_slice("err.*");
    roaring::Roaring result;
    ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::MATCH_REGEXP_QUERY, &result));

    // Should match docs containing words matching "err.*": doc 0, doc 2
    ASSERT_TRUE(result.contains(0));
    ASSERT_FALSE(result.contains(1));
    ASSERT_TRUE(result.contains(2));
    ASSERT_FALSE(result.contains(3));
}

TEST_F(TantivyInvertedIndexTest, ReaderEqualQueryTest) {
    std::vector<std::string> docs = {
            "hello",         // 0
            "world",         // 1
            "hello world",   // 2
            "foo",           // 3
    };
    ASSERT_OK(write_test_data(docs));

    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    // EQUAL_QUERY for "hello" — single term exact match (no tokenization)
    {
        Slice query_slice("hello");
        roaring::Roaring result;
        ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::EQUAL_QUERY, &result));

        ASSERT_TRUE(result.contains(0));
        ASSERT_FALSE(result.contains(1));
        ASSERT_TRUE(result.contains(2));
        ASSERT_FALSE(result.contains(3));
    }

    // EQUAL_QUERY for "hello world" — treated as a single term (not tokenized).
    // Unlike MATCH_ALL which would match doc 2 (has both "hello" AND "world"),
    // term query looks for the literal term "hello world" which doesn't exist after indexing.
    {
        Slice query_slice("hello world");
        roaring::Roaring result;
        ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::EQUAL_QUERY, &result));

        // No doc contains "hello world" as a single indexed term
        ASSERT_EQ(0, result.cardinality());
    }
}

TEST_F(TantivyInvertedIndexTest, ReaderWildcardQueryTest) {
    std::vector<std::string> docs = {
            "apple pie is delicious",  // 0
            "application started",     // 1
            "banana split",            // 2
            "apply the patch",         // 3
    };
    ASSERT_OK(write_test_data(docs));

    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    // MATCH_WILDCARD_QUERY: "appl%" — SQL LIKE pattern, matches words starting with "appl"
    Slice query_slice("appl%");
    roaring::Roaring result;
    ASSERT_OK(reader->query(nullptr, "content", &query_slice, InvertedIndexQueryType::MATCH_WILDCARD_QUERY, &result));

    // Should match: doc 0 (apple), doc 1 (application), doc 3 (apply)
    ASSERT_TRUE(result.contains(0));
    ASSERT_TRUE(result.contains(1));
    ASSERT_FALSE(result.contains(2));
    ASSERT_TRUE(result.contains(3));
}

TEST_F(TantivyInvertedIndexTest, NullBitmapTest) {
    std::vector<std::string> docs = {"hello world", "foo bar"};
    std::vector<bool> is_null = {false, true, false, true, true};
    ASSERT_OK(write_test_data_with_nulls(docs, is_null));

    std::string index_dir = _test_dir + "/index";
    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);

    roaring::Roaring null_bitmap;
    ASSERT_OK(reader->query_null(nullptr, "content", &null_bitmap));

    // Null rows: 1, 3, 4
    ASSERT_FALSE(null_bitmap.contains(0));
    ASSERT_TRUE(null_bitmap.contains(1));
    ASSERT_FALSE(null_bitmap.contains(2));
    ASSERT_TRUE(null_bitmap.contains(3));
    ASSERT_TRUE(null_bitmap.contains(4));
    ASSERT_EQ(3, null_bitmap.cardinality());
}

TEST_F(TantivyInvertedIndexTest, EqualQueryCaseSensitiveWithParserNone) {
    // Use parser=none which preserves case during indexing
    auto tablet_index_none = std::make_shared<TabletIndex>();
    tablet_index_none->add_common_properties(INVERTED_IMP_KEY, TYPE_TANTIVY);
    tablet_index_none->add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);

    std::string index_dir = _test_dir + "/index_none";
    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);

    std::unique_ptr<InvertedWriter> writer;
    ASSERT_OK(TantivyInvertedWriter::create(typeinfo, "content", index_dir, tablet_index_none.get(), &writer));
    ASSERT_OK(writer->init());

    // Write docs with mixed case — parser=none stores them as-is
    std::vector<Slice> slices;
    std::string d0 = "Hello";
    std::string d1 = "hello";
    std::string d2 = "HELLO";
    slices.emplace_back(d0);
    slices.emplace_back(d1);
    slices.emplace_back(d2);
    writer->add_values(slices.data(), slices.size());
    ASSERT_OK(writer->finish(nullptr, nullptr));

    auto reader = std::make_unique<TantivyInvertedReader>(index_dir, 0);
    ASSERT_OK(reader->load(IndexReadOptions{}, nullptr));

    // "Hello" should match only doc 0 (exact case with parser=none)
    {
        Slice q("Hello");
        roaring::Roaring result;
        ASSERT_OK(reader->query(nullptr, "content", &q, InvertedIndexQueryType::EQUAL_QUERY, &result));
        ASSERT_TRUE(result.contains(0));
        ASSERT_FALSE(result.contains(1));
        ASSERT_FALSE(result.contains(2));
    }

    // "hello" should match only doc 1
    {
        Slice q("hello");
        roaring::Roaring result;
        ASSERT_OK(reader->query(nullptr, "content", &q, InvertedIndexQueryType::EQUAL_QUERY, &result));
        ASSERT_FALSE(result.contains(0));
        ASSERT_TRUE(result.contains(1));
        ASSERT_FALSE(result.contains(2));
    }
}

TEST_F(TantivyInvertedIndexTest, PluginFactoryTest) {
    auto res = InvertedPluginFactory::get_plugin(InvertedImplementType::TANTIVY);
    ASSERT_TRUE(res.ok());
    ASSERT_NE(nullptr, res.value());
}

TEST_F(TantivyInvertedIndexTest, UnsupportedTypeTest) {
    TypeInfoPtr typeinfo = get_type_info(TYPE_INT);
    std::unique_ptr<InvertedWriter> writer;
    auto st = TantivyInvertedWriter::create(typeinfo, "col", _test_dir, _tablet_index.get(), &writer);
    ASSERT_FALSE(st.ok());
}

} // namespace starrocks
