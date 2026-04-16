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

#include "storage/index/inverted/tantivy/tantivy_inverted_reader.h"

#include <fmt/format.h>

#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "common/logging.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/tantivy_ffi/tantivy_ffi.h"
#include "storage/rowset/options.h"
#include "types/logical_type.h"

namespace starrocks {

TantivyInvertedReader::TantivyInvertedReader(std::string path, uint32_t index_id)
        : InvertedReader(std::move(path), index_id) {}

TantivyInvertedReader::~TantivyInvertedReader() {
    if (_reader != nullptr) {
        tantivy_reader_destroy(_reader);
        _reader = nullptr;
    }
}

Status TantivyInvertedReader::create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                                     LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    if (!is_string_type(field_type)) {
        return Status::InvalidArgument(fmt::format("Tantivy does not support type {}", field_type));
    }
    *res = std::make_unique<TantivyInvertedReader>(path, tablet_index->index_id());
    return Status::OK();
}

Status TantivyInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator, const IndexReadOptions& index_opt) {
    *iterator = new InvertedIndexIterator(index_meta, this, index_opt.stats);
    return Status::OK();
}

Status TantivyInvertedReader::load(const IndexReadOptions& opt, void* meta) {
    return _ensure_reader_opened();
}

Status TantivyInvertedReader::_ensure_reader_opened() {
    if (_reader != nullptr) {
        return Status::OK();
    }
    if (!index_exists(_index_path)) {
        return Status::NotFound(fmt::format("Tantivy index path not found: {}", _index_path));
    }
    _reader = tantivy_reader_open(_index_path.c_str());
    if (_reader == nullptr) {
        return Status::InternalError(fmt::format("Failed to open tantivy reader at: {}", _index_path));
    }
    return Status::OK();
}

Status TantivyInvertedReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                    const void* query_value, InvertedIndexQueryType query_type,
                                    roaring::Roaring* bit_map) {
    RETURN_IF_ERROR(_ensure_reader_opened());

    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    // Slice data is not guaranteed to be null-terminated
    std::string query_str(search_query->data, search_query->size);

    VLOG(2) << "Tantivy query: column=" << column_name << " query=" << query_str
            << " type=" << static_cast<int>(query_type);

    // Each column's tantivy index lives in its own directory with a single field named "content".
    // The column_name parameter identifies the StarRocks column but the tantivy field is always "content".
    static const char* kTantivyFieldName = "content";

    TantivyBitmap* result = nullptr;
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
        // Exact single-term match without tokenization, consistent with CLucene MatchTermOperator
        result = tantivy_query_term(_reader, kTantivyFieldName, query_str.c_str());
        break;
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        result = tantivy_query_match_any(_reader, kTantivyFieldName, query_str.c_str());
        break;
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
        result = tantivy_query_match_all(_reader, kTantivyFieldName, query_str.c_str());
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        result = tantivy_query_phrase(_reader, kTantivyFieldName, query_str.c_str());
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
        result = tantivy_query_phrase_prefix(_reader, kTantivyFieldName, query_str.c_str());
        break;
    case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
        result = tantivy_query_regexp(_reader, kTantivyFieldName, query_str.c_str());
        break;
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        // Convert SQL LIKE wildcard to regex: % -> .*, _ -> ., escape others
        std::string regex;
        regex.reserve(query_str.size() * 2);
        for (char c : query_str) {
            switch (c) {
            case '%':
                regex += ".*";
                break;
            case '_':
                regex += '.';
                break;
            case '.':
            case '\\':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
            case '^':
            case '$':
            case '|':
            case '+':
            case '?':
            case '*':
                regex += '\\';
                regex += c;
                break;
            default:
                regex += c;
                break;
            }
        }
        result = tantivy_query_regexp(_reader, kTantivyFieldName, regex.c_str());
        break;
    }
    default:
        return Status::InvalidArgument(fmt::format("Unsupported tantivy query type: {}", static_cast<int>(query_type)));
    }

    if (result == nullptr) {
        return Status::InternalError("Tantivy query returned null result");
    }

    // Convert TantivyBitmap to roaring::Roaring
    uint32_t count = tantivy_bitmap_count(result);
    const uint32_t* row_ids = tantivy_bitmap_row_ids(result);
    roaring::Roaring roaring_result;
    if (count > 0 && row_ids != nullptr) {
        roaring_result.addMany(count, row_ids);
    }
    tantivy_bitmap_destroy(result);

    bit_map->swap(roaring_result);
    return Status::OK();
}

Status TantivyInvertedReader::query_null(OlapReaderStatistics* stats, const std::string& column_name,
                                         roaring::Roaring* bit_map) {
    std::string null_bitmap_path = _index_path + "/null_bitmap";

    FILE* fp = fopen(null_bitmap_path.c_str(), "rb");
    if (fp == nullptr) {
        // No null bitmap file means no nulls
        *bit_map = roaring::Roaring();
        return Status::OK();
    }

    fseek(fp, 0, SEEK_END);
    size_t file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    faststring buf;
    buf.resize(file_size);
    size_t read_size = fread(buf.data(), 1, file_size, fp);
    fclose(fp);

    if (read_size != file_size) {
        return Status::IOError("Failed to read null bitmap file");
    }

    *bit_map = roaring::Roaring::read(reinterpret_cast<const char*>(buf.data()), false);
    bit_map->runOptimize();
    return Status::OK();
}

} // namespace starrocks
