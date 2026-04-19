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
#include "common/runtime_profile.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/tantivy_ffi/tantivy_ffi.h"
#include "storage/olap_common.h"
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
    if (_degraded) {
        return Status::OK();
    }
    if (!index_exists(_index_path)) {
        LOG(WARNING) << "Tantivy index not found, degrading to full scan: " << _index_path;
        _degraded = true;
        return Status::OK();
    }
    _reader = tantivy_reader_open(_index_path.c_str());
    if (_reader == nullptr) {
        LOG(WARNING) << "Failed to open tantivy reader, degrading to full scan: " << _index_path;
        _degraded = true;
        return Status::OK();
    }
    return Status::OK();
}

Status TantivyInvertedReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                    const void* query_value, InvertedIndexQueryType query_type,
                                    roaring::Roaring* bit_map) {
    SCOPED_RAW_TIMER(&stats->tantivy_query_ns);
    RETURN_IF_ERROR(_ensure_reader_opened());

    if (_degraded) {
        // Index corrupted or missing. MATCH predicates have no row-level evaluation fallback,
        // so the query will fail. Log a clear message for diagnosis.
        return Status::InternalError(
                fmt::format("Tantivy index is corrupted or missing at {}, cannot evaluate MATCH predicate. "
                            "Consider rebuilding the index via ALTER TABLE ... ALTER INDEX.",
                            _index_path));
    }

    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    std::string query_str(search_query->data, search_query->size);

    VLOG(2) << "Tantivy query: column=" << column_name << " query=" << query_str
            << " type=" << static_cast<int>(query_type);

    const char* kTantivyFieldName = TANTIVY_FIELD_NAME.c_str();

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
        // FFI returned null — could be user input error (e.g. invalid regex) or index issue.
        // Do NOT set _degraded here: this may be a transient/user error, not index corruption.
        // Index-level corruption is already caught by _ensure_reader_opened().
        LOG(WARNING) << "Tantivy query returned null: path=" << _index_path << " query=" << query_str
                     << " type=" << static_cast<int>(query_type);
        return Status::InternalError(
                fmt::format("Tantivy query failed for query '{}' (type={})", query_str, static_cast<int>(query_type)));
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
    stats->rows_tantivy_matched += count;
    return Status::OK();
}

Status TantivyInvertedReader::query_bm25(OlapReaderStatistics* stats, const std::string& column_name,
                                         const std::string& query, int32_t query_type, int32_t limit,
                                         std::vector<std::pair<uint32_t, float>>* results) {
    RETURN_IF_ERROR(_ensure_reader_opened());

    if (_degraded) {
        return Status::InternalError(
                fmt::format("Tantivy index is corrupted or missing at {}, cannot compute BM25 scores", _index_path));
    }

    const char* kTantivyFieldName = TANTIVY_FIELD_NAME.c_str();
    TantivyScoreResult* scores = tantivy_query_bm25(_reader, kTantivyFieldName, query.c_str(), query_type, limit);

    if (scores == nullptr) {
        LOG(WARNING) << "Tantivy BM25 query returned null: path=" << _index_path << " query=" << query;
        return Status::InternalError(fmt::format("Tantivy BM25 query failed for query '{}'", query));
    }

    uint32_t count = tantivy_score_count(scores);
    results->reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        results->emplace_back(tantivy_score_row_id(scores, i), tantivy_score_value(scores, i));
    }
    tantivy_score_destroy(scores);

    return Status::OK();
}

Status TantivyInvertedReader::query_null(OlapReaderStatistics* stats, const std::string& column_name,
                                         roaring::Roaring* bit_map) {
    RETURN_IF_ERROR(_ensure_reader_opened());

    if (_degraded) {
        return Status::InternalError(
                fmt::format("Tantivy index is corrupted or missing at {}, cannot evaluate IS NULL predicate. "
                            "Consider rebuilding the index via ALTER TABLE ... ALTER INDEX.",
                            _index_path));
    }

    std::string null_bitmap_path = _index_path + "/null_bitmap";

    auto file_or = fs::new_random_access_file(null_bitmap_path);
    if (!file_or.ok()) {
        if (file_or.status().is_not_found()) {
            // null_bitmap file not found within a valid index directory means no nulls were written.
            *bit_map = roaring::Roaring();
            return Status::OK();
        }
        // Other errors (IOError, permission denied, transient FS error) must propagate
        // to avoid false negatives where null rows are silently omitted.
        return file_or.status();
    }
    auto file = std::move(file_or.value());

    ASSIGN_OR_RETURN(auto file_size, file->get_size());

    faststring buf;
    buf.resize(file_size);
    RETURN_IF_ERROR(file->read_at_fully(0, buf.data(), file_size));

    *bit_map = roaring::Roaring::read(reinterpret_cast<const char*>(buf.data()), false);
    bit_map->runOptimize();
    return Status::OK();
}

} // namespace starrocks
