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

#include "exprs/gin_functions.h"

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <unordered_map>

#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/column_viewer.h"
#include "storage/index/inverted/tantivy_ffi/tantivy_ffi.h"
#include "types/datum.h"

namespace starrocks {

// Store the validated tokenizer name for reuse across rows (used by tokenize()).
struct TokenizerState {
    std::string tokenizer_name;
};

// Store validated constant arguments for bm25().
struct Bm25State {
    std::string query;
    std::string tokenizer_name;
    int32_t query_type = 0; // 0=any, 1=all, 2=phrase
};

Status GinFunctions::tokenize_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    // Arg 1 is the parser/tokenizer name (constant).
    auto column = context->get_constant_column(1);
    RETURN_IF(column == nullptr, Status::InvalidArgument("tokenize() requires a constant parser argument"));
    auto method = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::string tokenizer_name(method.data, method.size);

    // Validate tokenizer name.
    if (tokenizer_name != "standard" && tokenizer_name != "english" && tokenizer_name != "chinese" &&
        tokenizer_name != "none") {
        return Status::NotSupported("Unknown tokenizer '" + tokenizer_name +
                                    "'. Supported: 'standard', 'english', 'chinese', 'none'.");
    }

    auto* state = new TokenizerState();
    state->tokenizer_name = std::move(tokenizer_name);
    context->set_function_state(scope, state);
    return Status::OK();
}

Status GinFunctions::tokenize_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<TokenizerState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

Status GinFunctions::bm25_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    // Arg 1 (query) must be a constant.
    auto query_col = context->get_constant_column(1);
    RETURN_IF(query_col == nullptr, Status::InvalidArgument("bm25() requires a constant query argument"));
    auto query_val = ColumnHelper::get_const_value<TYPE_VARCHAR>(query_col);
    std::string query(query_val.data, query_val.size);

    // Arg 2 (tokenizer) is optional; if present, must be a constant.
    std::string tokenizer_name = "standard";
    if (context->get_num_args() >= 3) {
        auto tok_col = context->get_constant_column(2);
        RETURN_IF(tok_col == nullptr, Status::InvalidArgument("bm25() requires a constant tokenizer argument"));
        auto tok_val = ColumnHelper::get_const_value<TYPE_VARCHAR>(tok_col);
        tokenizer_name.assign(tok_val.data, tok_val.size);
    }

    // Validate tokenizer name.
    if (tokenizer_name != "standard" && tokenizer_name != "english" && tokenizer_name != "chinese" &&
        tokenizer_name != "none") {
        return Status::NotSupported("Unknown tokenizer '" + tokenizer_name +
                                    "'. Supported: 'standard', 'english', 'chinese', 'none'.");
    }

    // Arg 3 (query_type) is optional; if present, must be a constant.
    int32_t query_type = 0; // default: any (OR)
    if (context->get_num_args() >= 4) {
        auto qt_col = context->get_constant_column(3);
        RETURN_IF(qt_col == nullptr, Status::InvalidArgument("bm25() requires a constant query_type argument"));
        auto qt_val = ColumnHelper::get_const_value<TYPE_VARCHAR>(qt_col);
        std::string qt_str(qt_val.data, qt_val.size);
        if (qt_str == "any") {
            query_type = 0;
        } else if (qt_str == "all") {
            query_type = 1;
        } else if (qt_str == "phrase") {
            query_type = 2;
        } else {
            return Status::NotSupported("Unknown query_type '" + qt_str +
                                        "'. Supported: 'any', 'all', 'phrase'.");
        }
    }

    auto* state = new Bm25State();
    state->query = std::move(query);
    state->tokenizer_name = std::move(tokenizer_name);
    state->query_type = query_type;
    context->set_function_state(scope, state);
    return Status::OK();
}

Status GinFunctions::bm25_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<Bm25State*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

// TOKENIZE(text VARCHAR, parser VARCHAR) → ARRAY<VARCHAR>
StatusOr<ColumnPtr> GinFunctions::tokenize(FunctionContext* context, const starrocks::Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("tokenize() requires exactly 2 arguments: tokenize(text, parser)");
    }

    auto* state = reinterpret_cast<TokenizerState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (state == nullptr) {
        return Status::InternalError("tokenize(): prepare not called");
    }

    // Arg 0 is the text column.
    ColumnViewer<TYPE_VARCHAR> text_viewer(columns[0]);
    size_t num_rows = text_viewer.size();

    uint32_t offset = 0;
    auto array_offsets = UInt32Column::create();
    array_offsets->reserve(num_rows + 1);

    auto array_binary_column = BinaryColumn::create();
    auto null_array = NullColumn::create();

    const std::string& tokenizer_name = state->tokenizer_name;

    for (size_t row = 0; row < num_rows; ++row) {
        array_offsets->append(offset);

        if (text_viewer.is_null(row) || text_viewer.value(row).empty()) {
            null_array->append(1);
        } else {
            null_array->append(0);
            auto data = text_viewer.value(row);
            std::string text(data.data, data.size);

            if (tokenizer_name == "none") {
                // "none" tokenizer: return the original text as a single token.
                array_binary_column->append(Slice(text));
                offset++;
            } else {
                // Use tantivy FFI for tokenization.
                TantivyTokens* tokens = tantivy_tokenize(text.c_str(), tokenizer_name.c_str());
                if (tokens != nullptr) {
                    uint32_t count = tantivy_tokens_count(tokens);
                    for (uint32_t i = 0; i < count; i++) {
                        const char* token = tantivy_tokens_get(tokens, i);
                        if (token != nullptr) {
                            array_binary_column->append(Slice(token, strlen(token)));
                            offset++;
                        }
                    }
                    tantivy_tokens_destroy(tokens);
                }
            }
        }
    }
    array_offsets->append(offset);
    auto result_array = ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                            array_offsets);
    return NullableColumn::create(result_array, null_array);
}

// BM25(text VARCHAR, query VARCHAR [, tokenizer VARCHAR]) → DOUBLE
//
// WARNING: This is a batch-local BM25 implementation. IDF is computed over the current
// execution batch only, NOT over the full table. Scores from different batches are NOT
// directly comparable. For large tables with multiple batches, ORDER BY bm25(...) may
// produce unstable rankings. True global BM25 requires integration with the persistent
// GIN/inverted index read path (planned for a future phase).
//
// For single-batch queries (small tables or LIMIT), scores are correct standard BM25.
StatusOr<ColumnPtr> GinFunctions::bm25(FunctionContext* context, const starrocks::Columns& columns) {
    if (columns.size() < 2 || columns.size() > 4) {
        return Status::InvalidArgument(
                "bm25() requires 2-4 arguments: bm25(text, query [, tokenizer [, query_type]])");
    }

    auto* state = reinterpret_cast<Bm25State*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (state == nullptr) {
        return Status::InternalError("bm25(): prepare not called");
    }

    const std::string& query_str = state->query;
    const std::string& tokenizer_name = state->tokenizer_name;

    ColumnViewer<TYPE_VARCHAR> text_viewer(columns[0]);
    size_t num_rows = text_viewer.size();

    auto result_column = DoubleColumn::create();
    result_column->reserve(num_rows);
    auto null_column = NullColumn::create();
    null_column->reserve(num_rows);

    if (num_rows == 0) {
        return NullableColumn::create(result_column, null_column);
    }

    if (query_str.empty()) {
        // Empty query — all scores are NULL.
        for (size_t row = 0; row < num_rows; ++row) {
            result_column->append(0.0);
            null_column->append(1);
        }
        return NullableColumn::create(result_column, null_column);
    }

    // Create a temporary directory for the per-batch tantivy index.
    std::string tmp_dir = std::filesystem::temp_directory_path().string() + "/sr_bm25_" +
                          std::to_string(reinterpret_cast<uintptr_t>(context)) + "_" +
                          std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(tmp_dir);

    // RAII cleanup: remove temp directory on all exit paths (normal + error + exception).
    DeferOp cleanup([&tmp_dir]() { std::filesystem::remove_all(tmp_dir); });

    // Phase 1: Build a temporary tantivy index over all non-null rows in this batch.
    TantivyWriter* writer = tantivy_writer_create(tmp_dir.c_str(), "content", tokenizer_name.c_str());
    if (writer == nullptr) {
        return Status::InternalError("bm25(): failed to create tantivy writer (dir=" + tmp_dir +
                                     ", tokenizer=" + tokenizer_name + ")");
    }

    for (size_t row = 0; row < num_rows; ++row) {
        if (text_viewer.is_null(row)) {
            tantivy_writer_add_null(writer, static_cast<uint32_t>(row));
        } else {
            auto data = text_viewer.value(row);
            tantivy_writer_add_doc_with_len(writer, reinterpret_cast<const uint8_t*>(data.data),
                                            static_cast<uint32_t>(data.size), static_cast<uint32_t>(row));
        }
    }

    int32_t commit_rc = tantivy_writer_commit(writer);
    tantivy_writer_destroy(writer);

    if (commit_rc != 0) {
        return Status::InternalError("bm25(): tantivy writer commit failed (rc=" + std::to_string(commit_rc) + ")");
    }

    // Phase 2: Open reader and compute BM25 scores via tantivy_query_bm25().
    TantivyReader* reader = tantivy_reader_open(tmp_dir.c_str());
    if (reader == nullptr) {
        return Status::InternalError("bm25(): failed to open tantivy reader (dir=" + tmp_dir + ")");
    }

    // Use query_type from state (0=any, 1=all, 2=phrase), limit=0 (all results).
    TantivyScoreResult* scores = tantivy_query_bm25(reader, "content", query_str.c_str(), state->query_type, 0);

    // Build row_id → score map.
    std::unordered_map<uint32_t, float> score_map;
    if (scores != nullptr) {
        uint32_t score_count = tantivy_score_count(scores);
        for (uint32_t i = 0; i < score_count; i++) {
            uint32_t row_id = tantivy_score_row_id(scores, i);
            float score_val = tantivy_score_value(scores, i);
            score_map[row_id] = score_val;
        }
        tantivy_score_destroy(scores);
    }

    tantivy_reader_destroy(reader);

    // Phase 3: Populate result column from score map.
    for (size_t row = 0; row < num_rows; ++row) {
        if (text_viewer.is_null(row)) {
            result_column->append(0.0);
            null_column->append(1);
        } else {
            auto it = score_map.find(static_cast<uint32_t>(row));
            double score = (it != score_map.end()) ? static_cast<double>(it->second) : 0.0;
            result_column->append(score);
            null_column->append(0);
        }
    }

    return NullableColumn::create(result_column, null_column);
}

} // namespace starrocks
#include "gen_cpp/opcode/GinFunctions.inc"
