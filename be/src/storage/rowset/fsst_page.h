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

#pragma once

#include <cstring>
#include <string>
#include <vector>

#include "base/coding.h"
#include "base/string/faststring.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "storage/column_predicate.h"
#include "storage/range.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "util/fsst_encoding.h"

namespace starrocks {

// FSST (Fast Static Symbol Table) encoding for VARCHAR/CHAR columns.
//
// Page format:
//   [FSST Header]
//     string_count       (uint32)
//   [Offsets Array: uint32[string_count + 1]]  (offsets into encoded data)
//   [Encoded Strings: FSST-compressed bytes]
//
// Current format is per-column: symbol table is stored once in
// ColumnMetaPB.fsst_symbol_table (one per VARCHAR column, shared by all pages)
// and injected into each decoder by iterator before decode.

template <LogicalType Type>
class FSSTPageBuilder final : public PageBuilder {
public:
    explicit FSSTPageBuilder(const PageBuilderOptions& options) : _options(options) {}

    ~FSSTPageBuilder() override = default;

    // Set shared column-level symbol table (required).
    // The pointer must remain valid for the lifetime of this builder.
    void set_symbol_table(const fsst_detail::SymbolTable* table) { _shared_symbol_table = table; }

    bool is_page_full() override {
        return _count > 0 && _estimated_raw_size >= _options.data_page_size;
    }

    uint32_t add(const uint8_t* vals, uint32_t count) override {
        DCHECK(!_finished);
        if (count == 0) return 0;

        const auto* slices = reinterpret_cast<const Slice*>(vals);
        for (uint32_t i = 0; i < count; ++i) {
            if (is_page_full()) return i;
            _raw_strings.emplace_back(slices[i].data, slices[i].size);
            _estimated_raw_size += slices[i].size;
            _count++;
        }
        return count;
    }

    faststring* finish() override {
        DCHECK(!_finished);
        _finished = true;
        _buf.clear();

        if (_count == 0) {
            // Write header with count=0
            _buf.resize(sizeof(uint32_t));
            auto* p = _buf.data();
            encode_fixed32_le(p, 0);
            return &_buf;
        }

        CHECK(_shared_symbol_table != nullptr) << "FSST per-column mode requires shared symbol table";

        // 1. Encode all strings using the shared symbol table
        std::vector<std::string> encoded_strings;
        encoded_strings.reserve(_count);
        for (const auto& s : _raw_strings) {
            encoded_strings.push_back(
                    _shared_symbol_table->encode(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
        }

        // 2. Build offsets array
        std::vector<uint32_t> offsets(_count + 1);
        uint32_t offset = 0;
        for (uint32_t i = 0; i < _count; ++i) {
            offsets[i] = offset;
            offset += static_cast<uint32_t>(encoded_strings[i].size());
        }
        offsets[_count] = offset;

        // 3. Write page
        // Header: string_count (4 bytes)
        size_t header_size = sizeof(uint32_t);
        size_t offsets_size = (_count + 1) * sizeof(uint32_t);
        size_t total = header_size + offsets_size + offset;
        _buf.resize(total);

        auto* p = _buf.data();
        // string_count
        encode_fixed32_le(p, _count);
        p += sizeof(uint32_t);
        // offsets array
        for (uint32_t i = 0; i <= _count; ++i) {
            encode_fixed32_le(p, offsets[i]);
            p += sizeof(uint32_t);
        }
        // encoded strings
        for (const auto& enc : encoded_strings) {
            memcpy(p, enc.data(), enc.size());
            p += enc.size();
        }

        // Store first/last values (raw, for zone map)
        if (!_raw_strings.empty()) {
            _first_value.assign_copy(reinterpret_cast<const uint8_t*>(_raw_strings.front().data()),
                                     _raw_strings.front().size());
            _last_value.assign_copy(reinterpret_cast<const uint8_t*>(_raw_strings.back().data()),
                                    _raw_strings.back().size());
        }

        return &_buf;
    }

    void reset() override {
        _count = 0;
        _estimated_raw_size = 0;
        _finished = false;
        _raw_strings.clear();
        _buf.clear();
    }

    uint32_t count() const override { return _count; }

    uint64_t size() const override { return _estimated_raw_size; }

    Status get_first_value(void* value) const override {
        if (_count == 0) return Status::NotFound("page is empty");
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        if (_count == 0) return Status::NotFound("page is empty");
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
        return Status::OK();
    }

private:
    PageBuilderOptions _options;
    uint32_t _count{0};
    size_t _estimated_raw_size{0};
    bool _finished{false};
    faststring _buf;
    faststring _first_value;
    faststring _last_value;
    std::vector<std::string> _raw_strings;
    const fsst_detail::SymbolTable* _shared_symbol_table{nullptr};
};

template <LogicalType Type>
class FSSTPageDecoder final : public PageDecoder {
public:
    explicit FSSTPageDecoder(Slice data) : _data(data) {}

    ~FSSTPageDecoder() override = default;

    // Set shared column-level symbol table. Must be set before any decode/encode
    // operation, but may be set before or after init() (init only parses the page
    // header and offsets, no symbol table needed).
    void set_symbol_table(const fsst_detail::SymbolTable* table) {
        _shared_symbol_table = table;
        _active_symbol_table = table;
    }

    // Set external cache for encoded predicate values (owned by iterator,
    // persists across pages). Eliminates per-page encode_flat overhead.
    void set_encoded_pred_cache(std::string* cached_encoded, std::string* cached_raw) {
        _ext_cached_encoded_pred = cached_encoded;
        _ext_cached_pred_raw = cached_raw;
    }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < sizeof(uint32_t)) {
            return Status::Corruption("FSST page too small");
        }

        const auto* p = reinterpret_cast<const uint8_t*>(_data.data);
        _num_elements = decode_fixed32_le(p);
        p += sizeof(uint32_t);

        if (_num_elements == 0) {
            _parsed = true;
            return Status::OK();
        }

        size_t header_size = sizeof(uint32_t);
        // Note: symbol table may not be set yet at init() time — it is injected
        // by ScalarColumnIterator after page parsing. Checked in decode/evaluate.

        // Parse offsets array — zero-copy pointer into page buffer.
        size_t offsets_size = (_num_elements + 1) * sizeof(uint32_t);
        if (_data.size < header_size + offsets_size) {
            return Status::Corruption("FSST page too small for offsets");
        }
        _enc_offsets_data = p;
        p += offsets_size;

        _encoded_data = p;
        _encoded_data_size = _data.size - header_size - offsets_size;

        if (_enc_offset(_num_elements) > _encoded_data_size) {
            return Status::Corruption("FSST page: encoded data truncated");
        }

        _parsed = true;
        return Status::OK();
    }

    Status seek_to_position_in_page(uint32_t pos) override {
        DCHECK(_parsed);
        DCHECK_LE(pos, _num_elements);
        _cur_index = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, Column* dst) override {
        SparseRange<> read_range;
        uint32_t begin = current_index();
        read_range.add(Range<>(begin, begin + *n));
        RETURN_IF_ERROR(next_batch(read_range, dst));
        *n = current_index() - begin;
        return Status::OK();
    }

    Status next_batch(const SparseRange<>& range, Column* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(range.span_size() == 0 || _cur_index >= _num_elements)) {
            return Status::OK();
        }
        RETURN_IF_ERROR(_ensure_full_decode());

        size_t to_read =
                std::min(static_cast<size_t>(range.span_size()), static_cast<size_t>(_num_elements - _cur_index));

        // Zero-copy: create Slices pointing into the pre-decoded contiguous buffer.
        std::vector<Slice> slices;
        slices.reserve(to_read);

        SparseRangeIterator<> iter = range.new_iterator();
        while (to_read > 0 && _cur_index < _num_elements) {
            _cur_index = iter.begin();
            Range<> r = iter.next(to_read);
            uint32_t end = _cur_index + r.span_size();
            for (; _cur_index < end; ++_cur_index) {
                uint32_t dec_start = _decoded_offsets[_cur_index];
                uint32_t dec_len = _decoded_offsets[_cur_index + 1] - dec_start;
                slices.emplace_back(_decoded_data.data() + dec_start, dec_len);
            }
            to_read -= r.span_size();
        }

        if constexpr (Type == TYPE_CHAR) {
            for (auto& s : slices) {
                s.size = strnlen(s.data, s.size);
            }
        }
        if (!dst->append_strings(slices)) {
            return Status::InternalError("FSST: failed to append strings to column");
        }
        return Status::OK();
    }

    uint32_t count() const override { return _num_elements; }

    uint32_t current_index() const override { return _cur_index; }

    EncodingTypePB encoding_type() const override { return FSST_ENCODING; }

    Status next_batch_with_filter(Column* column, const SparseRange<>& range,
                                  const std::vector<const ColumnPredicate*>& compound_and_predicates,
                                  const uint8_t* null_data, uint8_t* selection, uint16_t* selected_idx) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(_cur_index >= _num_elements || range.span_size() == 0)) {
            return Status::OK();
        }

        size_t num_rows = range.span_size();

        // Level 1: For VARCHAR with all EQ/NE predicates, evaluate in encoded space.
        // FSST is injective: encode(a) == encode(b) iff a == b.
        // Skip for CHAR — stored values have null padding that predicates may not.
        if constexpr (Type != TYPE_CHAR) {
            if (!compound_and_predicates.empty() && _active_symbol_table != nullptr) {
                bool all_eq_ne = true;
                bool single_eq = (compound_and_predicates.size() == 1 &&
                                  compound_and_predicates[0]->type() == PredicateType::kEQ);

                // Fast path: single EQ predicate — cache encoded value across pages
                if (single_eq) {
                    Datum datum = compound_and_predicates[0]->value();
                    if (!datum.is_null()) {
                        const std::string& encoded = _get_cached_encoded_pred(datum.get_slice());
                        return _next_batch_single_eq(column, range, encoded,
                                                     null_data, selection, num_rows);
                    }
                }

                // General path: multiple predicates or NE
                std::vector<EncodedPred> encoded_preds;
                for (auto* pred : compound_and_predicates) {
                    auto t = pred->type();
                    if (t != PredicateType::kEQ && t != PredicateType::kNE) {
                        all_eq_ne = false;
                        break;
                    }
                    Datum datum = pred->value();
                    if (datum.is_null()) {
                        all_eq_ne = false;
                        break;
                    }
                    Slice val = datum.get_slice();
                    encoded_preds.push_back(
                            {_active_symbol_table->encode_flat(reinterpret_cast<const uint8_t*>(val.data), val.size),
                             t == PredicateType::kEQ});
                }

                if (all_eq_ne) {
                    return _next_batch_encoded_eq_ne(column, range, encoded_preds, null_data, selection,
                                                     num_rows);
                }
            }
        }

        // Level 2: Lazy full decode + evaluate for non-EQ/NE predicates.
        RETURN_IF_ERROR(_ensure_full_decode());

        auto temp_column = column->clone_empty();
        RETURN_IF_ERROR(next_batch(range, temp_column.get()));

        RETURN_IF_ERROR(compound_and_predicates_evaluate(compound_and_predicates, temp_column.get(), selection,
                                                         selected_idx, 0, static_cast<uint16_t>(num_rows)));

        uint32_t selected_count = 0;
        for (size_t i = 0; i < num_rows; ++i) {
            selected_count += selection[i];
        }
        if (selected_count == 0) {
            return Status::OK();
        }

        if (selected_count == num_rows) {
            column->append(*temp_column, 0, num_rows);
        } else {
            for (size_t i = 0; i < num_rows; ++i) {
                if (selection[i]) {
                    column->append(*temp_column, i, 1);
                }
            }
        }

        return Status::OK();
    }

    // Evaluate equality/inequality predicates on FSST-encoded data without decoding.
    // FSST encoding is injective, so encode(a) == encode(b) iff a == b.
    Status evaluate_predicate_compressed(const ColumnPredicate* pred, SparseRange<>* row_ranges) override {
        DCHECK(_parsed);
        if (_num_elements == 0) {
            return Status::OK();
        }

        auto pred_type = pred->type();

        if (pred_type != PredicateType::kEQ && pred_type != PredicateType::kNE) {
            row_ranges->add(Range<>(0, _num_elements));
            return Status::OK();
        }

        Datum datum = pred->value();
        if (datum.is_null()) {
            if (pred_type == PredicateType::kNE) {
                row_ranges->add(Range<>(0, _num_elements));
            }
            return Status::OK();
        }

        if (PREDICT_FALSE(_active_symbol_table == nullptr)) {
            // Symbol table must be injected before calling evaluate_predicate_compressed.
            // If we reach here, it's a programming error — fail loudly instead of
            // silently returning "all rows match".
            return Status::InternalError("FSST symbol table not set before evaluate_predicate_compressed");
        }

        Slice pred_value = datum.get_slice();
        std::string encoded_pred = _active_symbol_table->encode_flat(reinterpret_cast<const uint8_t*>(pred_value.data),
                                                                     pred_value.size);

        for (uint32_t i = 0; i < _num_elements; ++i) {
            uint32_t enc_start = _enc_offset(i);
            uint32_t enc_len = _enc_offset(i + 1) - enc_start;

            bool match = (enc_len == encoded_pred.size()) &&
                         (memcmp(_encoded_data + enc_start, encoded_pred.data(), enc_len) == 0);

            if (pred_type == PredicateType::kEQ) {
                if (match) row_ranges->add(Range<>(i, i + 1));
            } else {
                if (!match) row_ranges->add(Range<>(i, i + 1));
            }
        }

        return Status::OK();
    }

private:
    struct EncodedPred {
        std::string encoded;
        bool is_eq;
    };

    // Inline equality comparison with 8-byte prefix fast rejection.
    // Avoids calling memcmp (function call overhead) for the common case where
    // strings differ in the first 8 bytes. For strings ≤ 8 bytes, this is a
    // single uint64 comparison with no memcmp at all.
    // Returns 1 if equal, 0 if not.
    static inline uint8_t _inline_eq_compare(const uint8_t* data, const uint8_t* pred_data,
                                              uint64_t pred_prefix8, uint32_t len) {
        if (len == 0) return 1;
        if (len <= 8) {
            // Short string: compare as uint64 (padded with zeros at build time)
            uint64_t val = 0;
            memcpy(&val, data, len);
            return (val == pred_prefix8) ? 1 : 0;
        }
        // Long string: compare first 8 bytes inline, then memcmp the rest
        uint64_t val;
        memcpy(&val, data, 8);
        if (val != pred_prefix8) return 0;
        return (memcmp(data + 8, pred_data + 8, len - 8) == 0) ? 1 : 0;
    }

    // Level 1: Evaluate EQ/NE predicates in encoded space, decode only matching rows.
    // Optimized for the common case: single EQ predicate with very few matches per page.
    Status _next_batch_encoded_eq_ne(Column* column, const SparseRange<>& range,
                                     const std::vector<EncodedPred>& encoded_preds, const uint8_t* null_data,
                                     uint8_t* selection, size_t num_rows) {
        // For single EQ predicate (most common case), use a fast path that avoids
        // all heap allocations and minimizes per-row overhead.
        if (encoded_preds.size() == 1 && encoded_preds[0].is_eq) {
            return _next_batch_single_eq(column, range, encoded_preds[0].encoded, null_data, selection, num_rows);
        }

        // General path for multiple predicates or NE predicates.
        SparseRangeIterator<> iter = range.new_iterator();
        size_t row_idx = 0;
        uint32_t selected_count = 0;
        size_t to_read = std::min(num_rows, static_cast<size_t>(_num_elements - _cur_index));

        // Pass 1: Evaluate predicates in encoded space → fill selection[]
        while (to_read > 0 && iter.has_more()) {
            _cur_index = iter.begin();
            Range<> r = iter.next(to_read);
            uint32_t end = _cur_index + r.span_size();
            for (; _cur_index < end; ++_cur_index, ++row_idx) {
                if (null_data != nullptr && null_data[row_idx]) {
                    selection[row_idx] = 0;
                    continue;
                }

                uint32_t enc_start = _enc_offset(_cur_index);
                uint32_t enc_len = _enc_offset(_cur_index + 1) - enc_start;

                bool pass = true;
                for (const auto& ep : encoded_preds) {
                    bool match = (enc_len == ep.encoded.size()) &&
                                 (enc_len == 0 ||
                                  memcmp(_encoded_data + enc_start, ep.encoded.data(), enc_len) == 0);
                    if (ep.is_eq ? !match : match) {
                        pass = false;
                        break;
                    }
                }
                selection[row_idx] = pass ? 1 : 0;
                selected_count += pass;
            }
            to_read -= r.span_size();
        }

        if (selected_count == 0) {
            return Status::OK();
        }

        // Pass 2: Decode only selected rows.
        // Re-walk the range to find selected indices and decode them.
        RETURN_IF_ERROR(_decode_selected_rows(column, range, selection, row_idx, selected_count));
        return Status::OK();
    }

    // Fast path for single EQ predicate. Optimized inner loop:
    // - Carry-forward offset: one decode_fixed32_le per row instead of two
    // - Inline 8-byte prefix comparison: avoids memcmp function call overhead
    //   for the vast majority of length-matched strings
    // - Null-data branch hoisted outside loop
    // - Stack-local match collection (zero heap alloc for 0-1 matches)
    Status _next_batch_single_eq(Column* column, const SparseRange<>& range,
                                 const std::string& encoded_pred, const uint8_t* null_data,
                                 uint8_t* selection, size_t num_rows) {
        const uint32_t pred_len = static_cast<uint32_t>(encoded_pred.size());
        const uint8_t* pred_data = reinterpret_cast<const uint8_t*>(encoded_pred.data());

        // Pre-cache first 8 bytes as uint64 for inline prefix comparison.
        // This rejects the vast majority of length-matched strings without
        // calling memcmp (function call overhead is significant at 5M calls).
        uint64_t pred_prefix8 = 0;
        if (pred_len >= 8) {
            memcpy(&pred_prefix8, pred_data, 8);
        } else if (pred_len > 0) {
            memcpy(&pred_prefix8, pred_data, pred_len);
        }

        SparseRangeIterator<> iter = range.new_iterator();
        size_t row_idx = 0;
        size_t to_read = std::min(num_rows, static_cast<size_t>(_num_elements - _cur_index));

        uint32_t stack_matches[32];
        uint32_t match_count = 0;

        while (to_read > 0 && iter.has_more()) {
            _cur_index = iter.begin();
            Range<> r = iter.next(to_read);
            const uint32_t end = _cur_index + r.span_size();

            uint32_t cur_offset = _enc_offset(_cur_index);

            if (null_data != nullptr) {
                for (; _cur_index < end; ++_cur_index, ++row_idx) {
                    uint32_t next_offset = _enc_offset(_cur_index + 1);
                    uint32_t enc_len = next_offset - cur_offset;

                    if (null_data[row_idx] || enc_len != pred_len) {
                        selection[row_idx] = 0;
                        cur_offset = next_offset;
                        continue;
                    }

                    selection[row_idx] = _inline_eq_compare(_encoded_data + cur_offset, pred_data,
                                                            pred_prefix8, pred_len);
                    if (selection[row_idx]) {
                        if (match_count < 32) stack_matches[match_count] = _cur_index;
                        match_count++;
                    }
                    cur_offset = next_offset;
                }
            } else {
                for (; _cur_index < end; ++_cur_index, ++row_idx) {
                    uint32_t next_offset = _enc_offset(_cur_index + 1);
                    uint32_t enc_len = next_offset - cur_offset;

                    if (enc_len != pred_len) {
                        selection[row_idx] = 0;
                        cur_offset = next_offset;
                        continue;
                    }

                    selection[row_idx] = _inline_eq_compare(_encoded_data + cur_offset, pred_data,
                                                            pred_prefix8, pred_len);
                    if (selection[row_idx]) {
                        if (match_count < 32) stack_matches[match_count] = _cur_index;
                        match_count++;
                    }
                    cur_offset = next_offset;
                }
            }
            to_read -= r.span_size();
        }

        if (match_count == 0) {
            return Status::OK();
        }

        // Decode only matched rows (typically 0-1 for high-cardinality EQ).
        if (match_count <= 32) {
            size_t max_decoded = static_cast<size_t>(pred_len) * fsst_detail::FSST_MAX_SYMBOL_LEN;
            char stack_temp[4096];
            char* temp = (max_decoded <= sizeof(stack_temp)) ? stack_temp : new char[max_decoded + 8];
            for (uint32_t i = 0; i < match_count; ++i) {
                uint32_t idx = stack_matches[i];
                uint32_t enc_start = _enc_offset(idx);
                uint32_t enc_len = _enc_offset(idx + 1) - enc_start;
                size_t decoded_len = _active_symbol_table->decode_to_buffer(
                        _encoded_data + enc_start, enc_len, temp);
                Slice s(temp, decoded_len);
                if (!column->append_strings(&s, 1)) {
                    if (temp != stack_temp) delete[] temp;
                    return Status::InternalError("FSST: failed to append string");
                }
            }
            if (temp != stack_temp) delete[] temp;
        } else {
            RETURN_IF_ERROR(_decode_selected_rows(column, range, selection, row_idx, match_count));
        }
        return Status::OK();
    }

    // Decode selected rows from the page into the output column.
    // Single-pass: decode into contiguous buffer using offset tracking, then build slices.
    Status _decode_selected_rows(Column* column, const SparseRange<>& range,
                                 const uint8_t* selection, size_t total_rows, uint32_t selected_count) {
        // Temp buffer for single-string decode. Max decoded size per string =
        // encoded_len * FSST_MAX_SYMBOL_LEN. Use a generous fixed size.
        char temp[4096];

        std::string decode_buf;
        decode_buf.reserve(selected_count * 64);
        std::vector<uint32_t> offsets;
        offsets.reserve(selected_count + 1);
        offsets.push_back(0);

        SparseRangeIterator<> iter = range.new_iterator();
        size_t ri = 0;
        while (iter.has_more() && ri < total_rows) {
            uint32_t idx = iter.begin();
            Range<> r = iter.next(total_rows - ri);
            for (uint32_t j = 0; j < r.span_size() && ri < total_rows; ++j, ++ri) {
                if (!selection[ri]) continue;
                uint32_t cur_idx = idx + j;
                uint32_t enc_start = _enc_offset(cur_idx);
                uint32_t enc_len = _enc_offset(cur_idx + 1) - enc_start;
                size_t max_decoded = static_cast<size_t>(enc_len) * fsst_detail::FSST_MAX_SYMBOL_LEN;
                char* buf = (max_decoded <= sizeof(temp)) ? temp : new char[max_decoded + 8];
                size_t decoded_len = _active_symbol_table->decode_to_buffer(
                        _encoded_data + enc_start, enc_len, buf);
                decode_buf.append(buf, decoded_len);
                offsets.push_back(static_cast<uint32_t>(decode_buf.size()));
                if (buf != temp) delete[] buf;
            }
        }

        // Build slices from stable decode_buf (no more appends after this point)
        std::vector<Slice> slices;
        slices.reserve(selected_count);
        for (size_t i = 0; i + 1 < offsets.size(); ++i) {
            slices.emplace_back(decode_buf.data() + offsets[i], offsets[i + 1] - offsets[i]);
        }

        if (!column->append_strings(slices)) {
            return Status::InternalError("FSST: failed to append strings to column");
        }
        return Status::OK();
    }

    // Lazy full decode: batch-decode all strings into a single contiguous buffer.
    // Called on first need (full scan or non-EQ/NE predicate evaluation).
    // Uses decode_to_buffer() with a reusable temp buffer for ~3-4x throughput vs decode_append().
    Status _ensure_full_decode() {
        if (_fully_decoded) return Status::OK();
        _fully_decoded = true;
        if (PREDICT_FALSE(_active_symbol_table == nullptr)) {
            return Status::Corruption("FSST page: symbol table not set before decode");
        }

        if (_num_elements == 0) return Status::OK();

        _decoded_offsets.resize(_num_elements + 1);
        _decoded_offsets[0] = 0;

        // Find max encoded length to size the temp buffer.
        // Worst-case decoded size per string = encoded_len * FSST_MAX_SYMBOL_LEN (all 1-byte symbols
        // expanding to 8 bytes). In practice the expansion is ~2x.
        uint32_t max_enc_len = 0;
        for (uint32_t i = 0; i < _num_elements; ++i) {
            uint32_t enc_len = _enc_offset(i + 1) - _enc_offset(i);
            if (enc_len > max_enc_len) max_enc_len = enc_len;
        }

        // Temp buffer for single-string decode. Reused across all strings.
        std::vector<char> temp(static_cast<size_t>(max_enc_len) * fsst_detail::FSST_MAX_SYMBOL_LEN + 8);

        // Reserve estimated total decoded size (encoded size * 2 is a reasonable estimate).
        _decoded_data.reserve(_enc_offset(_num_elements) * 2);

        for (uint32_t i = 0; i < _num_elements; ++i) {
            uint32_t enc_start = _enc_offset(i);
            uint32_t enc_len = _enc_offset(i + 1) - enc_start;
            size_t decoded_len = _active_symbol_table->decode_to_buffer(_encoded_data + enc_start, enc_len, temp.data());
            _decoded_data.append(temp.data(), decoded_len);
            _decoded_offsets[i + 1] = static_cast<uint32_t>(_decoded_data.size());
        }
        return Status::OK();
    }

    Slice _data;
    bool _parsed{false};
    bool _fully_decoded{false};
    uint32_t _num_elements{0};
    uint32_t _cur_index{0};
    const fsst_detail::SymbolTable* _shared_symbol_table{nullptr};
    const fsst_detail::SymbolTable* _active_symbol_table{nullptr};

    // Zero-copy encoded offsets — byte pointer into page buffer.
    // Reads via decode_fixed32_le for alignment safety (no uint32_t* aliasing).
    const uint8_t* _enc_offsets_data{nullptr};

    uint32_t _enc_offset(uint32_t i) const {
        return decode_fixed32_le(_enc_offsets_data + i * sizeof(uint32_t));
    }
    const uint8_t* _encoded_data{nullptr};
    size_t _encoded_data_size{0};

    // Lazily decoded data: single contiguous buffer + offsets.
    // Built on first call to _ensure_full_decode(), subsequent reads are zero-copy.
    std::string _decoded_data;
    std::vector<uint32_t> _decoded_offsets;

    // External cache pointers (owned by iterator, persist across pages).
    // When set, avoids re-encoding the same predicate on every page.
    std::string* _ext_cached_encoded_pred{nullptr};
    std::string* _ext_cached_pred_raw{nullptr};

    // Local cache (used when external cache not set, e.g., standalone tests).
    std::string _local_cached_encoded_pred;
    std::string _local_cached_pred_raw;

    // Returns reference to the cached encoded predicate (valid until next call).
    const std::string& _get_cached_encoded_pred(const Slice& val) {
        std::string& cached_encoded = _ext_cached_encoded_pred ? *_ext_cached_encoded_pred
                                                                : _local_cached_encoded_pred;
        std::string& cached_raw = _ext_cached_pred_raw ? *_ext_cached_pred_raw
                                                        : _local_cached_pred_raw;

        if (cached_raw.size() == val.size &&
            (cached_raw.empty() ||
             memcmp(cached_raw.data(), val.data, val.size) == 0)) {
            return cached_encoded; // Cache hit
        }
        cached_raw.assign(val.data, val.size);
        cached_encoded = _active_symbol_table->encode_flat(
                reinterpret_cast<const uint8_t*>(val.data), val.size);
        return cached_encoded;
    }
};

} // namespace starrocks
