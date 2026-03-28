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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "base/phmap/phmap.h"

// FSST (Fast Static Symbol Table) encoding for string data.
//
// Reference: "FSST: Fast Random Access String Compression" (VLDB 2020)
//   https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf
//
// Algorithm overview:
// 1. Build a symbol table of up to 255 symbols (each 1-8 bytes) from input data.
// 2. Encode strings by replacing occurrences of symbols with 1-byte codes.
//    - Code 0 is the escape code: [0x00][original_byte] represents a literal byte.
//    - Codes 1-255 map to symbols in the table.
// 3. Decoding: scan encoded bytes, expand codes to symbols.
//
// Properties:
// - Deterministic: same input + same symbol table = same output (injective).
// - Equality-safe: encode(a) == encode(b) iff a == b (under same symbol table).
// - NOT order-preserving: encode(a) < encode(b) does NOT imply a < b.

namespace starrocks::fsst_detail {

static constexpr uint8_t FSST_ESCAPE = 0x00;
static constexpr int FSST_MAX_SYMBOL_LEN = 8;
static constexpr int FSST_MAX_SYMBOLS = 255; // codes 1..255
static constexpr int FSST_NUM_ITERATIONS = 5; // symbol table construction rounds

struct Symbol {
    uint8_t data[FSST_MAX_SYMBOL_LEN];
    uint8_t len;
};

// SymbolTable holds up to 255 symbols. Code 0 is reserved as escape.
class SymbolTable {
public:
    SymbolTable() = default;

    int num_symbols() const { return _num_symbols; }

    const Symbol& symbol(int code_minus_1) const { return _symbols[code_minus_1]; }

    // Add a symbol. Returns the assigned code (1-based), or 0 if table is full.
    int add_symbol(const uint8_t* data, uint8_t len) {
        if (_num_symbols >= FSST_MAX_SYMBOLS || len == 0 || len > FSST_MAX_SYMBOL_LEN) {
            return 0;
        }
        Symbol& s = _symbols[_num_symbols++];
        memcpy(s.data, data, len);
        s.len = len;
        return _num_symbols; // 1-based code
    }

    // Build lookup structures for encoding and decoding.
    // After calling this, both encode() and decode_to_buffer() are ready.
    void build_index() {
        _build_encode_index();
        _build_flat_tables();
    }

    // Encode a single string using this symbol table.
    // Returns the encoded bytes.
    // Uses first-byte index to skip hash lookups for bytes with no matching symbols,
    // and phmap heterogeneous lookup (string_view) to avoid temporary std::string construction.
    std::string encode(const uint8_t* input, size_t input_len) const {
        _ensure_encode_index();
        std::string result;
        result.reserve(input_len);
        size_t pos = 0;
        while (pos < input_len) {
            // Quick check: if no symbol starts with this byte, emit escape immediately.
            if (!_first_byte_has_symbol[input[pos]]) {
                result.push_back(static_cast<char>(FSST_ESCAPE));
                result.push_back(static_cast<char>(input[pos]));
                pos += 1;
                continue;
            }

            int best_code = 0;
            int best_len = 0;
            // Try longest prefix match (greedy, from max symbol length down to 1).
            // Uses string_view for zero-copy hash lookup (phmap heterogeneous find).
            int max_try = std::min(static_cast<int>(input_len - pos), FSST_MAX_SYMBOL_LEN);
            for (int try_len = max_try; try_len >= 1; --try_len) {
                std::string_view key(reinterpret_cast<const char*>(input + pos), try_len);
                auto it = _index.find(key);
                if (it != _index.end()) {
                    best_code = it->second;
                    best_len = try_len;
                    break;
                }
            }
            if (best_code > 0) {
                result.push_back(static_cast<char>(best_code));
                pos += best_len;
            } else {
                result.push_back(static_cast<char>(FSST_ESCAPE));
                result.push_back(static_cast<char>(input[pos]));
                pos += 1;
            }
        }
        return result;
    }

    // Encode a single string using first-byte candidate dispatch (no hash map).
    // Uses a sorted candidate array indexed by first byte for O(k) lookup per
    // position (k = candidates per first byte). Never falls back to hash map —
    // handles any symbol distribution, including all 255 symbols sharing one byte.
    std::string encode_flat(const uint8_t* input, size_t input_len) const {
        std::string result;
        result.reserve(input_len);
        size_t pos = 0;
        while (pos < input_len) {
            uint8_t fb = input[pos];
            int start = _candidate_start[fb];
            int end = _candidate_end[fb];
            if (start == end) {
                result.push_back(static_cast<char>(FSST_ESCAPE));
                result.push_back(static_cast<char>(fb));
                pos += 1;
                continue;
            }
            int best_code = 0;
            int best_len = 0;
            int remaining = static_cast<int>(input_len - pos);
            for (int ci = start; ci < end; ++ci) {
                const auto& c = _all_candidates[ci];
                if (c.len > best_len && c.len <= remaining &&
                    memcmp(input + pos, _flat_data[c.code], c.len) == 0) {
                    best_code = c.code;
                    best_len = c.len;
                }
            }
            if (best_code > 0) {
                result.push_back(static_cast<char>(best_code));
                pos += best_len;
            } else {
                result.push_back(static_cast<char>(FSST_ESCAPE));
                result.push_back(static_cast<char>(fb));
                pos += 1;
            }
        }
        return result;
    }

    // Decode an encoded string back to the original.
    std::string decode(const uint8_t* encoded, size_t encoded_len) const {
        std::string result;
        result.reserve(encoded_len * 2);
        decode_append(encoded, encoded_len, &result);
        return result;
    }

    // Decode an encoded string, appending to an existing buffer.
    // Avoids per-string heap allocation when batch-decoding.
    // Uses flat lookup tables for consistency with decode_to_buffer().
    void decode_append(const uint8_t* encoded, size_t encoded_len, std::string* output) const {
        size_t pos = 0;
        while (pos < encoded_len) {
            uint8_t code = encoded[pos];
            if (code == FSST_ESCAPE) {
                if (pos + 1 >= encoded_len) break;
                output->push_back(static_cast<char>(encoded[pos + 1]));
                pos += 2;
            } else {
                output->append(reinterpret_cast<const char*>(_flat_data[code]), _flat_lens[code]);
                pos += 1;
            }
        }
    }

    // Decode an encoded string directly into a caller-provided buffer.
    // Returns the number of decoded bytes written to output.
    // The output buffer must be large enough (worst case: encoded_len * FSST_MAX_SYMBOL_LEN).
    // Uses flat lookup tables for O(1) per-code decode without branches or indirection.
    size_t decode_to_buffer(const uint8_t* encoded, size_t encoded_len, char* output) const {
        char* out = output;
        size_t pos = 0;
        while (pos < encoded_len) {
            uint8_t code = encoded[pos];
            if (code == FSST_ESCAPE) {
                if (pos + 1 >= encoded_len) break;
                *out++ = static_cast<char>(encoded[pos + 1]);
                pos += 2;
            } else {
                // Fixed 8-byte copy: always copies 8 bytes (max symbol len), then advances
                // by actual symbol length. The extra bytes are harmless and will be overwritten.
                memcpy(out, _flat_data[code], FSST_MAX_SYMBOL_LEN);
                out += _flat_lens[code];
                pos += 1;
            }
        }
        return static_cast<size_t>(out - output);
    }

    // Serialize the symbol table to bytes.
    // Format: [num_symbols: uint8] [sym0_len: uint8] [sym0_data: 1-8 bytes] ...
    std::string serialize() const {
        std::string result;
        result.push_back(static_cast<char>(_num_symbols));
        for (int i = 0; i < _num_symbols; ++i) {
            result.push_back(static_cast<char>(_symbols[i].len));
            result.append(reinterpret_cast<const char*>(_symbols[i].data), _symbols[i].len);
        }
        return result;
    }

    // Deserialize from bytes. Returns number of bytes consumed, or 0 on error.
    // Only builds flat decode tables (decode-only path). The encode index (_index)
    // is built lazily on first encode() call — this saves ~38% of query-path time
    // for LIKE/FULL SCAN queries that never need encode().
    size_t deserialize(const uint8_t* data, size_t data_len) {
        _num_symbols = 0;
        _index.clear();
        _index_built = false;
        if (data_len < 1) return 0;
        int n = static_cast<uint8_t>(data[0]);
        size_t pos = 1;
        for (int i = 0; i < n; ++i) {
            if (pos >= data_len) return 0;
            uint8_t slen = data[pos++];
            if (slen == 0 || slen > FSST_MAX_SYMBOL_LEN || pos + slen > data_len) return 0;
            memcpy(_symbols[i].data, data + pos, slen);
            _symbols[i].len = slen;
            _num_symbols++;
            pos += slen;
        }
        // Only build flat decode tables. Encode index is built lazily via _ensure_encode_index().
        _build_flat_tables();
        return pos;
    }

private:
    // Build the encode hash index from _symbols.
    void _build_encode_index() {
        _index.clear();
        for (int i = 0; i < _num_symbols; ++i) {
            std::string_view key(reinterpret_cast<const char*>(_symbols[i].data), _symbols[i].len);
            if (_index.find(key) == _index.end()) {
                _index.emplace(std::string(key), i + 1); // 1-based code
            }
        }
        _index_built = true;
    }

    // Lazy init: build encode index on first encode() call.
    // Decode-only paths (LIKE/FULL SCAN) never pay this cost.
    void _ensure_encode_index() const {
        if (_index_built) return;
        // const_cast is safe: _index is a mutable cache, semantically const.
        const_cast<SymbolTable*>(this)->_build_encode_index();
    }

    // Build flat lookup tables from _symbols for O(1) decode and encode_flat().
    void _build_flat_tables() {
        memset(_flat_lens, 0, sizeof(_flat_lens));
        memset(_flat_data, 0, sizeof(_flat_data));
        memset(_first_byte_has_symbol, 0, sizeof(_first_byte_has_symbol));

        // Build decode tables and collect candidates with first-byte info.
        uint8_t counts[256] = {};
        for (int i = 0; i < _num_symbols; ++i) {
            int code = i + 1; // 1-based
            _flat_lens[code] = _symbols[i].len;
            memcpy(_flat_data[code], _symbols[i].data, _symbols[i].len);
            uint8_t fb = _symbols[i].data[0];
            _first_byte_has_symbol[fb] = true;
            counts[fb]++;
        }

        // Build prefix-sum index: _candidate_start[fb] = start offset in _all_candidates.
        int offset = 0;
        for (int fb = 0; fb < 256; ++fb) {
            _candidate_start[fb] = static_cast<uint16_t>(offset);
            offset += counts[fb];
            _candidate_end[fb] = static_cast<uint16_t>(offset);
        }

        // Fill _all_candidates in first-byte order using counts as write cursors.
        memset(counts, 0, sizeof(counts));
        for (int i = 0; i < _num_symbols; ++i) {
            uint8_t fb = _symbols[i].data[0];
            int idx = _candidate_start[fb] + counts[fb]++;
            _all_candidates[idx].code = static_cast<uint8_t>(i + 1);
            _all_candidates[idx].len = _symbols[i].len;
        }
    }

    Symbol _symbols[FSST_MAX_SYMBOLS];
    int _num_symbols{0};

    // Encode index: substring → 1-based code. Uses phmap for heterogeneous
    // string_view lookup (avoids temporary std::string construction per find()).
    // Built lazily via _ensure_encode_index() — decode-only paths skip this entirely.
    phmap::flat_hash_map<std::string, int> _index;
    bool _index_built{false};

    // Flat lookup tables for fast decode: code → (length, data).
    // Initialized by _build_flat_tables() — no need for default {} init.
    uint8_t _flat_lens[256];
    alignas(8) uint8_t _flat_data[256][FSST_MAX_SYMBOL_LEN];

    // First-byte index for encode_flat(): skip symbol scan for bytes with no matching symbol.
    bool _first_byte_has_symbol[256];

    // Sorted candidate array for encode_flat() — no hash map, no overflow.
    // Symbols are sorted by first byte into _all_candidates[], with
    // _candidate_start[fb]/_candidate_end[fb] giving the range for each byte.
    struct EncodingCandidate {
        uint8_t code;
        uint8_t len;
    };
    EncodingCandidate _all_candidates[FSST_MAX_SYMBOLS];
    uint16_t _candidate_start[256];
    uint16_t _candidate_end[256];
};

// Count occurrences of substrings (length 1-8) in the given strings.
// Uses a simple approach: count all substrings, pick the ones with highest gain.
struct SubstringGain {
    std::string substring;
    int64_t gain; // bytes saved if this substring becomes a symbol
};

// Build a symbol table from input strings using greedy iterative approach.
// Each iteration:
// 1. Count substring frequencies
// 2. Pick top-K substrings by compression gain
// 3. Rebuild the table
inline SymbolTable build_symbol_table(const std::vector<std::pair<const uint8_t*, size_t>>& strings,
                                      int max_iterations = FSST_NUM_ITERATIONS) {
    SymbolTable best_table;
    int64_t best_compressed_size = 0;

    // Calculate total uncompressed size
    int64_t total_size = 0;
    for (const auto& [ptr, len] : strings) {
        total_size += static_cast<int64_t>(len);
    }
    best_compressed_size = total_size * 2; // worst case: all escapes

    // Sample strings if too many (use at most 1000 strings or 128KB)
    std::vector<std::pair<const uint8_t*, size_t>> sample;
    {
        size_t sample_bytes = 0;
        const size_t max_sample_bytes = 128 * 1024;
        const size_t max_sample_count = 1000;
        for (const auto& [ptr, len] : strings) {
            if (sample.size() >= max_sample_count || sample_bytes >= max_sample_bytes) break;
            sample.emplace_back(ptr, len);
            sample_bytes += len;
        }
    }

    for (int iter = 0; iter < max_iterations; ++iter) {
        // Count substring frequencies using current encoding
        std::unordered_map<std::string, int64_t> freq;

        for (const auto& [ptr, len] : sample) {
            if (iter == 0) {
                // First iteration: count raw substrings
                for (size_t pos = 0; pos < len; ++pos) {
                    int max_slen = std::min(static_cast<int>(len - pos), FSST_MAX_SYMBOL_LEN);
                    for (int slen = 1; slen <= max_slen; ++slen) {
                        std::string key(reinterpret_cast<const char*>(ptr + pos), slen);
                        freq[key]++;
                    }
                }
            } else {
                // Subsequent iterations: count substrings of the residual (escaped bytes)
                std::string encoded = best_table.encode(ptr, len);
                const auto* enc = reinterpret_cast<const uint8_t*>(encoded.data());
                size_t enc_len = encoded.size();

                // Find runs of escaped bytes and count substrings in them
                size_t pos = 0;
                std::vector<uint8_t> residual;
                while (pos < enc_len) {
                    if (enc[pos] == FSST_ESCAPE && pos + 1 < enc_len) {
                        residual.push_back(enc[pos + 1]);
                        pos += 2;
                    } else {
                        // Flush residual if any
                        if (!residual.empty()) {
                            for (size_t rp = 0; rp < residual.size(); ++rp) {
                                int max_slen =
                                        std::min(static_cast<int>(residual.size() - rp), FSST_MAX_SYMBOL_LEN);
                                for (int slen = 1; slen <= max_slen; ++slen) {
                                    std::string key(reinterpret_cast<const char*>(residual.data() + rp), slen);
                                    freq[key]++;
                                }
                            }
                            residual.clear();
                        }
                        pos += 1;
                    }
                }
                // Flush remaining residual
                if (!residual.empty()) {
                    for (size_t rp = 0; rp < residual.size(); ++rp) {
                        int max_slen = std::min(static_cast<int>(residual.size() - rp), FSST_MAX_SYMBOL_LEN);
                        for (int slen = 1; slen <= max_slen; ++slen) {
                            std::string key(reinterpret_cast<const char*>(residual.data() + rp), slen);
                            freq[key]++;
                        }
                    }
                }
            }
        }

        // Calculate gain for each substring: gain = freq * (len - 1) - freq
        // Using the symbol saves (len - 1) bytes per occurrence (replace len bytes with 1 code byte),
        // but we lose nothing extra since escape codes already cost 2 bytes per literal.
        // Net gain per occurrence = (len - 1) if the substring is currently escaped,
        // or at minimum = (len - 1) for each occurrence.
        std::vector<SubstringGain> candidates;
        candidates.reserve(freq.size());
        for (auto& [key, count] : freq) {
            int slen = static_cast<int>(key.size());
            // gain = count * (slen - 1): each occurrence saves (slen - 1) bytes
            // (replace slen escaped bytes = 2*slen bytes with 1 code byte, saving 2*slen - 1 bytes)
            // But in early iterations the bytes might not all be escaped. Use simpler model:
            // gain = count * (slen - 1)
            int64_t gain = count * static_cast<int64_t>(slen - 1);
            if (gain > 0) {
                candidates.push_back({key, gain});
            }
        }

        // Sort by gain descending
        std::sort(candidates.begin(), candidates.end(),
                  [](const SubstringGain& a, const SubstringGain& b) { return a.gain > b.gain; });

        // Build new symbol table with top candidates
        SymbolTable new_table;
        for (const auto& c : candidates) {
            if (new_table.num_symbols() >= FSST_MAX_SYMBOLS) break;
            new_table.add_symbol(reinterpret_cast<const uint8_t*>(c.substring.data()),
                                static_cast<uint8_t>(c.substring.size()));
        }
        new_table.build_index();

        // Measure compressed size
        int64_t compressed_size = 0;
        for (const auto& [ptr, len] : sample) {
            compressed_size += static_cast<int64_t>(new_table.encode(ptr, len).size());
        }

        if (compressed_size < best_compressed_size) {
            best_compressed_size = compressed_size;
            best_table = new_table;
        }
    }

    return best_table;
}

// Estimate compression ratio for a BinaryColumn.
// Returns encoded_size / original_size (lower is better).
template <typename BinaryColumnType>
inline double estimate_compression_ratio(const BinaryColumnType& col) {
    if (col.size() == 0) return 1.0;

    // Sample up to 200 strings for estimation
    size_t sample_count = std::min(static_cast<size_t>(200), col.size());
    std::vector<std::pair<const uint8_t*, size_t>> sample;
    sample.reserve(sample_count);
    size_t total_original = 0;
    for (size_t i = 0; i < sample_count; ++i) {
        auto s = col.get_slice(i);
        sample.emplace_back(reinterpret_cast<const uint8_t*>(s.data), s.size);
        total_original += s.size;
    }

    if (total_original == 0) return 1.0;

    SymbolTable table = build_symbol_table(sample, 3); // fewer iterations for estimation
    size_t total_encoded = 0;
    for (const auto& [ptr, len] : sample) {
        total_encoded += table.encode(ptr, len).size();
    }

    return static_cast<double>(total_encoded) / static_cast<double>(total_original);
}

} // namespace starrocks::fsst_detail
