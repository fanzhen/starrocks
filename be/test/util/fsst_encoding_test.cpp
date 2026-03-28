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

#include "util/fsst_encoding.h"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

namespace starrocks {

class FSSTEncodingTest : public testing::Test {};

// Test basic encode/decode roundtrip with a simple symbol table
TEST_F(FSSTEncodingTest, BasicEncodeDecodeRoundtrip) {
    fsst_detail::SymbolTable table;
    table.add_symbol(reinterpret_cast<const uint8_t*>("http://"), 7);
    table.add_symbol(reinterpret_cast<const uint8_t*>(".com"), 4);
    table.build_index();

    std::string input = "http://example.com";
    auto encoded = table.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    auto decoded = table.decode(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());

    ASSERT_EQ(input, decoded);
    // Encoded should be shorter than input (7 bytes "http://" -> 1 byte, 4 bytes ".com" -> 1 byte)
    ASSERT_LT(encoded.size(), input.size());
}

// Test roundtrip with empty string
TEST_F(FSSTEncodingTest, EmptyString) {
    fsst_detail::SymbolTable table;
    table.add_symbol(reinterpret_cast<const uint8_t*>("abc"), 3);
    table.build_index();

    std::string input;
    auto encoded = table.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    auto decoded = table.decode(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
    ASSERT_EQ(input, decoded);
    ASSERT_EQ(0, encoded.size());
}

// Test roundtrip with no matching symbols (all escaped)
TEST_F(FSSTEncodingTest, AllEscaped) {
    fsst_detail::SymbolTable table;
    table.add_symbol(reinterpret_cast<const uint8_t*>("xyz"), 3);
    table.build_index();

    std::string input = "abc";
    auto encoded = table.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    auto decoded = table.decode(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
    ASSERT_EQ(input, decoded);
    // Each byte should be escaped: [0x00][byte] -> 2 bytes per char = 6 bytes
    ASSERT_EQ(6, encoded.size());
}

// Test serialize/deserialize roundtrip
TEST_F(FSSTEncodingTest, SerializeDeserialize) {
    fsst_detail::SymbolTable table1;
    table1.add_symbol(reinterpret_cast<const uint8_t*>("http"), 4);
    table1.add_symbol(reinterpret_cast<const uint8_t*>("://"), 3);
    table1.add_symbol(reinterpret_cast<const uint8_t*>(".com"), 4);
    table1.build_index();

    std::string serialized = table1.serialize();

    fsst_detail::SymbolTable table2;
    size_t consumed = table2.deserialize(reinterpret_cast<const uint8_t*>(serialized.data()), serialized.size());
    ASSERT_GT(consumed, 0);
    ASSERT_EQ(serialized.size(), consumed);
    ASSERT_EQ(table1.num_symbols(), table2.num_symbols());

    // Verify both tables produce the same encoding
    std::string input = "http://example.com";
    auto enc1 = table1.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    auto enc2 = table2.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    ASSERT_EQ(enc1, enc2);
}

// Test build_symbol_table with URL-like data
TEST_F(FSSTEncodingTest, BuildSymbolTableURLs) {
    std::vector<std::string> urls;
    for (int i = 0; i < 100; ++i) {
        urls.push_back("http://example.com/api/v" + std::to_string(i % 10) + "/user/" + std::to_string(i));
    }

    std::vector<std::pair<const uint8_t*, size_t>> string_ptrs;
    for (const auto& u : urls) {
        string_ptrs.emplace_back(reinterpret_cast<const uint8_t*>(u.data()), u.size());
    }

    auto table = fsst_detail::build_symbol_table(string_ptrs);
    ASSERT_GT(table.num_symbols(), 0);

    // Verify all strings roundtrip correctly
    for (const auto& u : urls) {
        auto encoded = table.encode(reinterpret_cast<const uint8_t*>(u.data()), u.size());
        auto decoded = table.decode(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
        ASSERT_EQ(u, decoded) << "Roundtrip failed for: " << u;
    }

    // Compression should be effective for URL-like data
    size_t total_original = 0, total_encoded = 0;
    for (const auto& u : urls) {
        total_original += u.size();
        total_encoded += table.encode(reinterpret_cast<const uint8_t*>(u.data()), u.size()).size();
    }
    double ratio = static_cast<double>(total_encoded) / static_cast<double>(total_original);
    ASSERT_LT(ratio, 0.8) << "Compression ratio should be < 0.8 for URL data, got " << ratio;
}

// Test with UTF-8 strings
TEST_F(FSSTEncodingTest, UTF8Strings) {
    std::vector<std::string> strings;
    for (int i = 0; i < 50; ++i) {
        strings.push_back("数据库" + std::to_string(i) + "分析引擎");
    }

    std::vector<std::pair<const uint8_t*, size_t>> string_ptrs;
    for (const auto& s : strings) {
        string_ptrs.emplace_back(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }

    auto table = fsst_detail::build_symbol_table(string_ptrs);

    for (const auto& s : strings) {
        auto encoded = table.encode(reinterpret_cast<const uint8_t*>(s.data()), s.size());
        auto decoded = table.decode(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
        ASSERT_EQ(s, decoded) << "UTF-8 roundtrip failed for: " << s;
    }
}

// Test with random data (should not crash, but compression may be poor)
TEST_F(FSSTEncodingTest, RandomData) {
    std::vector<std::string> strings;
    srand(42);
    for (int i = 0; i < 50; ++i) {
        std::string s;
        int len = rand() % 100 + 1;
        for (int j = 0; j < len; ++j) {
            s.push_back(static_cast<char>(rand() % 256));
        }
        strings.push_back(s);
    }

    std::vector<std::pair<const uint8_t*, size_t>> string_ptrs;
    for (const auto& s : strings) {
        string_ptrs.emplace_back(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }

    auto table = fsst_detail::build_symbol_table(string_ptrs);

    for (const auto& s : strings) {
        auto encoded = table.encode(reinterpret_cast<const uint8_t*>(s.data()), s.size());
        auto decoded = table.decode(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
        ASSERT_EQ(s, decoded) << "Random data roundtrip failed";
    }
}

// Test equality property: same string encodes to same bytes (injectivity)
TEST_F(FSSTEncodingTest, EqualityInjectivity) {
    fsst_detail::SymbolTable table;
    table.add_symbol(reinterpret_cast<const uint8_t*>("hello"), 5);
    table.add_symbol(reinterpret_cast<const uint8_t*>("world"), 5);
    table.build_index();

    std::string a = "hello world";
    std::string b = "hello world";
    std::string c = "hello earth";

    auto enc_a = table.encode(reinterpret_cast<const uint8_t*>(a.data()), a.size());
    auto enc_b = table.encode(reinterpret_cast<const uint8_t*>(b.data()), b.size());
    auto enc_c = table.encode(reinterpret_cast<const uint8_t*>(c.data()), c.size());

    ASSERT_EQ(enc_a, enc_b) << "Same strings should encode to same bytes";
    ASSERT_NE(enc_a, enc_c) << "Different strings should encode to different bytes";
}

// Test max symbols limit (255)
TEST_F(FSSTEncodingTest, MaxSymbolsLimit) {
    fsst_detail::SymbolTable table;
    for (int i = 0; i < 300; ++i) {
        uint8_t byte = static_cast<uint8_t>(i % 256);
        int code = table.add_symbol(&byte, 1);
        if (i < 255) {
            ASSERT_GT(code, 0) << "Should accept up to 255 symbols";
        } else {
            ASSERT_EQ(code, 0) << "Should reject beyond 255 symbols";
        }
    }
    ASSERT_EQ(255, table.num_symbols());
}

// Test encode_flat produces identical output to encode for random data.
// This is critical for EQ/NE correctness: encode_flat(str) must == encode(str).
TEST_F(FSSTEncodingTest, EncodeFlatEquivalence) {
    // Use a fixed seed for reproducibility
    std::mt19937 rng(42);

    for (int trial = 0; trial < 50; ++trial) {
        // Build a random symbol table with 50-255 symbols
        int num_symbols = 50 + (rng() % 206);
        fsst_detail::SymbolTable table;
        for (int i = 0; i < num_symbols; ++i) {
            int slen = 1 + (rng() % fsst_detail::FSST_MAX_SYMBOL_LEN);
            uint8_t sym_data[fsst_detail::FSST_MAX_SYMBOL_LEN];
            for (int b = 0; b < slen; ++b) {
                sym_data[b] = static_cast<uint8_t>(rng() % 256);
            }
            table.add_symbol(sym_data, static_cast<uint8_t>(slen));
        }
        table.build_index();

        // Test with 100 random strings of varying lengths
        for (int s = 0; s < 100; ++s) {
            int str_len = rng() % 500;
            std::string input(str_len, '\0');
            for (int b = 0; b < str_len; ++b) {
                input[b] = static_cast<char>(rng() % 256);
            }

            std::string enc_hash = table.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
            std::string enc_flat = table.encode_flat(reinterpret_cast<const uint8_t*>(input.data()), input.size());

            ASSERT_EQ(enc_hash, enc_flat) << "encode_flat != encode for trial=" << trial << " string=" << s
                                          << " input_len=" << str_len << " num_symbols=" << num_symbols;
        }
    }
}

// Stress test: encode_flat with worst-case symbol table (all 255 symbols start with same byte).
// Verifies encode_flat handles any first-byte distribution without fallback or overflow.
TEST_F(FSSTEncodingTest, EncodeFlatWorstCaseFirstByte) {
    fsst_detail::SymbolTable table;
    // Add 255 symbols all starting with byte 0x41 ('A')
    for (int i = 0; i < 255; ++i) {
        uint8_t sym_data[fsst_detail::FSST_MAX_SYMBOL_LEN];
        sym_data[0] = 0x41; // 'A'
        int slen = 1 + (i % fsst_detail::FSST_MAX_SYMBOL_LEN);
        for (int b = 1; b < slen; ++b) {
            sym_data[b] = static_cast<uint8_t>(i + b);
        }
        table.add_symbol(sym_data, static_cast<uint8_t>(slen));
    }
    table.build_index();

    // Test strings with many 'A' characters — encode_flat handles all 255 candidates
    std::string input = "AAAAAAAAAA_test_AAAAAAAAAA_data_AAAAAAAAAA";
    std::string enc_hash = table.encode(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    std::string enc_flat = table.encode_flat(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    ASSERT_EQ(enc_hash, enc_flat) << "encode_flat != encode with worst-case first-byte distribution";

    // Verify decode roundtrip
    std::string decoded = table.decode(reinterpret_cast<const uint8_t*>(enc_flat.data()), enc_flat.size());
    ASSERT_EQ(input, decoded);
}

// Test encode_flat after deserialize (decode-only path, no build_index called)
TEST_F(FSSTEncodingTest, EncodeFlatAfterDeserialize) {
    // Build and serialize a symbol table
    std::vector<std::pair<const uint8_t*, size_t>> strings;
    std::string s1 = "GET /api/v2/users/12345 HTTP/1.1 Host: example.com";
    std::string s2 = "POST /api/v2/orders/67890 HTTP/1.1 Host: example.com";
    std::string s3 = "GET /api/v2/products/11111 HTTP/1.1 Host: example.com";
    strings.emplace_back(reinterpret_cast<const uint8_t*>(s1.data()), s1.size());
    strings.emplace_back(reinterpret_cast<const uint8_t*>(s2.data()), s2.size());
    strings.emplace_back(reinterpret_cast<const uint8_t*>(s3.data()), s3.size());

    auto original = fsst_detail::build_symbol_table(strings);
    std::string serialized = original.serialize();

    // Deserialize into a new table (only flat tables built, no _index)
    fsst_detail::SymbolTable restored;
    size_t consumed = restored.deserialize(reinterpret_cast<const uint8_t*>(serialized.data()), serialized.size());
    ASSERT_GT(consumed, 0u);

    // encode_flat should work without build_index
    for (const auto& s : {s1, s2, s3}) {
        std::string enc_original = original.encode(reinterpret_cast<const uint8_t*>(s.data()), s.size());
        std::string enc_restored = restored.encode_flat(reinterpret_cast<const uint8_t*>(s.data()), s.size());
        ASSERT_EQ(enc_original, enc_restored) << "encode_flat after deserialize != original encode for: " << s;
    }
}

} // namespace starrocks
