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

#include <roaring/roaring.hh>

#include "storage/index/inverted/inverted_writer.h"
#include "storage/tablet_schema.h"
#include "types/type_info.h"

struct TantivyWriter;

namespace starrocks {

class TantivyInvertedWriter : public InvertedWriter {
public:
    TantivyInvertedWriter(std::string directory, const TabletIndex* tablet_index);

    ~TantivyInvertedWriter() override;

    static Status create(const TypeInfoPtr& typeinfo, const std::string& field_name, const std::string& directory,
                         TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res);

    Status init() override;

    void add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override;

    Status finish(WritableFile* wfile, ColumnMetaPB* meta) override;

    uint64_t size() const override { return _total_bytes; }

private:
    std::string _directory;
    const TabletIndex* _tablet_index;
    ::TantivyWriter* _writer = nullptr;
    uint32_t _rid = 0;
    uint64_t _total_bytes = 0;
    roaring::Roaring _null_bitmap;
};

} // namespace starrocks
