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

#include "storage/index/inverted/tantivy/tantivy_inverted_writer.h"

#include <sys/stat.h>

#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/tantivy_ffi/tantivy_ffi.h"
#include "types/logical_type.h"

namespace starrocks {

TantivyInvertedWriter::TantivyInvertedWriter(std::string directory, const TabletIndex* tablet_index)
        : _directory(std::move(directory)), _tablet_index(tablet_index) {}

TantivyInvertedWriter::~TantivyInvertedWriter() {
    if (_writer != nullptr) {
        tantivy_writer_destroy(_writer);
        _writer = nullptr;
    }
}

Status TantivyInvertedWriter::create(const TypeInfoPtr& typeinfo, const std::string& field_name,
                                     const std::string& directory, TabletIndex* tablet_index,
                                     std::unique_ptr<InvertedWriter>* res) {
    LogicalType type = typeinfo->type();
    if (!is_string_type(type)) {
        return Status::NotSupported(
                fmt::format("Tantivy inverted index does not support type: {}", type_to_string_v2(type)));
    }
    *res = std::make_unique<TantivyInvertedWriter>(directory, tablet_index);
    return Status::OK();
}

Status TantivyInvertedWriter::init() {
    // Create index directory (tantivy requires the directory to exist)
    if (::mkdir(_directory.c_str(), 0755) != 0 && errno != EEXIST) {
        return Status::IOError(fmt::format("Failed to create tantivy index directory: {}", _directory));
    }

    std::string parser_str = get_parser_string_from_properties(_tablet_index->index_properties());

    _writer = tantivy_writer_create(_directory.c_str(), TANTIVY_FIELD_NAME.c_str(), parser_str.c_str());
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create tantivy writer");
    }

    return Status::OK();
}

void TantivyInvertedWriter::add_values(const void* values, size_t count) {
    auto* slices = reinterpret_cast<const Slice*>(values);
    for (size_t i = 0; i < count; ++i) {
        // Slice data is not guaranteed to be null-terminated, so copy to std::string
        std::string value(slices[i].data, slices[i].size);
        tantivy_writer_add_doc(_writer, value.c_str(), _rid);
        _total_bytes += slices[i].size;
        ++_rid;
    }
}

void TantivyInvertedWriter::add_nulls(uint32_t count) {
    _null_bitmap.addRange(_rid, _rid + count);
    for (uint32_t i = 0; i < count; ++i) {
        tantivy_writer_add_null(_writer, _rid);
        ++_rid;
    }
}

Status TantivyInvertedWriter::finish(WritableFile* wfile, ColumnMetaPB* meta) {
    if (_writer == nullptr) {
        return Status::InternalError("Tantivy writer not initialized");
    }

    int32_t ret = tantivy_writer_commit(_writer);
    if (ret != 0) {
        return Status::InternalError("Failed to commit tantivy index");
    }

    tantivy_writer_destroy(_writer);
    _writer = nullptr;

    // Write null bitmap to a file in the index directory using FileSystem API.
    _null_bitmap.runOptimize();
    size_t bitmap_size = _null_bitmap.getSizeInBytes(false);
    if (bitmap_size > 0) {
        std::string null_bitmap_path = _directory + "/null_bitmap";
        ASSIGN_OR_RETURN(auto bitmap_file, fs::new_writable_file(null_bitmap_path));
        faststring buf;
        buf.resize(bitmap_size);
        _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
        RETURN_IF_ERROR(bitmap_file->append(Slice(buf.data(), bitmap_size)));
        RETURN_IF_ERROR(bitmap_file->close());
    }

    return Status::OK();
}

} // namespace starrocks
