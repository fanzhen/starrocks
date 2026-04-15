use std::path::Path;
use tantivy::schema::{Schema, TextFieldIndexing, TextOptions, IndexRecordOption, STORED};
use tantivy::{doc, Index, IndexWriter};

use crate::tokenizer::create_tokenizer_manager;

/// Opaque writer handle exposed via FFI.
pub struct TantivyWriterInner {
    index_writer: IndexWriter,
    field: tantivy::schema::Field,
    row_id_field: tantivy::schema::Field,
    _index: Index,
}

impl TantivyWriterInner {
    pub fn create(index_dir: &str, field_name: &str, tokenizer_name: &str) -> Result<Self, String> {
        let dir_path = Path::new(index_dir);
        std::fs::create_dir_all(dir_path)
            .map_err(|e| format!("Failed to create index dir {}: {}", index_dir, e))?;

        // Build schema: one text field (indexed + stored) + one u64 row_id field (stored + fast)
        let text_options = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(tokenizer_name)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();

        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field(field_name, text_options);
        let row_id_field = schema_builder.add_u64_field(
            "_row_id",
            tantivy::schema::FAST | STORED,
        );
        let schema = schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(dir_path)
            .map_err(|e| format!("Failed to open MmapDirectory: {}", e))?;

        let index = Index::open_or_create(mmap_dir, schema)
            .map_err(|e| format!("Failed to create index: {}", e))?;

        // Register tokenizers
        let tokenizer_manager = create_tokenizer_manager();
        // Copy tokenizers from our manager to the index's tokenizer manager
        for name in &["none", "standard", "english", "chinese"] {
            if let Some(tok) = tokenizer_manager.get(name) {
                index.tokenizers().register(name, tok);
            }
        }

        // 50MB heap for indexing
        let index_writer = index
            .writer(50_000_000)
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        Ok(Self {
            index_writer,
            field,
            row_id_field,
            _index: index,
        })
    }

    pub fn add_doc(&mut self, value: &str, row_id: u32) -> Result<(), String> {
        self.index_writer
            .add_document(doc!(
                self.field => value,
                self.row_id_field => row_id as u64,
            ))
            .map_err(|e| format!("Failed to add document: {}", e))?;
        Ok(())
    }

    pub fn add_null(&mut self, row_id: u32) -> Result<(), String> {
        // For NULL values, add a document with only the row_id (no text field value)
        self.index_writer
            .add_document(doc!(
                self.row_id_field => row_id as u64,
            ))
            .map_err(|e| format!("Failed to add null document: {}", e))?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.index_writer
            .commit()
            .map_err(|e| format!("Failed to commit: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_write() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut writer = TantivyWriterInner::create(dir_path, "content", "standard").unwrap();
        writer.add_doc("hello world", 0).unwrap();
        writer.add_doc("foo bar baz", 1).unwrap();
        writer.add_null(2).unwrap();
        writer.commit().unwrap();

        // Verify index files exist
        assert!(dir.path().join("meta.json").exists());
    }
}
