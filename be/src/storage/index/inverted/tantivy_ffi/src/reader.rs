use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::columnar::Column;
use tantivy::query::{
    BooleanQuery, Occur, PhrasePrefixQuery, PhraseQuery, RegexQuery,
    TermQuery,
};
use tantivy::schema::IndexRecordOption;
use tantivy::{Index, IndexReader, ReloadPolicy, Term};

use crate::tokenizer::create_tokenizer_manager;

/// A collector that directly extracts row_ids from fast fields during collection,
/// avoiding the overhead of DocSetCollector's HashSet<DocAddress>.
struct RowIdCollector;

struct RowIdSegmentCollector {
    row_id_column: Column<u64>,
    row_ids: Vec<u32>,
}

impl tantivy::collector::Collector for RowIdCollector {
    type Fruit = Vec<Vec<u32>>;
    type Child = RowIdSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment_reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let row_id_column = segment_reader
            .fast_fields()
            .u64("_row_id")
            .map_err(|e| tantivy::TantivyError::SchemaError(format!(
                "Failed to get _row_id fast field: {}", e
            )))?;
        Ok(RowIdSegmentCollector {
            row_id_column,
            row_ids: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<u32>>) -> tantivy::Result<Vec<Vec<u32>>> {
        Ok(segment_fruits)
    }
}

impl tantivy::collector::SegmentCollector for RowIdSegmentCollector {
    type Fruit = Vec<u32>;

    fn collect(&mut self, doc_id: tantivy::DocId, _score: tantivy::Score) {
        if let Some(rid) = self.row_id_column.values_for_doc(doc_id).next() {
            self.row_ids.push(rid as u32);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.row_ids
    }
}

/// Default upper bound for BM25 "unlimited" queries to prevent OOM.
const BM25_MAX_LIMIT: usize = 10_000;

/// Opaque reader handle exposed via FFI.
pub struct TantivyReaderInner {
    index: Index,
    reader: IndexReader,
}

/// Result of a bitmap query: a sorted vec of matching row_ids.
pub struct TantivyBitmapInner {
    pub row_ids: Vec<u32>,
}

/// Result of a BM25 scored query: vec of (row_id, score) pairs.
pub struct TantivyScoreResultInner {
    pub entries: Vec<(u32, f32)>,
}

impl TantivyReaderInner {
    pub fn open(index_dir: &str) -> Result<Self, String> {
        let dir_path = Path::new(index_dir);
        let mmap_dir = tantivy::directory::MmapDirectory::open(dir_path)
            .map_err(|e| format!("Failed to open MmapDirectory: {}", e))?;

        let index = Index::open(mmap_dir)
            .map_err(|e| format!("Failed to open index: {}", e))?;

        // Register tokenizers
        let tokenizer_manager = create_tokenizer_manager();
        for name in &["none", "standard", "english", "chinese"] {
            if let Some(tok) = tokenizer_manager.get(name) {
                index.tokenizers().register(name, tok);
            }
        }

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| format!("Failed to create reader: {}", e))?;

        Ok(Self { index, reader })
    }

    /// Tokenize the query text using the field's configured tokenizer.
    fn tokenize_query(&self, field_name: &str, query_text: &str) -> Result<Vec<String>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let field_entry = schema.get_field_entry(field);
        let tokenizer_name = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(ref text_options) => text_options
                .get_indexing_options()
                .map(|opts| opts.tokenizer())
                .unwrap_or("standard"),
            _ => "standard",
        };

        let mut tokenizer = self
            .index
            .tokenizers()
            .get(tokenizer_name)
            .ok_or_else(|| format!("Tokenizer not found: {}", tokenizer_name))?;

        let mut token_stream = tokenizer.token_stream(query_text);
        let mut tokens = Vec::new();
        while token_stream.advance() {
            tokens.push(token_stream.token().text.clone());
        }
        Ok(tokens)
    }

    /// Get the fast field column for _row_id from a segment reader.
    fn get_row_id_fast_field(
        &self,
        segment_reader: &tantivy::SegmentReader,
    ) -> Result<Column<u64>, String> {
        let fast_fields = segment_reader.fast_fields();
        fast_fields
            .u64("_row_id")
            .map_err(|e| format!("Failed to get _row_id fast field: {}", e))
    }

    /// Collect all matching row_ids using custom RowIdCollector (extracts row_ids during search,
    /// avoiding DocSetCollector's HashSet overhead and post-search store decompression).
    fn collect_row_ids(
        &self,
        query: Box<dyn tantivy::query::Query>,
    ) -> Result<Vec<u32>, String> {
        let searcher = self.reader.searcher();

        let segment_results = searcher
            .search(&query, &RowIdCollector)
            .map_err(|e| format!("Search failed: {}", e))?;

        // Flatten segment results and sort
        let total: usize = segment_results.iter().map(|v| v.len()).sum();
        let mut row_ids = Vec::with_capacity(total);
        for seg_ids in segment_results {
            row_ids.extend(seg_ids);
        }
        row_ids.sort_unstable();
        Ok(row_ids)
    }

    /// EQUAL_QUERY: exact term match without tokenization.
    /// Case handling follows the field's tokenizer: tokenizers that lowercase
    /// (standard, english, chinese) get lowercased input; parser=none preserves
    /// original case to match the indexed terms exactly.
    pub fn query_term(&self, field_name: &str, query_text: &str) -> Result<Vec<u32>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let field_entry = schema.get_field_entry(field);
        let tokenizer_name = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(ref text_options) => text_options
                .get_indexing_options()
                .map(|opts| opts.tokenizer())
                .unwrap_or("standard"),
            _ => "standard",
        };

        // parser=none indexes terms as-is (case-preserved), so query must also preserve case.
        // All other tokenizers (standard, english, chinese) lowercase during indexing.
        let term_text = if tokenizer_name == "none" {
            query_text.to_string()
        } else {
            query_text.to_lowercase()
        };

        let term = Term::from_field_text(field, &term_text);
        let query = TermQuery::new(term, IndexRecordOption::WithFreqs);
        self.collect_row_ids(Box::new(query))
    }

    /// MATCH_ANY: any token matches (OR semantics).
    pub fn query_match_any(&self, field_name: &str, query_text: &str) -> Result<Vec<u32>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let tokens = self.tokenize_query(field_name, query_text)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = tokens
            .into_iter()
            .map(|t| {
                let term = Term::from_field_text(field, &t);
                let q: Box<dyn tantivy::query::Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, q)
            })
            .collect();

        let query = BooleanQuery::new(subqueries);
        self.collect_row_ids(Box::new(query))
    }

    /// MATCH_ALL: all tokens must match (AND semantics).
    pub fn query_match_all(&self, field_name: &str, query_text: &str) -> Result<Vec<u32>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let tokens = self.tokenize_query(field_name, query_text)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = tokens
            .into_iter()
            .map(|t| {
                let term = Term::from_field_text(field, &t);
                let q: Box<dyn tantivy::query::Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Must, q)
            })
            .collect();

        let query = BooleanQuery::new(subqueries);
        self.collect_row_ids(Box::new(query))
    }

    /// MATCH_PHRASE: tokens must appear in order and adjacent.
    pub fn query_phrase(&self, field_name: &str, query_text: &str) -> Result<Vec<u32>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let tokens = self.tokenize_query(field_name, query_text)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let terms: Vec<Term> = tokens
            .iter()
            .map(|t| Term::from_field_text(field, t))
            .collect();

        // PhraseQuery requires >= 2 terms; for a single token, fall back to TermQuery.
        if terms.len() == 1 {
            let query = TermQuery::new(terms.into_iter().next().unwrap(), IndexRecordOption::WithFreqs);
            self.collect_row_ids(Box::new(query))
        } else {
            let query = PhraseQuery::new(terms);
            self.collect_row_ids(Box::new(query))
        }
    }

    /// MATCH_PHRASE_PREFIX: phrase match with the last token as prefix.
    pub fn query_phrase_prefix(
        &self,
        field_name: &str,
        query_text: &str,
    ) -> Result<Vec<u32>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let tokens = self.tokenize_query(field_name, query_text)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let terms: Vec<Term> = tokens
            .iter()
            .map(|t| Term::from_field_text(field, t))
            .collect();

        // PhrasePrefixQuery requires >= 2 terms; for a single token, use prefix-style RegexQuery.
        if terms.len() == 1 {
            // Escape regex special characters in the prefix token
            let mut escaped = String::new();
            for c in tokens[0].chars() {
                if "\\.*+?()[]{}^$|".contains(c) {
                    escaped.push('\\');
                }
                escaped.push(c);
            }
            let pattern = format!("{}.*", escaped);
            let query = RegexQuery::from_pattern(&pattern, field)
                .map_err(|e| format!("Prefix regex failed: {}", e))?;
            self.collect_row_ids(Box::new(query))
        } else {
            let query = PhrasePrefixQuery::new(terms);
            self.collect_row_ids(Box::new(query))
        }
    }

    /// MATCH_REGEXP: regex match against index terms.
    pub fn query_regexp(&self, field_name: &str, pattern: &str) -> Result<Vec<u32>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let query = RegexQuery::from_pattern(pattern, field)
            .map_err(|e| format!("Invalid regex pattern: {}", e))?;

        self.collect_row_ids(Box::new(query))
    }

    /// BM25 scoring query. Returns (row_id, score) pairs sorted by score descending.
    /// query_type: 0=any(OR), 1=all(AND), 2=phrase
    /// limit: max results, 0=use default limit (10000)
    pub fn query_bm25(
        &self,
        field_name: &str,
        query_text: &str,
        query_type: i32,
        limit: i32,
    ) -> Result<Vec<(u32, f32)>, String> {
        let schema = self.index.schema();
        let field = schema
            .get_field(field_name)
            .map_err(|_| format!("Field not found: {}", field_name))?;

        let tokens = self.tokenize_query(field_name, query_text)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let query: Box<dyn tantivy::query::Query> = match query_type {
            0 => {
                // OR (any)
                let subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = tokens
                    .into_iter()
                    .map(|t| {
                        let term = Term::from_field_text(field, &t);
                        let q: Box<dyn tantivy::query::Query> =
                            Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                        (Occur::Should, q)
                    })
                    .collect();
                Box::new(BooleanQuery::new(subqueries))
            }
            1 => {
                // AND (all)
                let subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = tokens
                    .into_iter()
                    .map(|t| {
                        let term = Term::from_field_text(field, &t);
                        let q: Box<dyn tantivy::query::Query> =
                            Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                        (Occur::Must, q)
                    })
                    .collect();
                Box::new(BooleanQuery::new(subqueries))
            }
            2 => {
                // Phrase
                let terms: Vec<Term> = tokens
                    .iter()
                    .map(|t| Term::from_field_text(field, t))
                    .collect();
                if terms.len() == 1 {
                    Box::new(TermQuery::new(terms.into_iter().next().unwrap(), IndexRecordOption::WithFreqs))
                } else {
                    Box::new(PhraseQuery::new(terms))
                }
            }
            _ => return Err(format!("Invalid query_type: {}", query_type)),
        };

        let actual_limit = if limit <= 0 {
            BM25_MAX_LIMIT
        } else {
            limit as usize
        };

        let searcher = self.reader.searcher();
        let num_segments = searcher.segment_readers().len();
        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(actual_limit))
            .map_err(|e| format!("BM25 search failed: {}", e))?;

        let mut results = Vec::with_capacity(top_docs.len());

        // Pre-allocate fast field readers for all segments (indexed by segment_ord)
        let mut fast_fields: Vec<Option<Column<u64>>> = Vec::with_capacity(num_segments);
        for i in 0..num_segments {
            let segment_reader = searcher.segment_reader(i as u32);
            fast_fields.push(Some(self.get_row_id_fast_field(segment_reader)?));
        }

        for (score, doc_addr) in &top_docs {
            let seg = doc_addr.segment_ord as usize;
            if let Some(ref col) = fast_fields[seg] {
                if let Some(rid) = col.values_for_doc(doc_addr.doc_id).next() {
                    results.push((rid as u32, *score));
                }
            }
        }

        // Re-sort by score descending (segment grouping may have reordered)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::TantivyWriterInner;

    fn create_test_index(dir: &str) {
        let mut writer = TantivyWriterInner::create(dir, "content", "standard").unwrap();
        writer
            .add_doc("StarRocks is a high-performance analytical database", 0)
            .unwrap();
        writer
            .add_doc("Full text search enables users to find relevant documents", 1)
            .unwrap();
        writer
            .add_doc("Optimizing database performance requires careful analysis", 2)
            .unwrap();
        writer
            .add_doc("Real-time analytics engine for modern data applications", 3)
            .unwrap();
        writer
            .add_doc("Search engine design involves inverted index and ranking", 4)
            .unwrap();
        writer.commit().unwrap();
    }

    #[test]
    fn test_match_any() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        let results = reader.query_match_any("content", "database performance").unwrap();
        // "database" in doc 0, 2; "performance" in doc 0, 2 → union = {0, 2}
        assert!(results.contains(&0));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_match_all() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        let results = reader.query_match_all("content", "database performance").unwrap();
        // Both "database" AND "performance" must be present
        assert!(results.contains(&2));
    }

    #[test]
    fn test_phrase() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        let results = reader.query_phrase("content", "text search").unwrap();
        // "text search" as adjacent tokens → doc 1 ("full text search")
        assert!(results.contains(&1));
        assert!(!results.contains(&4));
    }

    #[test]
    fn test_phrase_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        let results = reader.query_phrase_prefix("content", "search eng").unwrap();
        // "search" + prefix "eng" → matches "search engine" in doc 4
        assert!(results.contains(&4));
    }

    #[test]
    fn test_regexp() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        let results = reader.query_regexp("content", "data.*").unwrap();
        // Terms starting with "data": "database" (docs 0,2), "data" (doc 3)
        assert!(!results.is_empty());
    }

    #[test]
    fn test_bm25() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        let results = reader.query_bm25("content", "database", 0, 10).unwrap();
        assert!(!results.is_empty());
        // All scores should be > 0
        for (_rid, score) in &results {
            assert!(*score > 0.0);
        }
        // Results should be sorted by score descending
        for i in 1..results.len() {
            assert!(results[i - 1].1 >= results[i].1);
        }
    }

    #[test]
    fn test_term_query_standard() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path); // uses "standard" tokenizer

        let reader = TantivyReaderInner::open(dir_path).unwrap();

        // "database" matches docs 0 and 2
        let results = reader.query_term("content", "database").unwrap();
        assert!(results.contains(&0));
        assert!(results.contains(&2));

        // "Database" (uppercase) should also match because standard tokenizer lowercases
        let results_upper = reader.query_term("content", "Database").unwrap();
        assert_eq!(results, results_upper);

        // multi-word "database performance" is treated as one term, no match
        let results_multi = reader.query_term("content", "database performance").unwrap();
        assert!(results_multi.is_empty());
    }

    #[test]
    fn test_term_query_none_case_sensitive() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        // Create index with parser=none (case-preserving, no tokenization)
        let mut writer = TantivyWriterInner::create(dir_path, "content", "none").unwrap();
        writer.add_doc("Hello", 0).unwrap();
        writer.add_doc("hello", 1).unwrap();
        writer.add_doc("HELLO", 2).unwrap();
        writer.commit().unwrap();

        let reader = TantivyReaderInner::open(dir_path).unwrap();

        // parser=none: "Hello" matches only doc 0 (exact case)
        let results = reader.query_term("content", "Hello").unwrap();
        assert_eq!(results, vec![0]);

        // "hello" matches only doc 1
        let results = reader.query_term("content", "hello").unwrap();
        assert_eq!(results, vec![1]);

        // "HELLO" matches only doc 2
        let results = reader.query_term("content", "HELLO").unwrap();
        assert_eq!(results, vec![2]);
    }

    #[test]
    fn test_bm25_default_limit() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        create_test_index(dir_path);

        let reader = TantivyReaderInner::open(dir_path).unwrap();
        // limit=0 should use BM25_MAX_LIMIT (10000), not u32::MAX
        let results = reader.query_bm25("content", "database", 0, 0).unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= BM25_MAX_LIMIT);
    }
}
