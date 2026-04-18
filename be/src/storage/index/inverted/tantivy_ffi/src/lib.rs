pub mod reader;
pub mod tokenizer;
pub mod writer;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;

// ========================== Opaque Types ==========================

/// Opaque writer handle.
pub struct TantivyWriter {
    inner: writer::TantivyWriterInner,
}

/// Opaque bitmap handle (query result).
pub struct TantivyBitmap {
    inner: reader::TantivyBitmapInner,
}

/// Opaque reader handle.
pub struct TantivyReader {
    inner: reader::TantivyReaderInner,
}

/// Opaque score result handle.
pub struct TantivyScoreResult {
    inner: reader::TantivyScoreResultInner,
}

/// Opaque tokens handle.
pub struct TantivyTokens {
    tokens: Vec<CString>,
}

// ========================== Helpers ==========================

/// Safely convert a C string pointer to &str. Returns None if ptr is null or invalid UTF-8.
unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    CStr::from_ptr(ptr).to_str().ok()
}

// ========================== Writer ==========================

#[no_mangle]
pub unsafe extern "C" fn tantivy_writer_create(
    index_dir: *const c_char,
    field_name: *const c_char,
    tokenizer_name: *const c_char,
) -> *mut TantivyWriter {
    let index_dir = match cstr_to_str(index_dir) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let field_name = match cstr_to_str(field_name) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let tokenizer_name = match cstr_to_str(tokenizer_name) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };

    match writer::TantivyWriterInner::create(index_dir, field_name, tokenizer_name) {
        Ok(inner) => Box::into_raw(Box::new(TantivyWriter { inner })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_writer_add_doc(
    w: *mut TantivyWriter,
    value: *const c_char,
    row_id: u32,
) {
    if w.is_null() {
        return;
    }
    let value = match cstr_to_str(value) {
        Some(s) => s,
        None => return,
    };
    let w = &mut *w;
    let _ = w.inner.add_doc(value, row_id);
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_writer_add_null(w: *mut TantivyWriter, row_id: u32) {
    if w.is_null() {
        return;
    }
    let w = &mut *w;
    let _ = w.inner.add_null(row_id);
}

/// Returns 0 on success, -1 on failure.
#[no_mangle]
pub unsafe extern "C" fn tantivy_writer_commit(w: *mut TantivyWriter) -> i32 {
    if w.is_null() {
        return -1;
    }
    let w = &mut *w;
    match w.inner.commit() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_writer_destroy(w: *mut TantivyWriter) {
    if !w.is_null() {
        drop(Box::from_raw(w));
    }
}

// ========================== Reader ==========================

#[no_mangle]
pub unsafe extern "C" fn tantivy_reader_open(index_dir: *const c_char) -> *mut TantivyReader {
    let index_dir = match cstr_to_str(index_dir) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };

    match reader::TantivyReaderInner::open(index_dir) {
        Ok(inner) => Box::into_raw(Box::new(TantivyReader { inner })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_reader_destroy(r: *mut TantivyReader) {
    if !r.is_null() {
        drop(Box::from_raw(r));
    }
}

// ========================== Query (Bitmap) ==========================

unsafe fn do_query<F>(r: *mut TantivyReader, field: *const c_char, query: *const c_char, f: F) -> *mut TantivyBitmap
where
    F: FnOnce(&reader::TantivyReaderInner, &str, &str) -> Result<Vec<u32>, String> + panic::UnwindSafe,
{
    if r.is_null() {
        return std::ptr::null_mut();
    }
    let field = match cstr_to_str(field) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let query = match cstr_to_str(query) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let r = &*r;

    // catch_unwind prevents Rust panics from unwinding across FFI boundary
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| f(&r.inner, field, query)));
    match result {
        Ok(Ok(row_ids)) => Box::into_raw(Box::new(TantivyBitmap {
            inner: reader::TantivyBitmapInner { row_ids },
        })),
        Ok(Err(_)) | Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_query_term(
    r: *mut TantivyReader,
    field: *const c_char,
    query: *const c_char,
) -> *mut TantivyBitmap {
    do_query(r, field, query, |inner, f, q| inner.query_term(f, q))
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_query_match_any(
    r: *mut TantivyReader,
    field: *const c_char,
    query: *const c_char,
) -> *mut TantivyBitmap {
    do_query(r, field, query, |inner, f, q| inner.query_match_any(f, q))
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_query_match_all(
    r: *mut TantivyReader,
    field: *const c_char,
    query: *const c_char,
) -> *mut TantivyBitmap {
    do_query(r, field, query, |inner, f, q| inner.query_match_all(f, q))
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_query_phrase(
    r: *mut TantivyReader,
    field: *const c_char,
    query: *const c_char,
) -> *mut TantivyBitmap {
    do_query(r, field, query, |inner, f, q| inner.query_phrase(f, q))
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_query_phrase_prefix(
    r: *mut TantivyReader,
    field: *const c_char,
    query: *const c_char,
) -> *mut TantivyBitmap {
    do_query(r, field, query, |inner, f, q| {
        inner.query_phrase_prefix(f, q)
    })
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_query_regexp(
    r: *mut TantivyReader,
    field: *const c_char,
    pattern: *const c_char,
) -> *mut TantivyBitmap {
    do_query(r, field, pattern, |inner, f, p| inner.query_regexp(f, p))
}

// ========================== Bitmap accessors ==========================

#[no_mangle]
pub unsafe extern "C" fn tantivy_bitmap_count(b: *const TantivyBitmap) -> u32 {
    if b.is_null() {
        return 0;
    }
    (*b).inner.row_ids.len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_bitmap_row_ids(b: *const TantivyBitmap) -> *const u32 {
    if b.is_null() {
        return std::ptr::null();
    }
    (*b).inner.row_ids.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_bitmap_destroy(b: *mut TantivyBitmap) {
    if !b.is_null() {
        drop(Box::from_raw(b));
    }
}

// ========================== BM25 ==========================

/// query_type: 0=any(OR), 1=all(AND), 2=phrase
/// limit: max results, 0=use default (10000)
#[no_mangle]
pub unsafe extern "C" fn tantivy_query_bm25(
    r: *mut TantivyReader,
    field: *const c_char,
    query: *const c_char,
    query_type: i32,
    limit: i32,
) -> *mut TantivyScoreResult {
    if r.is_null() {
        return std::ptr::null_mut();
    }
    let field = match cstr_to_str(field) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let query = match cstr_to_str(query) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let r = &*r;

    // catch_unwind prevents Rust panics from unwinding across FFI boundary
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        r.inner.query_bm25(field, query, query_type, limit)
    }));
    match result {
        Ok(Ok(entries)) => Box::into_raw(Box::new(TantivyScoreResult {
            inner: reader::TantivyScoreResultInner { entries },
        })),
        Ok(Err(_)) | Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_score_count(s: *const TantivyScoreResult) -> u32 {
    if s.is_null() {
        return 0;
    }
    (*s).inner.entries.len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_score_row_id(s: *const TantivyScoreResult, idx: u32) -> u32 {
    if s.is_null() {
        return 0;
    }
    let entries = &(*s).inner.entries;
    if (idx as usize) < entries.len() {
        entries[idx as usize].0
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_score_value(s: *const TantivyScoreResult, idx: u32) -> f32 {
    if s.is_null() {
        return 0.0;
    }
    let entries = &(*s).inner.entries;
    if (idx as usize) < entries.len() {
        entries[idx as usize].1
    } else {
        0.0
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_score_destroy(s: *mut TantivyScoreResult) {
    if !s.is_null() {
        drop(Box::from_raw(s));
    }
}

// ========================== Tokenize ==========================

#[no_mangle]
pub unsafe extern "C" fn tantivy_tokenize(
    text: *const c_char,
    tokenizer_name: *const c_char,
) -> *mut TantivyTokens {
    let text = match cstr_to_str(text) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let tokenizer_name = match cstr_to_str(tokenizer_name) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };

    // catch_unwind prevents Rust panics from unwinding across FFI boundary
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        tokenizer::tokenize_text(text, tokenizer_name)
    }));
    match result {
        Ok(Ok(tokens)) => {
            let c_tokens: Vec<CString> = tokens
                .into_iter()
                .filter_map(|t| CString::new(t).ok())
                .collect();
            Box::into_raw(Box::new(TantivyTokens { tokens: c_tokens }))
        }
        Ok(Err(_)) | Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_tokens_count(t: *const TantivyTokens) -> u32 {
    if t.is_null() {
        return 0;
    }
    (*t).tokens.len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_tokens_get(t: *const TantivyTokens, idx: u32) -> *const c_char {
    if t.is_null() {
        return std::ptr::null();
    }
    let tokens = &(*t).tokens;
    if (idx as usize) < tokens.len() {
        tokens[idx as usize].as_ptr()
    } else {
        std::ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_tokens_destroy(t: *mut TantivyTokens) {
    if !t.is_null() {
        drop(Box::from_raw(t));
    }
}

// ========================== Integration Tests ==========================

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_ffi_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let field = CString::new("content").unwrap();
        let tokenizer = CString::new("standard").unwrap();

        unsafe {
            // Write
            let w = tantivy_writer_create(
                dir_path.as_ptr(),
                field.as_ptr(),
                tokenizer.as_ptr(),
            );
            assert!(!w.is_null());

            let doc0 = CString::new("hello world database").unwrap();
            let doc1 = CString::new("foo bar database performance").unwrap();
            let doc2 = CString::new("search engine design").unwrap();

            tantivy_writer_add_doc(w, doc0.as_ptr(), 0);
            tantivy_writer_add_doc(w, doc1.as_ptr(), 1);
            tantivy_writer_add_doc(w, doc2.as_ptr(), 2);
            assert_eq!(tantivy_writer_commit(w), 0);
            tantivy_writer_destroy(w);

            // Read
            let r = tantivy_reader_open(dir_path.as_ptr());
            assert!(!r.is_null());

            // MATCH_ANY "database"
            let q = CString::new("database").unwrap();
            let b = tantivy_query_match_any(r, field.as_ptr(), q.as_ptr());
            assert!(!b.is_null());
            let count = tantivy_bitmap_count(b);
            assert_eq!(count, 2); // doc 0 and 1
            tantivy_bitmap_destroy(b);

            // MATCH_ALL "database performance"
            let q2 = CString::new("database performance").unwrap();
            let b2 = tantivy_query_match_all(r, field.as_ptr(), q2.as_ptr());
            assert!(!b2.is_null());
            let count2 = tantivy_bitmap_count(b2);
            assert_eq!(count2, 1); // only doc 1
            tantivy_bitmap_destroy(b2);

            // PHRASE "hello world"
            let q3 = CString::new("hello world").unwrap();
            let b3 = tantivy_query_phrase(r, field.as_ptr(), q3.as_ptr());
            assert!(!b3.is_null());
            let count3 = tantivy_bitmap_count(b3);
            assert_eq!(count3, 1); // doc 0
            tantivy_bitmap_destroy(b3);

            // PHRASE_PREFIX "search eng"
            let q4 = CString::new("search eng").unwrap();
            let b4 = tantivy_query_phrase_prefix(r, field.as_ptr(), q4.as_ptr());
            assert!(!b4.is_null());
            let count4 = tantivy_bitmap_count(b4);
            assert_eq!(count4, 1); // doc 2
            tantivy_bitmap_destroy(b4);

            // REGEXP "data.*"
            let q5 = CString::new("data.*").unwrap();
            let b5 = tantivy_query_regexp(r, field.as_ptr(), q5.as_ptr());
            assert!(!b5.is_null());
            let count5 = tantivy_bitmap_count(b5);
            assert!(count5 >= 2); // docs 0, 1
            tantivy_bitmap_destroy(b5);

            // BM25
            let q6 = CString::new("database").unwrap();
            let s = tantivy_query_bm25(r, field.as_ptr(), q6.as_ptr(), 0, 10);
            assert!(!s.is_null());
            let sc = tantivy_score_count(s);
            assert!(sc >= 1);
            let score0 = tantivy_score_value(s, 0);
            assert!(score0 > 0.0);
            tantivy_score_destroy(s);

            tantivy_reader_destroy(r);
        }
    }

    #[test]
    fn test_ffi_tokenize() {
        unsafe {
            let text = CString::new("Hello World").unwrap();
            let tok_name = CString::new("standard").unwrap();

            let t = tantivy_tokenize(text.as_ptr(), tok_name.as_ptr());
            assert!(!t.is_null());

            let count = tantivy_tokens_count(t);
            assert_eq!(count, 2);

            let t0 = CStr::from_ptr(tantivy_tokens_get(t, 0)).to_str().unwrap();
            let t1 = CStr::from_ptr(tantivy_tokens_get(t, 1)).to_str().unwrap();
            assert_eq!(t0, "hello");
            assert_eq!(t1, "world");

            tantivy_tokens_destroy(t);
        }
    }

    #[test]
    fn test_ffi_null_pointer_safety() {
        unsafe {
            // All functions should return null/0/-1 for null inputs, not crash
            assert!(tantivy_writer_create(std::ptr::null(), std::ptr::null(), std::ptr::null()).is_null());
            assert!(tantivy_reader_open(std::ptr::null()).is_null());
            assert!(tantivy_query_match_any(std::ptr::null_mut(), std::ptr::null(), std::ptr::null()).is_null());
            assert!(tantivy_query_bm25(std::ptr::null_mut(), std::ptr::null(), std::ptr::null(), 0, 10).is_null());
            assert!(tantivy_tokenize(std::ptr::null(), std::ptr::null()).is_null());

            // Accessors with null handles
            assert_eq!(tantivy_bitmap_count(std::ptr::null()), 0);
            assert!(tantivy_bitmap_row_ids(std::ptr::null()).is_null());
            assert_eq!(tantivy_score_count(std::ptr::null()), 0);
            assert_eq!(tantivy_score_row_id(std::ptr::null(), 0), 0);
            assert_eq!(tantivy_score_value(std::ptr::null(), 0), 0.0);
            assert_eq!(tantivy_tokens_count(std::ptr::null()), 0);
            assert!(tantivy_tokens_get(std::ptr::null(), 0).is_null());

            // Destroy with null should be no-op (not crash)
            tantivy_writer_destroy(std::ptr::null_mut());
            tantivy_reader_destroy(std::ptr::null_mut());
            tantivy_bitmap_destroy(std::ptr::null_mut());
            tantivy_score_destroy(std::ptr::null_mut());
            tantivy_tokens_destroy(std::ptr::null_mut());

            // Writer ops with null handle
            tantivy_writer_add_doc(std::ptr::null_mut(), std::ptr::null(), 0);
            tantivy_writer_add_null(std::ptr::null_mut(), 0);
            assert_eq!(tantivy_writer_commit(std::ptr::null_mut()), -1);
        }
    }
}
