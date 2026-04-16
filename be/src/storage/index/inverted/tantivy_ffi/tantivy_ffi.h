#ifndef TANTIVY_FFI_H
#define TANTIVY_FFI_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque bitmap handle (query result).
 */
typedef struct TantivyBitmap TantivyBitmap;

/**
 * Opaque reader handle.
 */
typedef struct TantivyReader TantivyReader;

/**
 * Opaque score result handle.
 */
typedef struct TantivyScoreResult TantivyScoreResult;

/**
 * Opaque tokens handle.
 */
typedef struct TantivyTokens TantivyTokens;

/**
 * Opaque writer handle.
 */
typedef struct TantivyWriter TantivyWriter;

struct TantivyWriter *tantivy_writer_create(const char *index_dir,
                                            const char *field_name,
                                            const char *tokenizer_name);

void tantivy_writer_add_doc(struct TantivyWriter *w, const char *value, uint32_t row_id);

void tantivy_writer_add_null(struct TantivyWriter *w, uint32_t row_id);

/**
 * Returns 0 on success, -1 on failure.
 */
int32_t tantivy_writer_commit(struct TantivyWriter *w);

void tantivy_writer_destroy(struct TantivyWriter *w);

struct TantivyReader *tantivy_reader_open(const char *index_dir);

void tantivy_reader_destroy(struct TantivyReader *r);

struct TantivyBitmap *tantivy_query_term(struct TantivyReader *r,
                                         const char *field,
                                         const char *query);

struct TantivyBitmap *tantivy_query_match_any(struct TantivyReader *r,
                                              const char *field,
                                              const char *query);

struct TantivyBitmap *tantivy_query_match_all(struct TantivyReader *r,
                                              const char *field,
                                              const char *query);

struct TantivyBitmap *tantivy_query_phrase(struct TantivyReader *r,
                                           const char *field,
                                           const char *query);

struct TantivyBitmap *tantivy_query_phrase_prefix(struct TantivyReader *r,
                                                  const char *field,
                                                  const char *query);

struct TantivyBitmap *tantivy_query_regexp(struct TantivyReader *r,
                                           const char *field,
                                           const char *pattern);

uint32_t tantivy_bitmap_count(const struct TantivyBitmap *b);

const uint32_t *tantivy_bitmap_row_ids(const struct TantivyBitmap *b);

void tantivy_bitmap_destroy(struct TantivyBitmap *b);

/**
 * query_type: 0=any(OR), 1=all(AND), 2=phrase
 * limit: max results, 0=use default (10000)
 */
struct TantivyScoreResult *tantivy_query_bm25(struct TantivyReader *r,
                                              const char *field,
                                              const char *query,
                                              int32_t query_type,
                                              int32_t limit);

uint32_t tantivy_score_count(const struct TantivyScoreResult *s);

uint32_t tantivy_score_row_id(const struct TantivyScoreResult *s, uint32_t idx);

float tantivy_score_value(const struct TantivyScoreResult *s, uint32_t idx);

void tantivy_score_destroy(struct TantivyScoreResult *s);

struct TantivyTokens *tantivy_tokenize(const char *text, const char *tokenizer_name);

uint32_t tantivy_tokens_count(const struct TantivyTokens *t);

const char *tantivy_tokens_get(const struct TantivyTokens *t, uint32_t idx);

void tantivy_tokens_destroy(struct TantivyTokens *t);

#ifdef __cplusplus
}
#endif

#endif  /* TANTIVY_FFI_H */
