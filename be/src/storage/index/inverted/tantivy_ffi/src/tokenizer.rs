use std::sync::LazyLock;
use tantivy::tokenizer::{
    LowerCaser, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer, Token,
    TextAnalyzer, Tokenizer, TokenStream, TokenizerManager,
};

/// Global jieba instance — dictionary loading is expensive (~100ms),
/// so we initialize once and share across all tokenize calls.
static JIEBA: LazyLock<jieba_rs::Jieba> = LazyLock::new(|| jieba_rs::Jieba::new());

/// A jieba-based tokenizer that implements tantivy's Tokenizer trait.
#[derive(Clone)]
pub struct JiebaTokenizer;

pub struct JiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let words = JIEBA.cut(text, true); // precise mode

        let mut tokens = Vec::new();
        let mut offset = 0usize;
        for word in words {
            let start = text[offset..].find(word).map(|i| i + offset).unwrap_or(offset);
            let end = start + word.len();
            tokens.push(Token {
                offset_from: start,
                offset_to: end,
                position: tokens.len(),
                text: word.to_string(),
                position_length: 1,
            });
            offset = end;
        }

        JiebaTokenStream { tokens, index: 0 }
    }
}

impl TokenStream for JiebaTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

/// Register all supported tokenizers into a TokenizerManager.
pub fn create_tokenizer_manager() -> TokenizerManager {
    let manager = TokenizerManager::new();

    // "none" — no tokenization, the entire string is a single term
    manager.register(
        "none",
        TextAnalyzer::builder(RawTokenizer::default())
            .filter(RemoveLongFilter::limit(256))
            .build(),
    );

    // "standard" — split on non-alphanumeric + lowercase
    manager.register(
        "standard",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(256))
            .filter(LowerCaser)
            .build(),
    );

    // "english" — standard + Porter2 stemming
    manager.register(
        "english",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(256))
            .filter(LowerCaser)
            .filter(Stemmer::new(tantivy::tokenizer::Language::English))
            .build(),
    );

    // "chinese" — jieba segmentation + lowercase for latin chars
    manager.register(
        "chinese",
        TextAnalyzer::builder(JiebaTokenizer)
            .filter(RemoveLongFilter::limit(256))
            .filter(LowerCaser)
            .build(),
    );

    manager
}

/// Tokenize text using the specified tokenizer, returning a Vec of token strings.
pub fn tokenize_text(text: &str, tokenizer_name: &str) -> Result<Vec<String>, String> {
    let manager = create_tokenizer_manager();
    let mut tokenizer = manager
        .get(tokenizer_name)
        .ok_or_else(|| format!("Unknown tokenizer: {}", tokenizer_name))?;

    let mut token_stream = tokenizer.token_stream(text);
    let mut tokens = Vec::new();
    while token_stream.advance() {
        tokens.push(token_stream.token().text.clone());
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_tokenizer() {
        let tokens = tokenize_text("Hello World", "standard").unwrap();
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn test_english_tokenizer() {
        let tokens = tokenize_text("running databases", "english").unwrap();
        // Porter2: "running" → "run", "databases" → "databas"
        assert_eq!(tokens, vec!["run", "databas"]);
    }

    #[test]
    fn test_none_tokenizer() {
        let tokens = tokenize_text("Hello World", "none").unwrap();
        assert_eq!(tokens, vec!["Hello World"]);
    }

    #[test]
    fn test_chinese_tokenizer() {
        let tokens = tokenize_text("全文检索引擎", "chinese").unwrap();
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_unknown_tokenizer() {
        let result = tokenize_text("hello", "nonexistent");
        assert!(result.is_err());
    }
}
