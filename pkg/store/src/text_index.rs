//! Per-tenant full-text index: Unicode word segmentation, BM25 scoring.
//!
//! One [`TenantTextIndex`] per tenant holds an inverted index from analysed
//! term to a posting list `(document slot, term frequency)` sorted by slot,
//! plus each document's length, so BM25 needs no pass over the tenant at
//! query time. The store keeps it in step with every claim upsert, re-upsert
//! and tombstone (it is updated by the same functions that maintain the
//! other claim indexes), so the WAL replay at startup, a replication
//! follower applying frames and a resync all rebuild exactly the same index.
//! It is not persisted: building it is part of the replay (see ADR 0003,
//! section 12, for the measured cost).
//!
//! # Analysis
//!
//! [`analyze`] splits text into words with the Unicode word boundary rules
//! (UAX #29), lowercases them with full Unicode case mapping, maps the
//! typographic apostrophe to `'`, stems words written only in ASCII letters
//! (and apostrophes) with the English Snowball stemmer and truncates terms
//! to [`MAX_TERM_CHARS`] characters. Words in other scripts are kept as they
//! are (lowercased). Han and Hiragana characters have no word boundaries in
//! UAX #29, so each character is a term; Katakana runs, Hangul words and
//! Latin, Greek or Cyrillic words are single terms.
//!
//! [`analyze_query`] analyses a query the same way and additionally drops
//! English stop words ("the", "of", ...) unless the query holds nothing
//! else, so "the merger" searches for "merger" while "the who" still finds
//! documents with those words. Documents are indexed with their stop words.
//!
//! # Scoring
//!
//! BM25 with `k1 = 1.2`, `b = 0.75` and the non-negative idf
//! `ln(1 + (N - df + 0.5) / (df + 0.5))` (the Lucene / tantivy form), where
//! `N` is the number of documents in the tenant, `df` the number of
//! documents containing the term, and document length is counted in terms.
//! Repeated query terms count once. Scores are summed in query-term order in
//! `f64`, so every replica computes bit-identical scores from the same log.

use std::collections::HashMap;

use rust_stemmers::{Algorithm, Stemmer};
use unicode_segmentation::UnicodeSegmentation;

/// BM25 term-frequency saturation.
pub const BM25_K1: f64 = 1.2;
/// BM25 length normalisation.
pub const BM25_B: f64 = 0.75;
/// Longer words are truncated to this many characters (bounds the memory a
/// single pathological token can take).
pub const MAX_TERM_CHARS: usize = 64;

/// English stop words removed from queries (not from documents).
const STOP_WORDS: &[&str] = &[
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are",
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but",
    "by", "can", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for",
    "from", "further", "had", "has", "have", "having", "he", "her", "here", "hers", "herself",
    "him", "himself", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just",
    "me", "more", "most", "my", "myself", "no", "nor", "not", "now", "of", "off", "on", "once",
    "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "same", "she",
    "should", "so", "some", "such", "than", "that", "the", "their", "theirs", "them",
    "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "which",
    "while", "who", "whom", "why", "will", "with", "would", "you", "your", "yours", "yourself",
    "yourselves",
];

fn is_stop_word(word: &str) -> bool {
    STOP_WORDS.binary_search(&word).is_ok()
}

fn stemmer() -> &'static Stemmer {
    use std::sync::OnceLock;
    static STEMMER: OnceLock<Stemmer> = OnceLock::new();
    STEMMER.get_or_init(|| Stemmer::create(Algorithm::English))
}

/// Lowercased, apostrophe-normalised words of `text`, before stemming.
fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.unicode_words().map(|word| {
        let lower = word.to_lowercase();
        if lower.contains('\u{2019}') {
            lower.replace('\u{2019}', "'")
        } else {
            lower
        }
    })
}

/// The index term of one lowercased word.
fn term_of(word: &str) -> String {
    let stemmable = word.bytes().any(|b| b.is_ascii_alphabetic())
        && word.bytes().all(|b| b.is_ascii_alphabetic() || b == b'\'');
    let mut term = if stemmable {
        let stemmed = stemmer().stem(word);
        let cleaned: String = stemmed.chars().filter(|c| *c != '\'').collect();
        if cleaned.is_empty() {
            word.chars().filter(|c| *c != '\'').collect()
        } else {
            cleaned
        }
    } else {
        word.to_string()
    };
    if let Some((cut, _)) = term.char_indices().nth(MAX_TERM_CHARS) {
        term.truncate(cut);
    }
    term
}

/// The index terms of `text`, in order, with repetitions.
pub fn analyze(text: &str) -> Vec<String> {
    words(text)
        .map(|word| term_of(&word))
        .filter(|term| !term.is_empty())
        .collect()
}

/// The terms a query searches for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTerms {
    /// Distinct terms, in first-occurrence order, stop words removed unless
    /// the query has nothing else.
    pub terms: Vec<String>,
}

impl QueryTerms {
    /// `true` when the query has no word at all (empty, or punctuation and
    /// symbols only).
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}

/// Analyse a query: like [`analyze`], minus stop words (unless only stop
/// words are present), each term once.
pub fn analyze_query(query: &str) -> QueryTerms {
    let all: Vec<String> = words(query).collect();
    let content: Vec<&String> = all.iter().filter(|w| !is_stop_word(w)).collect();
    let chosen: Vec<&String> = if content.is_empty() {
        all.iter().collect()
    } else {
        content
    };
    let mut terms: Vec<String> = Vec::with_capacity(chosen.len());
    for word in chosen {
        let term = term_of(word);
        if !term.is_empty() && !terms.contains(&term) {
            terms.push(term);
        }
    }
    QueryTerms { terms }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Posting {
    doc: u32,
    tf: u32,
}

#[derive(Debug, Clone)]
struct DocEntry {
    claim_id: Box<str>,
    len: u32,
}

/// BM25 inputs of one query term, resolved once per query.
#[derive(Debug, Clone)]
pub struct ResolvedTerm<'a> {
    postings: &'a [Posting],
    idf: f64,
}

/// One tenant's inverted index. See the module docs.
#[derive(Debug, Clone, Default)]
pub struct TenantTextIndex {
    slots: HashMap<String, u32>,
    docs: Vec<Option<DocEntry>>,
    free: Vec<u32>,
    postings: HashMap<Box<str>, Vec<Posting>>,
    total_len: u64,
}

impl TenantTextIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of indexed documents.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Number of distinct terms.
    pub fn term_count(&self) -> usize {
        self.postings.len()
    }

    /// Average document length in terms (0 for an empty index).
    pub fn avg_doc_len(&self) -> f64 {
        if self.slots.is_empty() {
            0.0
        } else {
            self.total_len as f64 / self.slots.len() as f64
        }
    }

    /// Number of documents containing `term` (an analysed term).
    pub fn doc_freq(&self, term: &str) -> usize {
        self.postings.get(term).map(Vec::len).unwrap_or(0)
    }

    pub fn contains(&self, claim_id: &str) -> bool {
        self.slots.contains_key(claim_id)
    }

    /// Index `claim_id` with `text`. A document already indexed under
    /// `claim_id` must be removed first ([`Self::remove`] with its text).
    pub fn insert(&mut self, claim_id: &str, text: &str) {
        debug_assert!(!self.slots.contains_key(claim_id), "insert of an indexed id");
        if self.slots.contains_key(claim_id) {
            self.remove_by_scan(claim_id);
        }
        let terms = analyze(text);
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.docs.push(None);
                (self.docs.len() - 1) as u32
            }
        };
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for term in &terms {
            *tf.entry(term.as_str()).or_insert(0) += 1;
        }
        for (term, count) in tf {
            let list = match self.postings.get_mut(term) {
                Some(list) => list,
                None => self.postings.entry(Box::from(term)).or_default(),
            };
            let at = list.partition_point(|p| p.doc < slot);
            list.insert(
                at,
                Posting {
                    doc: slot,
                    tf: count,
                },
            );
        }
        let len = terms.len() as u32;
        self.total_len += u64::from(len);
        self.docs[slot as usize] = Some(DocEntry {
            claim_id: Box::from(claim_id),
            len,
        });
        self.slots.insert(claim_id.to_string(), slot);
    }

    /// Remove `claim_id`, which was indexed with `text` (the analyser is
    /// deterministic, so re-analysing the text finds exactly its posting
    /// entries without storing a term list per document). A no-op when the
    /// id is not indexed; returns whether anything was removed. Emptied
    /// posting lists are dropped, so term and document statistics always
    /// describe the live documents only. Should `text` not be the indexed
    /// text, the removal falls back to a scan of every posting list, so the
    /// index never keeps a stale entry.
    pub fn remove(&mut self, claim_id: &str, text: &str) -> bool {
        let Some(&slot) = self.slots.get(claim_id) else {
            return false;
        };
        let expected_len = self.docs[slot as usize].as_ref().map(|d| d.len).unwrap_or(0);
        let mut terms = analyze(text);
        if terms.len() as u32 != expected_len {
            return self.remove_by_scan(claim_id);
        }
        terms.sort_unstable();
        terms.dedup();
        let present = terms.iter().all(|term| {
            self.postings
                .get(term.as_str())
                .is_some_and(|list| list.binary_search_by(|p| p.doc.cmp(&slot)).is_ok())
        });
        if !present {
            return self.remove_by_scan(claim_id);
        }
        for term in &terms {
            let list = self.postings.get_mut(term.as_str()).expect("checked above");
            let at = list
                .binary_search_by(|p| p.doc.cmp(&slot))
                .expect("checked above");
            list.remove(at);
            if list.is_empty() {
                self.postings.remove(term.as_str());
            }
        }
        self.release_slot(claim_id, slot);
        true
    }

    fn remove_by_scan(&mut self, claim_id: &str) -> bool {
        let Some(&slot) = self.slots.get(claim_id) else {
            return false;
        };
        self.postings.retain(|_, list| {
            if let Ok(at) = list.binary_search_by(|p| p.doc.cmp(&slot)) {
                list.remove(at);
            }
            !list.is_empty()
        });
        self.release_slot(claim_id, slot);
        true
    }

    fn release_slot(&mut self, claim_id: &str, slot: u32) {
        self.slots.remove(claim_id);
        let entry = self.docs[slot as usize]
            .take()
            .expect("an interned slot holds a document");
        self.total_len -= u64::from(entry.len);
        self.free.push(slot);
        if self.slots.is_empty() {
            self.docs.clear();
            self.free.clear();
        }
    }

    fn idf(&self, df: usize) -> f64 {
        let n = self.slots.len() as f64;
        let df = df as f64;
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
    }

    /// Resolve query terms against this index (terms absent from the index
    /// are skipped).
    pub fn resolve<'a>(&'a self, query: &QueryTerms) -> Vec<ResolvedTerm<'a>> {
        query
            .terms
            .iter()
            .filter_map(|term| {
                let postings = self.postings.get(term.as_str())?;
                Some(ResolvedTerm {
                    postings,
                    idf: self.idf(postings.len()),
                })
            })
            .collect()
    }

    /// The largest score any document could reach for `query`
    /// (`sum(idf * (k1 + 1))` over the query terms present in the index),
    /// used to map BM25 into `[0, 1)`. Zero when no term is present.
    pub fn max_score(&self, query: &QueryTerms) -> f64 {
        self.resolve(query)
            .iter()
            .map(|term| term.idf * (BM25_K1 + 1.0))
            .sum()
    }

    fn length_norm(&self, len: u32) -> f64 {
        let avg = self.avg_doc_len().max(f64::EPSILON);
        BM25_K1 * (1.0 - BM25_B + BM25_B * (f64::from(len) / avg))
    }

    fn term_weight(idf: f64, tf: u32, norm: f64) -> f64 {
        let tf = f64::from(tf);
        idf * (tf * (BM25_K1 + 1.0)) / (tf + norm)
    }

    /// BM25 of one document (0 when it is not indexed or shares no term).
    pub fn score(&self, query: &QueryTerms, claim_id: &str) -> f64 {
        let Some(&slot) = self.slots.get(claim_id) else {
            return 0.0;
        };
        let resolved = self.resolve(query);
        self.score_slot(&resolved, slot)
    }

    /// BM25 of each of `claim_ids` (0 for ids not indexed or sharing no
    /// term), resolving the query terms once.
    pub fn score_many(&self, query: &QueryTerms, claim_ids: &[String]) -> Vec<f64> {
        let resolved = self.resolve(query);
        claim_ids
            .iter()
            .map(|id| match self.slots.get(id.as_str()) {
                Some(&slot) if !resolved.is_empty() => self.score_slot(&resolved, slot),
                _ => 0.0,
            })
            .collect()
    }

    fn score_slot(&self, resolved: &[ResolvedTerm<'_>], slot: u32) -> f64 {
        let Some(entry) = self.docs[slot as usize].as_ref() else {
            return 0.0;
        };
        let norm = self.length_norm(entry.len);
        let mut score = 0.0;
        for term in resolved {
            if let Ok(at) = term.postings.binary_search_by(|p| p.doc.cmp(&slot)) {
                score += Self::term_weight(term.idf, term.postings[at].tf, norm);
            }
        }
        score
    }

    /// The `top_n` documents with the highest BM25 for `query` among those
    /// sharing at least one query term and accepted by `filter`, best first;
    /// equal scores are ordered by claim id.
    pub fn search(
        &self,
        query: &QueryTerms,
        top_n: usize,
        filter: Option<&dyn Fn(&str) -> bool>,
    ) -> Vec<(String, f64)> {
        if top_n == 0 || self.slots.is_empty() {
            return Vec::new();
        }
        let resolved = self.resolve(query);
        if resolved.is_empty() {
            return Vec::new();
        }
        // Term-at-a-time accumulation into a dense array indexed by slot.
        // Lengths are normalised once per touched document.
        let mut acc: Vec<f64> = vec![0.0; self.docs.len()];
        let mut touched: Vec<u32> = Vec::new();
        let avg = self.avg_doc_len().max(f64::EPSILON);
        let mut norms: Vec<f64> = vec![-1.0; self.docs.len()];
        for term in &resolved {
            for posting in term.postings {
                let slot = posting.doc as usize;
                if norms[slot] < 0.0 {
                    let len = self.docs[slot].as_ref().map(|d| d.len).unwrap_or(0);
                    norms[slot] = BM25_K1 * (1.0 - BM25_B + BM25_B * (f64::from(len) / avg));
                    touched.push(posting.doc);
                }
                acc[slot] += Self::term_weight(term.idf, posting.tf, norms[slot]);
            }
        }
        let mut hits: Vec<(f64, &str)> = touched
            .into_iter()
            .filter_map(|slot| {
                let entry = self.docs[slot as usize].as_ref()?;
                let id: &str = &entry.claim_id;
                if filter.is_some_and(|accept| !accept(id)) {
                    return None;
                }
                Some((acc[slot as usize], id))
            })
            .collect();
        let order = |a: &(f64, &str), b: &(f64, &str)| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1));
        if hits.len() > top_n {
            hits.select_nth_unstable_by(top_n - 1, order);
            hits.truncate(top_n);
        }
        hits.sort_unstable_by(order);
        hits.into_iter()
            .map(|(score, id)| (id.to_string(), score))
            .collect()
    }

    /// Approximate heap bytes held by the index (postings, document table,
    /// claim-id interning, term strings).
    pub fn heap_bytes(&self) -> usize {
        let postings: usize = self
            .postings
            .iter()
            .map(|(term, list)| {
                term.len()
                    + std::mem::size_of::<Box<str>>()
                    + std::mem::size_of::<Vec<Posting>>()
                    + list.capacity() * std::mem::size_of::<Posting>()
                    + 8
            })
            .sum();
        let docs: usize = self.docs.capacity() * std::mem::size_of::<Option<DocEntry>>()
            + self
                .docs
                .iter()
                .flatten()
                .map(|d| d.claim_id.len())
                .sum::<usize>();
        let slots: usize = self
            .slots
            .keys()
            .map(|k| k.capacity() + std::mem::size_of::<String>() + 4 + 8)
            .sum();
        postings + docs + slots + self.free.capacity() * 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_word_list_is_sorted_for_binary_search() {
        let mut sorted = STOP_WORDS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted, STOP_WORDS);
    }

    #[test]
    fn analyzer_lowercases_strips_punctuation_and_stems() {
        assert_eq!(
            analyze("Company X ACQUIRED Company-Y, acquiring its rivals."),
            vec!["compani", "x", "acquir", "compani", "y", "acquir", "it", "rival"]
        );
        assert_eq!(analyze("The company's pumps"), vec!["the", "compani", "pump"]);
        assert_eq!(analyze("the company\u{2019}s"), analyze("the company's"));
        assert_eq!(analyze("   ...!!! ---  "), Vec::<String>::new());
    }

    #[test]
    fn analyzer_keeps_unicode_words_and_numbers() {
        assert_eq!(analyze("Café RÉSUMÉ naïve"), vec!["café", "résumé", "naïve"]);
        assert_eq!(analyze("ΩMEGA Straße"), vec!["ωmega", "straße"]);
        assert_eq!(analyze("Москва Moscow"), vec!["москва", "moscow"]);
        assert_eq!(analyze("revenue rose 3.5% in 2025"), vec!["revenu", "rose", "3.5", "in", "2025"]);
        // Emoji are not words.
        assert_eq!(analyze("emoji 😀 here"), vec!["emoji", "here"]);
    }

    #[test]
    fn analyzer_splits_cjk_without_dropping_it() {
        // Han characters have no UAX #29 word boundary rule: one term each.
        assert_eq!(analyze("日本語"), vec!["日", "本", "語"]);
        assert_eq!(analyze("東京で会議"), vec!["東", "京", "で", "会", "議"]);
        // Katakana runs and Hangul words stay together.
        assert_eq!(analyze("カタカナ 한국어"), vec!["カタカナ", "한국어"]);
        let mut index = TenantTextIndex::new();
        index.insert("c1", "café 日本語 résumé");
        index.insert("c2", "plain english text");
        let hits = index.search(&analyze_query("日本"), 10, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "c1");
    }

    #[test]
    fn long_terms_are_truncated() {
        let long = "a".repeat(200);
        let terms = analyze(&format!("x{long}1"));
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].chars().count(), MAX_TERM_CHARS);
    }

    #[test]
    fn query_drops_stop_words_unless_nothing_else_remains() {
        assert_eq!(analyze_query("What did the merger do?").terms, vec!["merger"]);
        assert_eq!(analyze_query("the who").terms, vec!["the", "who"]);
        assert_eq!(analyze_query("Pump pumps PUMPING").terms, vec!["pump"]);
        assert!(analyze_query("?!").is_empty());
    }

    /// Hand-computed example. Documents (after analysis):
    ///   d1 = [cat, sat, mat]           len 3
    ///   d2 = [cat, cat, dog]           len 3
    ///   d3 = [dog, bark, loud, night]  len 4
    /// N = 3, avgdl = 10 / 3.
    /// idf(cat) = ln(1 + (3 - 2 + 0.5) / (2 + 0.5)) = ln(1.6)
    /// idf(dog) = ln(1.6), idf(mat) = ln(1 + 2.5 / 1.5) = ln(8 / 3)
    /// norm(len 3) = 1.2 * (0.25 + 0.75 * 3 / (10 / 3)) = 1.11
    /// norm(len 4) = 1.2 * (0.25 + 0.75 * 4 / (10 / 3)) = 1.38
    /// Query "cat mat":
    ///   d1 = ln(1.6) * 2.2 / 2.11 + ln(8/3) * 2.2 / 2.11
    ///   d2 = ln(1.6) * 4.4 / 3.11
    ///   d3 = 0 (no shared term, not returned)
    #[test]
    fn bm25_matches_a_hand_computed_example() {
        let mut index = TenantTextIndex::new();
        index.insert("d1", "cat sat mat");
        index.insert("d2", "cat cat dog");
        index.insert("d3", "dog bark loud night");
        let query = analyze_query("cat mat");
        let d1 = 1.6f64.ln() * 2.2 / 2.11 + (8.0f64 / 3.0).ln() * 2.2 / 2.11;
        let d2 = 1.6f64.ln() * 4.4 / 3.11;
        assert!((index.score(&query, "d1") - d1).abs() < 1e-12);
        assert!((index.score(&query, "d2") - d2).abs() < 1e-12);
        assert_eq!(index.score(&query, "d3"), 0.0);
        let hits = index.search(&query, 10, None);
        let ids: Vec<&str> = hits.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["d1", "d2"]);
        assert!((hits[0].1 - d1).abs() < 1e-12);
        assert!((hits[1].1 - d2).abs() < 1e-12);
        let bound = (1.6f64.ln() + (8.0f64 / 3.0).ln()) * 2.2;
        assert!((index.max_score(&query) - bound).abs() < 1e-12);
        assert!(hits[0].1 < bound);
    }

    #[test]
    fn search_honours_top_n_filter_and_tie_order() {
        let mut index = TenantTextIndex::new();
        for id in ["c3", "c1", "c2", "c4"] {
            index.insert(id, "same words here");
        }
        let query = analyze_query("words");
        let ids = |hits: Vec<(String, f64)>| hits.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        assert_eq!(ids(index.search(&query, 2, None)), vec!["c1", "c2"]);
        let odd = |id: &str| id == "c3" || id == "c1";
        assert_eq!(ids(index.search(&query, 10, Some(&odd))), vec!["c1", "c3"]);
        assert!(index.search(&query, 0, None).is_empty());
        assert!(index.search(&analyze_query("absent"), 10, None).is_empty());
    }

    #[test]
    fn updates_and_deletes_change_postings_and_statistics() {
        let mut index = TenantTextIndex::new();
        index.insert("a", "reactor pump valve");
        index.insert("b", "reactor coolant");
        assert_eq!(index.doc_freq("reactor"), 2);
        assert!((index.avg_doc_len() - 2.5).abs() < 1e-12);

        assert!(index.remove("a", "reactor pump valve"));
        index.insert("a", "bridge toll");
        assert_eq!(index.len(), 2);
        assert_eq!(index.doc_freq("reactor"), 1);
        assert_eq!(index.doc_freq("pump"), 0);
        assert_eq!(index.doc_freq("bridg"), 1);
        assert!(index.search(&analyze_query("pump"), 10, None).is_empty());
        assert!((index.avg_doc_len() - 2.0).abs() < 1e-12);

        assert!(index.remove("b", "reactor coolant"));
        assert!(!index.remove("b", "reactor coolant"));
        assert_eq!(index.doc_freq("reactor"), 0);
        assert_eq!(index.term_count(), 2);
        assert!(index.search(&analyze_query("reactor"), 10, None).is_empty());

        // A slot freed by a delete is reused and postings stay sorted.
        index.insert("c", "toll road");
        index.insert("d", "toll booth");
        let hits = index.search(&analyze_query("toll"), 10, None);
        assert_eq!(hits.len(), 3);
        // Removing with a text that is not the indexed one still removes
        // every posting of the document.
        assert!(index.remove("c", "something else entirely"));
        assert_eq!(index.doc_freq("road"), 0);
        assert!(index.remove("a", "bridge toll") && index.remove("d", "toll booth"));
        assert!(index.is_empty());
        assert_eq!(index.term_count(), 0);
    }

    #[test]
    fn index_equals_a_fresh_build_after_churn() {
        let mut churned = TenantTextIndex::new();
        let mut texts: HashMap<String, String> = HashMap::new();
        let mut set = |index: &mut TenantTextIndex, id: String, text: String| {
            if let Some(old) = texts.get(&id) {
                index.remove(&id, old);
            }
            index.insert(&id, &text);
            texts.insert(id, text);
        };
        for i in 0..50 {
            set(&mut churned, format!("c{i}"), format!("word{} common text {}", i % 7, i % 3));
        }
        for i in (1..50).step_by(5) {
            set(&mut churned, format!("c{i}"), format!("updated word{} text", i % 4));
        }
        let mut live = texts.clone();
        for i in (0..50).step_by(3) {
            let id = format!("c{i}");
            let text = live.remove(&id).expect("indexed");
            assert!(churned.remove(&id, &text));
        }
        let mut ids: Vec<&String> = live.keys().collect();
        ids.sort();
        let mut fresh = TenantTextIndex::new();
        for id in ids {
            fresh.insert(id, &live[id]);
        }
        assert_eq!(churned.len(), fresh.len());
        assert_eq!(churned.term_count(), fresh.term_count());
        for query in ["word1 text", "common", "updated word3", "text 2"] {
            let q = analyze_query(query);
            assert_eq!(churned.search(&q, 100, None), fresh.search(&q, 100, None), "{query}");
        }
    }
}
