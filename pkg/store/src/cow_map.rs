//! A hash map from `String` keys whose clone is cheap and whose writes copy
//! only a small part of the map.
//!
//! The entries are spread over [`SHARDS`] shards by key hash, and each shard
//! is an `Arc<HashMap>`. Cloning the map clones the shard pointers (a few
//! microseconds, independent of the number of entries). A write to a shard
//! that is shared with a clone copies that one shard first
//! (`Arc::make_mut`), so the first write per shard after a clone costs
//! about `len / SHARDS` entry copies and every later write to that shard
//! costs nothing extra.
//!
//! The store keeps the maps a snapshot is written from (claims, evidence,
//! edges, vectors, batch metadata) in this type, so a checkpoint can take a
//! consistent copy of the state under the store lock in microseconds and
//! write it out without the lock (see `InMemoryStore::begin_checkpoint`).

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

/// Number of shards. With a million entries a shard holds about 250, so the
/// copy a write may trigger while a checkpoint holds a clone stays well
/// below a millisecond.
pub(crate) const SHARDS: usize = 4096;

pub(crate) struct CowMap<V> {
    /// `None` for a shard that never held an entry (no allocation).
    shards: Box<[Option<Arc<HashMap<String, V>>>]>,
    len: usize,
    hasher: RandomState,
}

impl<V> Default for CowMap<V> {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| None).collect(),
            len: 0,
            hasher: RandomState::new(),
        }
    }
}

impl<V> Clone for CowMap<V> {
    /// Shares every shard with the original; O(`SHARDS`).
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            len: self.len,
            hasher: self.hasher.clone(),
        }
    }
}

impl<V: std::fmt::Debug> std::fmt::Debug for CowMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<V> CowMap<V> {
    fn shard_of<Q>(&self, key: &Q) -> usize
    where
        Q: Hash + ?Sized,
    {
        (self.hasher.hash_one(key) as usize) % SHARDS
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shards[self.shard_of(key)].as_ref()?.get(key)
    }

    pub(crate) fn contains_key<Q>(&self, key: &Q) -> bool
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.shards.iter().flatten().flat_map(|shard| shard.iter())
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &String> {
        self.iter().map(|(k, _)| k)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }
}

impl<V: Clone> CowMap<V> {
    /// The shard for `index`, unshared (copied first if a clone holds it).
    fn shard_mut(&mut self, index: usize) -> &mut HashMap<String, V> {
        Arc::make_mut(self.shards[index].get_or_insert_with(Default::default))
    }

    pub(crate) fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self.shard_of(key);
        // Do not copy a shared shard for a key it does not hold.
        if !self.shards[index].as_ref()?.contains_key(key) {
            return None;
        }
        self.shard_mut(index).get_mut(key)
    }

    pub(crate) fn insert(&mut self, key: String, value: V) -> Option<V> {
        let index = self.shard_of(key.as_str());
        let previous = self.shard_mut(index).insert(key, value);
        if previous.is_none() {
            self.len += 1;
        }
        previous
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self.shard_of(key);
        if !self.shards[index].as_ref()?.contains_key(key) {
            return None;
        }
        let removed = self.shard_mut(index).remove(key);
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }

    /// `entry(key).or_default()`.
    pub(crate) fn get_or_default_mut(&mut self, key: String) -> &mut V
    where
        V: Default,
    {
        let index = self.shard_of(key.as_str());
        let exists = self.shards[index]
            .as_ref()
            .is_some_and(|shard| shard.contains_key(key.as_str()));
        if !exists {
            self.len += 1;
        }
        self.shard_mut(index).entry(key).or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn behaves_like_a_hash_map() {
        let mut map: CowMap<u32> = CowMap::default();
        assert_eq!(map.len(), 0);
        assert_eq!(map.insert("a".into(), 1), None);
        assert_eq!(map.insert("b".into(), 2), None);
        assert_eq!(map.insert("a".into(), 3), Some(1));
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a"), Some(&3));
        assert!(map.contains_key("b"));
        assert!(!map.contains_key("c"));
        *map.get_mut("b").unwrap() += 10;
        assert_eq!(map.get("b"), Some(&12));
        assert!(map.get_mut("c").is_none());
        *map.get_or_default_mut("c".into()) += 5;
        *map.get_or_default_mut("c".into()) += 5;
        assert_eq!(map.get("c"), Some(&10));
        assert_eq!(map.len(), 3);
        assert_eq!(map.remove("a"), Some(3));
        assert_eq!(map.remove("a"), None);
        assert_eq!(map.len(), 2);
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        assert_eq!(keys, vec!["b", "c"]);
        assert_eq!(map.values().sum::<u32>(), 22);
    }

    #[test]
    fn a_clone_is_unaffected_by_later_writes_and_shares_untouched_shards() {
        let mut map: CowMap<Vec<u8>> = CowMap::default();
        for i in 0..10_000 {
            map.insert(format!("k{i}"), vec![1]);
        }
        let frozen = map.clone();
        map.insert("k1".into(), vec![2]);
        map.get_mut("k2").unwrap().push(9);
        map.remove("k3");
        *map.get_or_default_mut("new".into()) = vec![7];
        assert_eq!(frozen.get("k1"), Some(&vec![1]));
        assert_eq!(frozen.get("k2"), Some(&vec![1]));
        assert_eq!(frozen.get("k3"), Some(&vec![1]));
        assert!(frozen.get("new").is_none());
        assert_eq!(frozen.len(), 10_000);
        assert_eq!(frozen.iter().count(), 10_000);
        assert_eq!(map.len(), 10_000);
        assert_eq!(map.get("k1"), Some(&vec![2]));
        assert_eq!(map.get("k2"), Some(&vec![1, 9]));
        // At most the four touched shards were copied.
        let shared = map
            .shards
            .iter()
            .zip(frozen.shards.iter())
            .filter(|(a, b)| match (a, b) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            })
            .count();
        let populated = frozen.shards.iter().flatten().count();
        assert!(shared + 4 >= populated, "shared={shared} populated={populated}");
    }
}
