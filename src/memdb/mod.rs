extern crate bytes;
extern crate rand;

use std::iter::{IntoIterator, Iterator};

mod skiplist;
use ikey::{InternalKey, KeyKind};
use slice::Bytes;

const MAX_HEIGHT: usize = 12;

pub struct MemDB {
    inner: skiplist::SkipList,
}

impl MemDB {
    pub fn new() -> Self {
        MemDB {
            inner: skiplist::SkipList::new(),
        }
    }

    pub fn empty(&self) -> bool {
        self.inner.empty()
    }

    pub fn get(&self, key: &InternalKey) -> Option<Bytes> {
        let k = key.memtable_key();
        debug!("Get {:?} from memdb", k);
        self.inner.get(&k)
    }

    pub fn add(&mut self, ikey: &InternalKey, value: &Bytes) {
        let k = ikey.memtable_key();
        debug!("Set {:?}=>{:?} to memdb", k, value);
        self.inner.insert(&k, &value)
    }
}

impl IntoIterator for MemDB {
    type Item = (Bytes, Bytes);
    type IntoIter = MemDBIterator;

    fn into_iter(self) -> Self::IntoIter {
        MemDBIterator {
            inner: self.inner.into_iter(),
        }
    }
}

pub struct MemDBIterator {
    inner: skiplist::SkipListIterator,
}

impl Iterator for MemDBIterator {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skiplist() {
        let mut db = MemDB::new();

        let hash = vec![
            ("key", Bytes::from("value")),
            ("key1", Bytes::from("value1")),
            ("key2", Bytes::from("value2")),
            ("key3", Bytes::from("value3")),
            ("key4", Bytes::from("value4")),
            ("key5", Bytes::from("value5")),
            ("key6", Bytes::from("value___6")),
            ("key77", Bytes::from("value   7")),
        ];

        for v in hash {
            db.add(0, KeyKind::Value, &Bytes::from(v.0), &v.1);
            assert_eq!(db.get(&InternalKey::new(&v.0, 0)).unwrap(), v.1);
        }

        assert_eq!(db.get(&InternalKey::new("notfound", 0)), None);
    }

    #[test]
    fn test_skiplist_iter() {
        let mut db = MemDB::new();

        let hash: Vec<(&str, Bytes)> = vec![
            ("key", Bytes::from("value")),
            ("key1", Bytes::from("value1")),
            ("key2", Bytes::from("value2")),
            ("key3", Bytes::from("value3")),
            ("key4", Bytes::from("value4")),
            ("key5", Bytes::from("value5")),
            ("key6", Bytes::from("value___6")),
            ("key77", Bytes::from("value   7")),
        ];

        for v in &hash.clone() {
            db.add(0, KeyKind::Value, &Bytes::from(v.0), &v.1);
        }

        let mut it = db.into_iter();
        for v in hash.into_iter() {
            assert_eq!(it.next().unwrap(), (Bytes::from(v.0), v.1));
        }
    }
}
