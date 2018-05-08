extern crate bytes;
extern crate rand;

mod skiplist;

use std::iter::Iterator;
use ikey::{InternalKey, KeyKind};
use slice::{ByteRead, ByteWrite, Bytes, U32_BYTE_SIZE, U64_BYTE_SIZE};
use comparator::{Comparator, InternalKeyComparator};

pub struct MemDB {
    inner: skiplist::SkipList<KeyComparator>,
}

impl MemDB {
    pub fn new() -> Self {
        MemDB {
            inner: skiplist::SkipList::new(KeyComparator(InternalKeyComparator)),
        }
    }

    pub fn empty(&self) -> bool {
        self.inner.empty()
    }

    pub fn approximately_size(&self) -> usize {
        self.inner.data_usage()
    }

    pub fn get(&self, key: &InternalKey) -> Option<Bytes> {
        let k = key.memtable_key();
        debug!("Get {:?} from memdb", k);
        self.inner.seek(&k).and_then(|mut v| {
            let key_size = v.read_u32();
            let ikey = v.read(key_size as usize - U64_BYTE_SIZE);
            let seq_kind = v.read_u64();
            let kind = KeyKind::from((seq_kind & 1) as u8);

            match (kind, key.user_key() == ikey) {
                (KeyKind::Value, true) => Some(get_length_prefixed_key(&v)),
                _ => None,
            }
        })
    }

    pub fn add(&mut self, ikey: &InternalKey, value: &Bytes) {
        let mut v = ikey.memtable_key()
            .try_mut()
            .expect("can't convert bytes to mutable bytes");
        v.write_u32(value.len() as u32);
        v.write(value);
        debug!("Set {:?} to memdb", v);
        self.inner.insert(v.freeze())
    }

    pub fn iter<'a>(&'a self) -> MemDBIterator<'a> {
        MemDBIterator {
            inner: self.inner.iter(),
        }
    }
}

pub struct MemDBIterator<'a> {
    inner: skiplist::SkipListIterator<'a, KeyComparator>,
}

impl<'a> Iterator for MemDBIterator<'a> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|mut v| {
            // To get length of key
            let size = v.get_u32(0) as usize;
            let k = v.read(size + U32_BYTE_SIZE);
            let v = get_length_prefixed_key(&v);
            (k, v)
        })
    }
}

fn get_length_prefixed_key(v: &Bytes) -> Bytes {
    let size = v.get_u32(0) as usize;
    v.gets(U32_BYTE_SIZE, size)
}

pub struct KeyComparator(InternalKeyComparator);

use std::cmp::Ordering;
impl Comparator for KeyComparator {
    fn compare(&self, a: &Bytes, b: &Bytes) -> Ordering {
        self.0
            .compare(&get_length_prefixed_key(&a), &get_length_prefixed_key(&b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memdb() {
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
            let key_bytes = v.0.as_bytes();
            let k = InternalKey::new(key_bytes, 1);
            db.add(&k, &v.1);
            assert_eq!(db.get(&InternalKey::new(key_bytes, 0)).unwrap(), v.1);
        }

        assert_eq!(db.get(&InternalKey::new(b"notfound", 0)), None);
    }

    #[test]
    fn memdb_seqeunce() {
        let mut db = MemDB::new();
        let key = "key1".as_bytes();
        let value = Bytes::from("value1");

        db.add(&InternalKey::new(key, 10), &value);
        assert_eq!(db.get(&InternalKey::new(key, 9)), Some(value.clone()));
        assert_eq!(db.get(&InternalKey::new(key, 10)), Some(value));
        assert_eq!(db.get(&InternalKey::new(key, 11)), None);
    }

    #[test]
    fn memdb_iter() {
        let mut db = MemDB::new();

        let hash: Vec<(InternalKey, Bytes)> = vec![
            (InternalKey::new("key".as_bytes(), 1), Bytes::from("value")),
            (
                InternalKey::new("key1".as_bytes(), 1),
                Bytes::from("value1"),
            ),
            (
                InternalKey::new("key2".as_bytes(), 1),
                Bytes::from("value2"),
            ),
            (
                InternalKey::new("key3".as_bytes(), 1),
                Bytes::from("value3"),
            ),
            (
                InternalKey::new("key4".as_bytes(), 1),
                Bytes::from("value4"),
            ),
            (
                InternalKey::new("key5".as_bytes(), 1),
                Bytes::from("value5"),
            ),
            (
                InternalKey::new("key6".as_bytes(), 1),
                Bytes::from("value___6"),
            ),
            (
                InternalKey::new("key77".as_bytes(), 1),
                Bytes::from("value   7"),
            ),
        ];
        for v in &hash.clone() {
            db.add(&v.0, &v.1);
        }

        let mut it = db.iter();
        for v in hash.into_iter() {
            assert_eq!(it.next().unwrap(), (v.0.inner(), v.1));
        }
    }

    #[test]
    fn memdb_iter_is_desc_order() {
        let mut db = MemDB::new();
        let hash: Vec<(InternalKey, Bytes)> = vec![
            (InternalKey::new("key01".as_bytes(), 1), Bytes::from("v")),
            (InternalKey::new("key00".as_bytes(), 2), Bytes::from("v")),
            (InternalKey::new("key00".as_bytes(), 1), Bytes::from("v")),
        ];

        for v in &hash.clone() {
            db.add(&v.0, &v.1);
        }

        let mut it = db.iter();
        assert_eq!(it.next().unwrap().0, hash[2].0);
        assert_eq!(it.next().unwrap().0, hash[1].0);
        assert_eq!(it.next().unwrap().0, hash[0].0);
    }
}
