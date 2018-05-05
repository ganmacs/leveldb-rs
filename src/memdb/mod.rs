extern crate bytes;
extern crate rand;

use std::iter::Iterator;
mod skiplist;
use ikey::{InternalKey, KeyKind};
use slice::{ByteRead, ByteWrite, Bytes, U64_BYTE_SIZE};
use comparator::InternalKeyComparator;

pub struct MemDB {
    inner: skiplist::SkipList<InternalKeyComparator>,
}

impl MemDB {
    pub fn new() -> Self {
        MemDB {
            inner: skiplist::SkipList::new(InternalKeyComparator),
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
                (KeyKind::Value, true) => Some(get_length_prefixed_key(&mut v)),
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
    inner: skiplist::SkipListIterator<'a, InternalKeyComparator>,
}

impl<'a> Iterator for MemDBIterator<'a> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|mut v| {
            let k = get_length_prefixed_key(&mut v);
            let v = get_length_prefixed_key(&mut v);
            (k, v)
        })
    }
}

fn get_length_prefixed_key(v: &mut Bytes) -> Bytes {
    let size = v.read_u32() as usize;
    v.read(size)
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
    fn skiplist_iter() {
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
            let key_bytes = v.0.as_bytes();
            let k = InternalKey::new(key_bytes, 1);
            db.add(&k, &v.1);
        }

        let mut it = db.iter();
        for v in hash.into_iter() {
            let expected_key = Bytes::from(format!("{}\x02\0\0\0\0\0\0\0", v.0).as_bytes());
            assert_eq!(it.next().unwrap(), (expected_key, v.1));
        }
    }
}
