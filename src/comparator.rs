use byteorder::{ByteOrder, LittleEndian};
use std::cmp::Ordering;

use slice::Bytes;

pub trait Comparator {
    fn compare(&self, a: &Bytes, b: &Bytes) -> Ordering;
}

pub struct InternalKeyComparator;

// user key is asc. seq is desc.
impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &Bytes, b: &Bytes) -> Ordering {
        match extract_user_key(a).cmp(extract_user_key(b)) {
            Ordering::Equal => {
                let a_s = a.len();
                let b_s = b.len();
                LittleEndian::read_u64(&b[b_s - 8..b_s])
                    .cmp(&LittleEndian::read_u64(&a[a_s - 8..a_s]))
            }
            t => t,
        }
    }
}

fn extract_user_key<'a>(key: &'a Bytes) -> &'a [u8] {
    let size = key.len();
    &key[0..size - 8]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ikey::InternalKey;

    #[test]
    fn internal_key_comparator() {
        let v0 = InternalKey::new(&Bytes::from("key1"), 10).memtable_key();

        let v10 = InternalKey::new(&Bytes::from("key1"), 10).memtable_key();
        let v11 = InternalKey::new(&Bytes::from("key1"), 1).memtable_key();
        let v12 = InternalKey::new(&Bytes::from("key1"), 11).memtable_key();
        assert_eq!(InternalKeyComparator.compare(&v0, &v10), Ordering::Equal);
        assert_eq!(InternalKeyComparator.compare(&v0, &v11), Ordering::Less);
        assert_eq!(InternalKeyComparator.compare(&v0, &v12), Ordering::Greater);

        let v20 = InternalKey::new(&Bytes::from("key0"), 10).memtable_key();
        let v21 = InternalKey::new(&Bytes::from("key00"), 10).memtable_key();
        let v22 = InternalKey::new(&Bytes::from("key10"), 10).memtable_key();
        assert_eq!(InternalKeyComparator.compare(&v0, &v20), Ordering::Greater);
        assert_eq!(InternalKeyComparator.compare(&v0, &v21), Ordering::Less);
        assert_eq!(InternalKeyComparator.compare(&v0, &v22), Ordering::Less);
    }
}
