use slice::Bytes;
use std::cmp::Ordering;
use byteorder::{ByteOrder, LittleEndian};

pub trait Comparator {
    fn compare(&self, a: &Bytes, b: &Bytes) -> Ordering;
}

pub struct InternalKeyComparator;

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &Bytes, b: &Bytes) -> Ordering {
        match extract_user_key(a).cmp(extract_user_key(b)) {
            Ordering::Equal => {
                let a_s = a.len();
                let b_s = b.len();
                LittleEndian::read_u64(&a[b_s - 8..b_s])
                    .cmp(&LittleEndian::read_u64(&b[a_s - 8..a_s]))
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
    use ikey::InternalKey;
    use super::*;

    #[test]
    fn internal_key_comparator() {
        let v0 = InternalKey::new(&Bytes::from("aaa"), 1).memtable_key();
        let v1 = InternalKey::new(&Bytes::from("aaa"), 1).memtable_key();
        let v2 = InternalKey::new(&Bytes::from("aaa"), 2).memtable_key();
        let v3 = InternalKey::new(&Bytes::from("aab"), 1).memtable_key();

        assert_eq!(InternalKeyComparator.compare(&v0, &v1), Ordering::Equal);
        assert_eq!(InternalKeyComparator.compare(&v0, &v2), Ordering::Less);
        assert_eq!(InternalKeyComparator.compare(&v0, &v3), Ordering::Less);
    }
}
