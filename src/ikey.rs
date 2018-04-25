use std::cmp;
use slice::{ByteRead, ByteWrite, Bytes, BytesMut};

const UKEY_LENGTH: usize = 4;
const SEQ_LENGTH: usize = 8;
const UKEY_INDEX: usize = 4;
const SEQ_MAX_NUMBER: usize = (1 << (64 - 8));

pub enum KeyKind {
    Value,
    Delete,
}

impl From<u8> for KeyKind {
    fn from(v: u8) -> Self {
        match v {
            0 => KeyKind::Value,
            1 => KeyKind::Delete,
            _ => unreachable!(),
        }
    }
}

// key = | value_length(4 bytes) | value (n bytes) | seq + kind (8 bytes: seq(63 bits), kind(1 bit))
#[derive(Clone, Eq, Ord, Debug)]
pub struct InternalKey {
    inner: Bytes,
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.inner.cmp(&other.inner))
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl PartialOrd<Bytes> for InternalKey {
    fn partial_cmp(&self, other: &Bytes) -> Option<cmp::Ordering> {
        self.inner.partial_cmp(other)
    }
}

impl PartialEq<Bytes> for InternalKey {
    fn eq(&self, other: &Bytes) -> bool {
        self.inner == other
    }
}

impl PartialOrd<InternalKey> for Bytes {
    fn partial_cmp(&self, other: &InternalKey) -> Option<cmp::Ordering> {
        self.partial_cmp(&other.inner)
    }
}

impl PartialEq<InternalKey> for Bytes {
    fn eq(&self, other: &InternalKey) -> bool {
        self == &other.inner
    }
}

impl AsRef<[u8]> for InternalKey {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

fn make_key(user_key: &[u8], seq: u64, kind: KeyKind) -> Bytes {
    let size = user_key.len();
    let mut bytes = BytesMut::with_capacity(UKEY_LENGTH + size + SEQ_LENGTH);
    bytes.write_u32(size as u32);
    bytes.write_slice(user_key);
    bytes.write_u64(seq << 1 | kind as u64);
    debug!("key is {:?}", bytes);
    bytes.freeze()
}

impl InternalKey {
    pub fn from(inner: Bytes) -> Self {
        InternalKey { inner }
    }

    pub fn new_with_kind(user_key: &[u8], seq: u64, kind: KeyKind) -> Self {
        InternalKey {
            inner: make_key(user_key, seq, kind),
        }
    }

    pub fn new(user_key: &[u8], seq: u64) -> Self {
        InternalKey {
            inner: make_key(user_key, seq, KeyKind::Value),
        }
    }

    pub fn new_delete_key(user_key: &[u8], seq: u64) -> Self {
        InternalKey {
            inner: make_key(user_key, seq, KeyKind::Delete),
        }
    }

    pub fn inner(&self) -> Bytes {
        self.inner.clone()
    }

    pub fn user_key(&self) -> Bytes {
        self.inner.gets(UKEY_LENGTH, self.key_size())
    }

    pub fn memtable_key(&self) -> Bytes {
        self.inner
            .gets(0, self.key_size() + UKEY_LENGTH + SEQ_LENGTH)
    }

    pub fn seq_number(&self) -> usize {
        (self.compacted_seq_kind() >> 1) as usize
    }

    pub fn kind(&self) -> KeyKind {
        match self.compacted_seq_kind() & 8 {
            0 => KeyKind::Value,
            1 => KeyKind::Delete,
            _ => unreachable!(),
        }
    }

    fn compacted_seq_kind(&self) -> u64 {
        self.inner.get_u64(self.key_size() + UKEY_INDEX)
    }

    fn key_size(&self) -> usize {
        self.inner.get_u32(0) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_key() {
        let ikey = InternalKey::new(&Bytes::from("hoge"), 2);
        assert_eq!(ikey.user_key(), "hoge");
        assert_eq!(
            ikey.memtable_key(),
            Bytes::from("\x04\0\0\0hoge\x04\0\0\0\0\0\0\0")
        );
        assert_eq!(ikey.seq_number(), 2);
    }
}
