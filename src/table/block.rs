use std::io;
use slice::{ByteRead, Bytes, U32_BYTE_SIZE};
use super::format;

#[derive(Debug)]
pub struct Block {
    inner: Bytes,
    size: usize,
    restart_offset: usize,
}

pub fn read<T: io::Read + io::Seek>(reader: &mut T, bh_value: &Bytes) -> Block {
    let bh = format::BlockHandle::decode_from(&mut bh_value.clone());
    let block = format::read_block(reader, &bh).expect("block ga!!!!");
    block
}

impl Block {
    pub fn new(inner: Bytes) -> Self {
        let size = inner.len();
        let mut b = Block {
            size: size,
            inner: inner,
            restart_offset: 0,
        };

        b.restart_offset = if size < U32_BYTE_SIZE {
            panic!("TODO: fix")
        } else {
            size - ((b.restart_count() + 1) * U32_BYTE_SIZE)
        };

        debug!("restart_offset={:?}, size={:?}", b.restart_offset, b.size);
        b
    }

    pub fn restart_count(&self) -> usize {
        self.inner.get_u32(self.size - U32_BYTE_SIZE) as usize
    }

    pub fn iter(&self) -> BlockItertor {
        BlockItertor::new(
            self.inner.clone(),
            self.restart_offset,
            self.restart_count(),
        )
    }
}

pub struct BlockItertor {
    inner: Bytes,
    restart_offset: usize,
    restart_num: usize,
    key: Option<Bytes>,
    value: Option<Bytes>,
}

impl BlockItertor {
    pub fn new(inner: Bytes, restart_offset: usize, restart_num: usize) -> Self {
        debug!(
            "inner={:?}, restart_offset={:?}, restart_num={:?}",
            inner, restart_offset, restart_num
        );
        Self {
            inner,
            restart_offset,
            restart_num,
            key: None,
            value: None,
        }
    }

    fn restart_point(&self, idx: usize) -> Option<usize> {
        Some(self.inner
            .get_u32(idx * U32_BYTE_SIZE + self.restart_offset) as usize)
    }

    pub fn seek(&mut self, key: &Bytes) -> Option<Bytes> {
        let mut left = 0;
        let mut right = self.restart_num - 1;

        while left < right {
            let mid = (left + right) / 2;
            let rpoint = self.restart_point(mid).expect("Invalid restart point");
            let (shared, not_shared, value_length, offset) = decode_block(&self.inner, rpoint);
            let index_key = self.inner.gets(offset, not_shared);

            debug!(
                "shared={:?},not_shared={:?},value_length={:?},key={:?}",
                shared, not_shared, value_length, index_key
            );

            if &index_key < key {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        let mut offset = self.restart_point(left).expect("invalid restart pont");
        while let Some(next_offset) = self.parse_key(offset) {
            if let Some(k) = self.key.as_ref() {
                if k >= key {
                    break;
                }
                offset = next_offset;
            } else {
                break;
            }
        }

        self.value.clone()
    }

    pub fn parse_key(&mut self, offset: usize) -> Option<usize> {
        let limit = self.restart_offset;
        if limit <= offset {
            return None; // End of data
        }

        let (shared, not_shared, value_length, next_offset) = decode_block(&self.inner, offset);
        let index_key = self.inner.gets(next_offset, not_shared);
        let k = if let Some(last_key) = self.key.as_ref().as_mut() {
            let mut k = last_key.clone();
            k.truncate(shared as usize);
            k.extend(&index_key);
            k
        } else {
            Bytes::from(index_key)
        };

        let v = self.inner.gets(next_offset + not_shared, value_length);

        debug!("key={:?}, value={:?}", k, v);
        self.key = Some(k);
        self.value = Some(v);
        Some(next_offset + not_shared + value_length)
    }
}

// shared, not_shared, value.len()
fn decode_block(slice: &Bytes, offset: usize) -> (usize, usize, usize, usize) {
    let shared = slice.get_u32(offset);
    let not_shared = slice.get_u32(offset + U32_BYTE_SIZE);
    let value_length = slice.get_u32(offset + U32_BYTE_SIZE * 2);

    (
        shared as usize,
        not_shared as usize,
        value_length as usize,
        offset + U32_BYTE_SIZE * 3,
    )
}
