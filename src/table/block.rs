use std::io;
use slice::{ByteRead, Bytes, U32_BYTE_SIZE};
use super::format;
use random_access_file::RandomAccessFile;

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

pub fn read2<T: RandomAccessFile>(inner: &T, bh_value: &Bytes) -> Block {
    let bh = format::BlockHandle::decode_from(&mut bh_value.clone());
    let block = format::read_block2(inner, &bh).expect("block ga!!!!");
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

        debug!(
            "new block restart_offset={:?}, size={:?}",
            b.restart_offset, b.size
        );
        b
    }

    pub fn restart_count(&self) -> usize {
        self.inner.get_u32(self.size - U32_BYTE_SIZE) as usize
    }

    pub fn iter(&self) -> BlockIterator {
        BlockIterator::new(
            self.inner.clone(),
            self.restart_offset,
            self.restart_count(),
        )
    }
}

pub struct BlockIterator {
    inner: Bytes,
    restart_offset: usize,
    restart_num: usize,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    current: usize,
}

impl BlockIterator {
    pub fn new(inner: Bytes, restart_offset: usize, restart_num: usize) -> Self {
        debug!(
            "new blockiterator restart_offset={:?}, restart_num={:?}",
            restart_offset, restart_num
        );
        Self {
            inner,
            restart_offset,
            restart_num,
            key: None,
            value: None,
            current: 0,
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
            let mid = (left + right + 1) / 2;
            let rpoint = self.restart_point(mid).expect("Invalid restart point");
            let (shared, not_shared, value_length, offset) = decode_block(&self.inner, rpoint);
            let index_key = self.inner.gets(offset, not_shared);

            debug!(
                "shared={:?},not_shared={:?},value_length={:?},key={:?}",
                shared, not_shared, value_length, index_key
            );

            // MUST use comparator
            if &index_key < key {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        let p = self.restart_point(left).expect("invalid restart pont");
        self.set_seek_point(p);
        while self.parse_key() {
            if let Some(k) = self.key.as_ref() {
                if k >= key {
                    break;
                }
            } else {
                break;
            }
        }

        self.value.clone()
    }

    pub fn parse_key(&mut self) -> bool {
        if self.restart_offset <= self.current {
            return false; // End of data
        }

        let (shared, not_shared, value_length, next_offset) =
            decode_block(&self.inner, self.current);
        let index_key = self.inner.gets(next_offset, not_shared);

        let k = if let Some(last_key) = self.key.as_ref() {
            let mut k = last_key.clone();
            k.truncate(shared as usize);
            k.extend(&index_key);
            k
        } else {
            Bytes::from(index_key)
        };

        let v = self.inner.gets(next_offset + not_shared, value_length);

        self.key = Some(k);
        self.value = Some(v);
        self.set_seek_point(next_offset + not_shared + value_length);
        true
    }

    fn set_seek_point(&mut self, p: usize) {
        self.current = p
    }
}

impl Iterator for BlockIterator {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.parse_key() {
            match (self.key.as_ref(), self.value.as_ref()) {
                (Some(k), Some(v)) => Some((k.clone(), v.clone())),
                _ => None,
            }
        } else {
            None
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::block_builder::BlockBuilder;

    fn create_seed_helper(size: usize) -> Vec<(Bytes, Bytes)> {
        (0..size)
            .into_iter()
            .map(|v| {
                (
                    Bytes::from(format!("key{:02?}", v).as_bytes()),
                    Bytes::from(format!("value{:02?}", v).as_bytes()),
                )
            })
            .collect()
    }

    #[test]
    fn test_block_iterator() {
        let mut bb = BlockBuilder::new();
        let dic = create_seed_helper(30);

        for v in &dic {
            bb.add(&v.0, &v.1);
        }

        let block = Block::new(bb.build()).iter();
        for (b, d) in block.zip(&dic) {
            assert_eq!(b.0, d.0);
            assert_eq!(b.1, d.1);
        }
    }

    #[test]
    fn test_block_iterator_seek() {
        let mut bb = BlockBuilder::new();
        let dic = create_seed_helper(5);

        for v in &dic {
            bb.add(&v.0, &v.1);
        }

        let mut block = Block::new(bb.build()).iter();
        for d in &dic {
            println!("{:?}", d.0);
            println!("{:?}", d.1);
            println!("{:?} ", block.seek(&d.0));
            assert_eq!(block.seek(&d.0).as_ref(), Some(&d.1));
        }

        // restart_size is 2
        let mut bb = BlockBuilder::new();
        let dic = create_seed_helper(30);

        for v in &dic {
            bb.add(&v.0, &v.1);
        }

        let mut block = Block::new(bb.build()).iter();
        for d in &dic {
            println!("{:?}", d.0);
            println!("{:?}", d.1);
            println!("{:?} ", block.seek(&d.0));
            assert_eq!(block.seek(&d.0).as_ref(), Some(&d.1));
        }
    }
}
