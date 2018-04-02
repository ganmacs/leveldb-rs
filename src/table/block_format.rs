use slice::Slice;
// TABLE_MAGIC_NUMBER was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.

const TABLE_MAGIC_NUMBER: i64 = 0xdb4775248b80fb57;

pub struct BlockHandle {
    pub size: Option<u64>,
    pub offset: Option<u64>,
}

impl BlockHandle {
    pub fn new() -> Self {
        Self {
            size: None,
            offset: None,
        }
    }

    pub fn from(size: u64, offset: u64) -> Self {
        Self {
            size: Some(size),
            offset: Some(offset),
        }
    }

    pub fn decode_from(input: &mut Slice) -> Self {
        let size = input.read_u64();
        let offset = input.read_u64();
        Self { size, offset }
    }

    pub fn set_size(&mut self, v: u64) {
        self.size = Some(v)
    }

    pub fn set_offset(&mut self, v: u64) {
        self.offset = Some(v)
    }

    pub fn encode(&self) -> Slice {
        let mut slice = Slice::with_capacity(16);
        // TODO: put num as varint64 to reduce size
        let size = self.size.expect("size must be set");
        slice.put_u64(size);
        let offset = self.offset.expect("offset must be set");
        slice.put_u64(offset);
        slice
    }
}

pub struct Footer {
    index_block_hanel: BlockHandle,
    metaindex_block_hanel: BlockHandle,
}

impl Footer {
    pub fn new(ibh: BlockHandle, mbh: BlockHandle) -> Self {
        Self {
            index_block_hanel: ibh,
            metaindex_block_hanel: mbh,
        }
    }

    pub fn encode(&self) -> Slice {
        let mut slice = Slice::with_capacity(2 * 8 + 8);
        slice.put(&self.index_block_hanel.encode());
        slice.put(&self.metaindex_block_hanel.encode());
        slice.put_i64(TABLE_MAGIC_NUMBER);
        slice
    }
}

#[cfg(test)]
mod tests {
    use super::BlockHandle;

    #[test]
    fn block_handle_test() {
        let mut bh = BlockHandle::new();
        bh.set_size(10);
        bh.set_offset(10);
        let v: Vec<u8> = vec![10, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(bh.encode().as_ref(), v.as_ref() as &[u8]);

        let mut bh = BlockHandle::from(1111111111, 200000000000);
        let bh2 = BlockHandle::decode_from(&mut bh.encode());

        assert_eq!(bh2.size, Some(1111111111));
        assert_eq!(bh2.offset, Some(200000000000));
    }
}
