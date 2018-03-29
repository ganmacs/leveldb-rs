use bytes::{BufMut, ByteOrder, Bytes, BytesMut, LittleEndian};
use slice::SliceMut;
// TABLE_MAGIC_NUMBER was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.

const TABLE_MAGIC_NUMBER: i64 = 0xdb4775248b80fb57;

pub struct BlockHandle {
    pub size: Option<u64>, // varint64?
    pub offset: Option<u64>,
}

const U64_BYTE_SIZE: usize = 8;

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

    pub fn decode_from(input: Bytes) -> Self {
        let mut i_offset = 0;

        let size = Some({
            let buf = input.slice(i_offset, U64_BYTE_SIZE);
            i_offset += U64_BYTE_SIZE;
            LittleEndian::read_u64(&buf)
        });

        let offset = Some({
            let buf = input.slice(i_offset, U64_BYTE_SIZE);
            i_offset += U64_BYTE_SIZE;
            LittleEndian::read_u64(&buf)
        });

        Self { size, offset }
    }

    pub fn set_size(&mut self, v: u64) {
        self.size = Some(v)
    }

    pub fn set_offset(&mut self, v: u64) {
        self.offset = Some(v)
    }

    pub fn encode(&self) -> SliceMut {
        let mut slice = SliceMut::with_capacity(10);
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

    pub fn encode(&self) -> SliceMut {
        let mut slice = SliceMut::with_capacity(2 * 10 + 8);
        slice.put(&self.index_block_hanel.encode());
        slice.put(&self.metaindex_block_hanel.encode());
        slice.resize(2 * 10 + 8);
        slice.put_i64(TABLE_MAGIC_NUMBER);
        slice
    }
}
