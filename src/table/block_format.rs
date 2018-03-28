use bytes::{Bytes, BytesMut, LittleEndian, BufMut, ByteOrder};

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

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(10);
        let size = self.size.expect("size must be set");
        b.put_u64::<LittleEndian>(size);
        let offset = self.offset.expect("offset must be set");
        b.put_u64::<LittleEndian>(offset);
        b.freeze()
    }
}
