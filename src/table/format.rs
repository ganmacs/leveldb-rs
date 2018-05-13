use super::table_builder::TRAILER_SIZE;
use super::{Compression, block::Block};
use random_access_file::RandomAccessFile;
use slice::{ByteRead, ByteWrite, Bytes, BytesMut};
use std::io;

// TABLE_MAGIC_NUMBER was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.

pub const FOOTER_MAX_LENGTH: usize = 2 * 2 * 8 + 8;
const TABLE_MAGIC_NUMBER: i64 = 0xdb4775248b80fb57;

#[derive(Debug)]
pub struct BlockHandle {
    size: Option<u64>,
    offset: Option<u64>,
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

    pub fn decode_from(input: &mut Bytes) -> Self {
        let size = Some(input.read_u64());
        let offset = Some(input.read_u64());
        Self { size, offset }
    }

    pub fn set_size(&mut self, v: u64) {
        self.size = Some(v)
    }

    pub fn set_offset(&mut self, v: u64) {
        self.offset = Some(v)
    }

    pub fn offset(&self) -> u64 {
        self.offset.expect("block handle must set offset")
    }

    pub fn size(&self) -> u64 {
        self.size.expect("block handle must set size")
    }

    pub fn encode(&self) -> Bytes {
        let mut slice = BytesMut::new();
        // TODO: put num as varint64 to reduce size
        let size = self.size.expect("size must be set");
        slice.write_u64(size);
        let offset = self.offset.expect("offset must be set");
        slice.write_u64(offset);
        slice.freeze()
    }
}

pub struct Footer {
    pub index_block_handle: BlockHandle,
    pub metaindex_block_handle: BlockHandle,
}

impl Footer {
    pub fn new(ibh: BlockHandle, mbh: BlockHandle) -> Self {
        Self {
            index_block_handle: ibh,
            metaindex_block_handle: mbh,
        }
    }

    pub fn decode(input: &[u8]) -> Self {
        let mut slice = Bytes::from(input);
        let index_block_handle = BlockHandle::decode_from(&mut slice);
        let metaindex_block_handle = BlockHandle::decode_from(&mut slice);
        if slice.read_i64() == TABLE_MAGIC_NUMBER {
            return Self {
                index_block_handle: index_block_handle,
                metaindex_block_handle: metaindex_block_handle,
            };
        };

        panic!("magic number is not correct")
    }

    pub fn encode(&self) -> Bytes {
        let mut slice = BytesMut::with_capacity(FOOTER_MAX_LENGTH);
        slice.write(&self.index_block_handle.encode());
        slice.write(&self.metaindex_block_handle.encode());
        slice.write_i64(TABLE_MAGIC_NUMBER);

        slice.freeze()
    }
}

pub fn read_block<T: io::Read + io::Seek>(
    reader: &mut T,
    block_handle: &BlockHandle,
) -> Option<Block> {
    reader.seek(io::SeekFrom::Start(block_handle.offset()));
    let block_size = block_handle.size() as usize;
    let mut buff = vec![0; TRAILER_SIZE + block_size];
    reader.read(&mut buff);

    let mut slice = Bytes::from(buff);
    let mut content = slice.read(block_size + 1);
    let _crc = slice.read_u32();
    // check crc

    let v = content.read(block_size);
    Some(match Compression::from(content[0]) {
        Compression::No => Block::new(v),
    })
}

pub fn read_block2<T: RandomAccessFile>(reader: &T, block_handle: &BlockHandle) -> Option<Block> {
    let block_size = block_handle.size() as usize;
    let slice = reader
        .read(block_handle.offset() as usize, block_size + TRAILER_SIZE)
        .map(|b| Bytes::from(b));

    match slice {
        Ok(mut b) => {
            let mut content = b.read(block_size + 1);
            let _crc = b.read_u32();
            // check crc

            let v = content.read(block_size);
            Some(match Compression::from(content[0]) {
                Compression::No => Block::new(v),
            })
        }
        Err(e) => {
            error!("{:?}", e);
            None
        }
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
