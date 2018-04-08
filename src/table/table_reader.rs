use std::io;
use slice::Slice;
use super::block_format::BlockHandle;
use super::block::Block;
use table::Compression;
use super::builder::TRAILER_SIZE;

pub struct TableReader {}

impl TableReader {
    pub fn read_block<T: io::Read + io::Seek>(
        reader: &mut T,
        block_handle: &BlockHandle,
    ) -> Option<Block> {
        reader.seek(io::SeekFrom::Start(block_handle.offset()));
        let mut buff = vec![0; TRAILER_SIZE + block_handle.size() as usize];
        reader.read(&mut buff);

        let mut slice = Slice::from(&buff);
        let content = slice
            .read(block_handle.size() as usize)
            .expect("content is missing");
        let _crc = slice.read_u32().expect("invalid crc");
        // check crc

        let mut cs = Slice::from(&content);
        cs.read_u8().map(|v| match Compression::from(v) {
            Compression::No => Block::new(cs),
        })
    }
}
