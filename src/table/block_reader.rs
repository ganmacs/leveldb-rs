use std::io;
use super::{block_format, block::Block, table_reader::TableReader};
use slice;

pub struct BlockReader {}

impl BlockReader {
    pub fn new<T: io::Read + io::Seek>(reader: &mut T, bh_value: &mut slice::Slice) -> Block {
        let bh = block_format::BlockHandle::decode_from(bh_value);
        let block = TableReader::read_block(reader, &bh).expect("block ga!!!!");
        block
    }
}
