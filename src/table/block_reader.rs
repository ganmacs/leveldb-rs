use std::io;
use super::{format, block::Block, table_reader::TableReader};
use slice;

pub struct BlockReader {}

impl BlockReader {
    pub fn new<T: io::Read + io::Seek>(reader: &mut T, bh_value: &mut slice::Slice) -> Block {
        let block = TableReader::read_block(reader, &bh).expect("block ga!!!!");
        let bh = format::BlockHandle::decode_from(bh_value);
        block
    }
}
