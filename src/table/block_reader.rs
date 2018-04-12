use std::io;
use super::{format, block::Block};
use slice;

pub struct BlockReader {}

impl BlockReader {
    pub fn new<T: io::Read + io::Seek>(reader: &mut T, bh_value: &mut slice::Slice) -> Block {
        let bh = format::BlockHandle::decode_from(bh_value);
        let block = format::read_block(reader, &bh).expect("block ga!!!!");
        block
    }
}
