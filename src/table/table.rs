use std::fs;
use std::io;
use std::io::{BufReader, Seek};
use std::io::Read;

use super::block_format::{Footer, FOOTER_MAX_LENGTH};
use super::table_reader::TableReader;
use super::{block::Block, block_reader::BlockReader};
use slice::Slice;

pub struct Table {
    index_block: Block,
    reader: BufReader<fs::File>,
}

impl Table {
    pub fn open(fname: &str, file_size: u64) -> Self {
        debug!("Open Table file {:?} for read", fname);
        let fd = fs::OpenOptions::new() // add read permission?
            .read(true)
            .open(fname)
            .unwrap();

        if (FOOTER_MAX_LENGTH as u64) > file_size {
            error!("file size is too samll {:?}", file_size);
        }

        let mut reader = BufReader::new(fd);

        let offset = file_size - FOOTER_MAX_LENGTH as u64;
        reader.seek(io::SeekFrom::Start(offset));
        let mut _footer = [0; FOOTER_MAX_LENGTH];
        reader
            .read_exact(&mut _footer)
            .expect(&format!("Failed to read footer from {:?}", fname));
        let footer = Footer::decode(&_footer);

        reader.seek(io::SeekFrom::Start(0));
        debug!(
            "Read footer data index_block(offset={:?}, size={:?}), metaindex(offset={:?}, size={:?})",
            footer.index_block_handle.offset(),
            footer.index_block_handle.size(),
            footer.metaindex_block_handle.offset(),
            footer.metaindex_block_handle.size()
        );
        let index_block =
            TableReader::read_block(&mut reader, &footer.index_block_handle).expect("block need");
        Self {
            index_block: index_block,
            reader: reader,
        }
    }

    pub fn get(&mut self, key: &Slice) -> Option<Slice> {
        if let Some(index_value) = self.index_block.iter().seek(key) {
            let mut block = BlockReader::new(&mut self.reader, &mut index_value.clone()); // XXX
            block.iter().seek(key)
        } else {
            None
        }
    }
}
