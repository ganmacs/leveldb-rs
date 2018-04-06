use std::fs;
use std::io::BufWriter;
use crc::{Hasher32, crc32};
use table::{Compression, block_builder::BlockBuilder, block_format::{BlockHandle, Footer},
            table_writer::TableWriter};
use slice::Slice;
use slice;

pub struct TableBuilder {
    writer: TableWriter<BufWriter<fs::File>>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    filter_block: Option<u64>, // FIX
    pending_handle: BlockHandle,
    pending_index_entry: bool,
    last_key: Slice,
}

const TRAILER_SIZE: usize = 5;

impl TableBuilder {
    pub fn new(fname: &str) -> Self {
        debug!("Open file {:?} for table", fname);
        let fd = fs::OpenOptions::new() // add read permission?
            .write(true)
            .create(true)
            .open(fname)
            .unwrap();

        Self {
            writer: TableWriter::new(BufWriter::new(fd)),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            pending_handle: BlockHandle::new(),
            pending_index_entry: false,
            filter_block: None,
            last_key: Slice::new(),
        }
    }

    pub fn add(&mut self, key: &mut Slice, value: &Slice) {
        if self.pending_index_entry {
            slice::short_successor(key);

            let content = self.pending_handle.encode();
            self.index_block.add(key, &content);
            self.pending_index_entry = false;
        }

        self.data_block.add(key, value);
        self.last_key = key.clone();

        // FIX: 1024
        if self.data_block.estimated_current_size() >= 1024 {
            debug!("Estimated size exceeds specifed size");
            self.build()
        }
    }

    pub fn build(&mut self) {
        self.flush();

        if let Some(_) = self.filter_block {
            // TODO: write filter block
        }

        let metaindex_block_handle = {
            let mut meta_index_block = BlockBuilder::new();
            if let Some(_) = self.filter_block {
                // TODO: write filter block
            }
            let content = meta_index_block.build();
            self.write_block(&content)
        };

        // index
        let index_block_handle = {
            if self.pending_index_entry {
                slice::short_successor(&mut self.last_key);
                // let ss = TableBuilder::succ(&self.last_key);
                let content = self.pending_handle.encode();
                self.index_block.add(&self.last_key, &content);
                self.pending_index_entry = false;
            }
            let content = self.index_block.build();
            self.write_block(&content)
        };

        // footer
        {
            let footer = Footer::new(index_block_handle, metaindex_block_handle);
            let content = footer.encode();
            debug!("Write footer to file. offset is {:?}", self.size());
            self.writer
                .write(content.as_ref())
                .expect("Writing data is failed");
        }
    }

    pub fn size(&self) -> usize {
        self.writer.offset() as usize
    }

    fn flush(&mut self) {
        if self.data_block.empty() {
            return;
        }

        let content = self.data_block.build();
        self.pending_handle = self.write_block(&content);
        self.pending_index_entry = true;
    }

    fn write_block(&mut self, content: &Slice) -> BlockHandle {
        let kind = Compression::No;
        self.write_raw_block(content, kind)
    }

    fn write_raw_block(&mut self, content: &Slice, kindt: Compression) -> BlockHandle {
        // offset must be set before writer.write
        let bh = BlockHandle::from((TRAILER_SIZE + content.len()) as u64, self.writer.offset());

        let kind = kindt as u8;
        let content_slice = content.as_ref();
        self.writer
            .write(content_slice)
            .expect("Writing data is failed");

        // crc
        {
            let crc = {
                let mut digest = crc32::Digest::new(crc32::IEEE);
                digest.write(content_slice);
                digest.write(&[kind]);
                digest.sum32()
            };

            let mut trailer = Slice::with_capacity(TRAILER_SIZE);
            trailer.put_u8(kind);
            trailer.put_u32(crc);
            self.writer
                .write(trailer.as_ref())
                .expect("Writing data is failed");
        }

        debug!(
            "Write data to filesize={:?}. and offset={:?}",
            bh.size(),
            bh.offset()
        );

        bh
    }
}
