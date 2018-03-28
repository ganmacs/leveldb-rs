use std::{fs, cmp};
use std::io::BufWriter;
use crc::{Hasher32, crc32};
use bytes::{BytesMut, Bytes, LittleEndian, BufMut};
use table::table_writer::TableWriter;
use table::block_builder::BlockBuilder;
use table::block_format::BlockHandle;
use super::Compression;

pub struct TableBuilder {
    writer: TableWriter<BufWriter<fs::File>>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    filter_block: Option<u64>, // FIX
    pending_handle: BlockHandle,
    pending_index_entry: bool,
    last_key: Bytes,
}

const TRAILER_SIZE: usize = 5;

impl TableBuilder {
    pub fn new(fname: &str) -> Self {
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
            last_key: Bytes::new(),
        }
    }

    pub fn succ(key: &Bytes) -> Bytes {
        key.clone() // FIX
    }

    pub fn short_sep(k1: &Bytes, k2: &Bytes) -> Bytes {
        let mut count = 0;
        let min_size = cmp::min(k1.len(), k2.len());
        for i in 0..min_size {
            if k1[i] == k2[i] {
                count += 1;
            } else {
                // let mut k =  k1.slice(0, count-1);
                // let v = k1.slice(count, count+1) as u8;
                // k.extend(v+1)
                return k1.slice(0, count - 1); // should be incrementd
            }
        }
        k1.clone()
    }

    pub fn add(&mut self, key: &Bytes, value: &Bytes) {
        if self.pending_index_entry {
            let ss = TableBuilder::short_sep(key, &self.last_key);
            let content = self.pending_handle.encode();
            self.index_block.add(&ss, &content);
            self.pending_index_entry = false;
        }

        self.data_block.add(key, value);
        self.last_key = key.clone();
    }

    pub fn build(&mut self) {
        self.flush();

        if let Some(_) = self.filter_block {
            // TODO: write filter block
        }

        // let metaindex_block_handle = {
        //     let mut meta_index_block = BlockBuilder::new();
        //     if let Some(_) = self.filter_block {
        //         // TODO: write filter block
        //     }
        //     let content = meta_index_block.build();
        //     self.write_block(&content)
        // };

        // index
        let index_block_handle = {
            if self.pending_index_entry {
                let ss = TableBuilder::succ(&self.last_key);
                let content = self.pending_handle.encode();
                self.index_block.add(&ss, &content);
                self.pending_index_entry = false;
            }
            let content = self.index_block.build();
            self.write_block(&content)
        };

    }

    pub fn size(&self) -> usize {
        unimplemented!()
    }

    fn flush(&mut self) {
        if self.data_block.empty() {
            return;
        }

        let content = self.data_block.build();
        self.pending_handle = self.write_block(&content);
        self.pending_index_entry = true;
    }

    fn write_block(&mut self, content: &Bytes) -> BlockHandle {
        let kind = Compression::No;
        self.write_raw_block(content, kind)
    }

    fn write_raw_block(&mut self, content: &Bytes, kindt: Compression) -> BlockHandle {
        let kind = kindt as u8;
        self.writer.write(content).expect("Writing data is failed");

        let crc = {
            let mut digest = crc32::Digest::new(crc32::IEEE);
            digest.write(content);
            digest.write(&[kind]);
            digest.sum32()
        };

        let mut v = BytesMut::with_capacity(TRAILER_SIZE);
        v.put_u8(kind);
        v.put_u32::<LittleEndian>(crc);
        self.writer.write(&v.freeze()).expect(
            "Writing data is failed",
        );

        BlockHandle::from(content.len() as u64, self.writer.offset())
    }
}
