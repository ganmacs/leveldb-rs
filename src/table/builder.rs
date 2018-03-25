use std::fs;
use std::io::BufWriter;
use crc::{Hasher32, crc32};
use bytes::{BytesMut, Bytes, LittleEndian, BufMut};
use table::table_writer::TableWriter;
use table::block_builder::BlockBuilder;
use super::Compression;

pub struct TableBuilder {
    writer: TableWriter<BufWriter<fs::File>>,
    data_block: BlockBuilder,
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
        }
    }

    pub fn add(&mut self, key: &Bytes, value: &Bytes) {
        self.data_block.add(key, value)
    }

    pub fn build(&mut self) {
        unimplemented!()
    }

    pub fn size(&self) -> usize {
        unimplemented!()
    }

    fn flush(&mut self) {
        if self.data_block.empty() {
            return;
        }

        let content = self.data_block.build();
        self.write_block(&content)
    }

    fn write_block(&mut self, content: &Bytes) {
        let kind = Compression::No;
        self.write_raw_block(content, kind)
    }

    fn write_raw_block(&mut self, content: &Bytes, kindt: Compression) {
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
    }
}
