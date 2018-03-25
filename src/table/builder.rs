use std::fs;
use std::io::BufWriter;
use bytes::Bytes;
use table::table_writer::TableWriter;
use table::block_builder::BlockBuilder;

pub struct TableBuilder {
    writer: TableWriter<BufWriter<fs::File>>,
}

impl TableBuilder {
    pub fn new(fname: &str) -> Self {
        let fd = fs::OpenOptions::new() // add read permission?
            .write(true)
            .create(true)
            .open(fname)
            .unwrap();

        Self { writer: TableWriter::new(BufWriter::new(fd)) }
    }

    pub fn add(&mut self, key: &Bytes, value: &Bytes) {}

    pub fn build(&mut self) {}

    pub fn size(&self) -> usize {
        0
    }
}
