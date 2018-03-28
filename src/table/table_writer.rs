use std::io;
use bytes::Bytes;

pub struct TableWriter<T: io::Write> {
    inner: T,
    offset: usize,
}

impl<T: io::Write> TableWriter<T> {
    pub fn new(writer: T) -> TableWriter<T> {
        TableWriter {
            inner: writer,
            offset: 0,
        }
    }

    pub fn write(&mut self, content: &Bytes) -> Result<usize, io::Error> {
        self.offset += content.len();
        self.inner.write(content)
    }

    pub fn offset(&self) -> u64 {
        self.offset as u64
    }
}
