use std::io;

pub struct TableWriter<T> {
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

    pub fn write(&mut self, content: &[u8]) -> Result<usize, io::Error> {
        debug!("write data to table {:?}", content);
        self.offset += content.len();
        self.inner.write(content)
    }

    pub fn offset(&self) -> u64 {
        self.offset as u64
    }
}
