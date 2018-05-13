use super::{RecordType, crc32, BLOCK_SIZE, CHECKSUM_SIZE, HEADER_SIZE, LENGTH_SIZE, TYPE_SIZE};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use std::io::Read;
use std::iter::Iterator;

pub struct LogReader<T: Read> {
    inner: T,
    buffer: BytesMut,
}

impl<T: Read> LogReader<T> {
    pub fn new(reader: T) -> Self {
        LogReader {
            inner: reader,
            buffer: BytesMut::new(),
        }
    }

    pub fn read_record(&mut self) -> Option<Bytes> {
        let mut slice = Bytes::with_capacity(BLOCK_SIZE);
        let record_type = self.read_physical_record(&mut slice)
            .expect("invalid record type");

        // TODO fragment
        let record = match record_type {
            RecordType::FULL => None,
            RecordType::FIRST => self.read_record(),
            RecordType::MIDDLE => self.read_record(),
            RecordType::LAST => None,
            RecordType::EOF => return None,
        };

        if let Some(r) = record {
            slice.extend(r);
        }

        Some(slice)
    }

    fn read_physical_record(&mut self, ret: &mut Bytes) -> Result<RecordType, &'static str> {
        if self.buffer.len() < HEADER_SIZE {
            let mut v = [0; BLOCK_SIZE];
            let s = self.inner.read(&mut v).unwrap();
            if s == 0 {
                return Ok(RecordType::EOF);
            }
            self.buffer = BytesMut::from(&v[0..s]); // ignore size
        }

        let mut header = self.buffer.split_to(HEADER_SIZE);
        let expected_checksum = {
            let c = header.split_to(CHECKSUM_SIZE);
            LittleEndian::read_u32(&c)
        };

        let length = {
            let c = header.split_to(LENGTH_SIZE);
            LittleEndian::read_u16(&c)
        };

        let rtype = {
            let c = header.split_to(TYPE_SIZE);
            RecordType::from(c[0])
        };

        let record = self.buffer.split_to(length as usize);
        if crc32(&record) != expected_checksum {
            println!("invalid");
            return Err("validation failed");
        }

        debug!(
            "length={:?}, rtype={:?}, record={:?}",
            length, rtype, record
        );

        ret.extend(record);
        Ok(rtype)
    }
}

impl<T: Read> Iterator for LogReader<T> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_record()
    }
}

#[cfg(test)]
mod tests {
    use super::super::LogWriter;
    use super::*;
    use std::io::{BufReader, BufWriter, Cursor};

    #[test]
    fn log_reader_full_record() {
        let b = Bytes::from("key");
        let mut value: Vec<u8> = vec![];
        {
            let w = BufWriter::new(Cursor::new(&mut value));
            let mut lw = LogWriter::new(w);
            lw.add_record(b.clone());
        }

        let r = BufReader::new(Cursor::new(value));
        let mut reader = LogReader::new(r);
        assert_eq!(reader.read_record(), Some(b));
        assert_eq!(reader.read_record(), None);
    }

    #[test]
    fn log_reader_across() {
        let bs: Vec<Bytes> = (1..1000)
            .map(|v| Bytes::from(format!("key{:?}", v)))
            .collect();
        let mut value: Vec<u8> = vec![];
        {
            let w = BufWriter::new(Cursor::new(&mut value));
            let mut lw = LogWriter::new(w);

            for b in &bs {
                lw.add_record(b.clone());
            }
        }

        let r = BufReader::new(Cursor::new(value));
        let mut reader = LogReader::new(r);

        for b in &bs {
            assert_eq!(reader.read_record(), Some(b.clone()));
        }
    }
}
