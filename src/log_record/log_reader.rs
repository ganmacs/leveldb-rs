use std::io::Read;
use bytes::{Bytes, LittleEndian, ByteOrder, BytesMut};
use super::{RecordType, BLOCK_SIZE, HEADER_SIZE, LENGTH_SIZE, TYPE_SIZE, CHECKSUM_SIZE, crc32};
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
        let record_type = self.read_physical_record(&mut slice).expect(
            "invalid record type",
        );

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
            length,
            rtype,
            record
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

// #[cfg(test)]
// mod tests {
//     use batch::WriteBatch;
//     use std::fs;
//     use std::io::BufReader;
//     use super::LogReader;

//     #[test]
//     fn read_full_record() {
//         let reader = BufReader::new(fs::File::open("test/data/full_record.log").unwrap());
//         let mut lr = LogReader::new(reader);
//         let s = lr.read_record().unwrap();
//         let batch = WriteBatch::load_data(s);
//         assert_eq!(batch.count(), 1);
//     }

//     #[test]
//     fn read_across_record() {
//         let reader = BufReader::new(fs::File::open("test/data/across_record.log").unwrap());
//         let mut lr = LogReader::new(reader);
//         let s = lr.read_record().unwrap();
//         let batch = WriteBatch::load_data(s);
//         assert_eq!(batch.count(), 1);
//     }
// }
