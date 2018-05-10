use std::io::Write;
use bytes::BufMut;

use log_record::LogWriter;
use super::{FileMetaData, BLOCK_SIZE};
use slice::{ByteRead, Bytes, BytesMut};
use ikey::InternalKey;

enum Tag {
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    PrevLogNumber = 8,
}

impl From<u8> for Tag {
    fn from(v: u8) -> Self {
        match v {
            1 => Tag::CompactPointer,
            2 => Tag::LogNumber,
            3 => Tag::NextFileNumber,
            4 => Tag::LastSequence,
            5 => Tag::Comparator,
            6 => Tag::DeletedFile,
            7 => Tag::NewFile,
            8 => Tag::PrevLogNumber,
            _ => panic!(format!("convert failed {:?}", v)),
        }
    }
}

pub struct VersionEdit {
    pub files: Vec<FileMetaData>,
    pub deleted_files: Vec<FileMetaData>,
    pub next_file_number: u64,
    pub last_sequence: u64,
    pub log_number: u64,
    pub prev_log_number: u64,
}

impl VersionEdit {
    pub fn new(nex_file_num: u64) -> Self {
        VersionEdit {
            files: vec![],
            deleted_files: vec![],
            next_file_number: nex_file_num,
            log_number: 0,
            last_sequence: 0,
            prev_log_number: 0,
        }
    }

    pub fn files<'a>(&self) -> &Vec<FileMetaData> {
        &self.files
    }

    pub fn deleted_files<'a>(&self) -> &Vec<FileMetaData> {
        &self.deleted_files
    }

    pub fn decode_from(&mut self, record: Bytes) {
        let mut input = record.clone(); // copy?

        while input.len() > 0 {
            let tag = input.read_u8();
            match Tag::from(tag) {
                Tag::LogNumber => self.log_number = input.read_u64(),
                Tag::NextFileNumber => self.next_file_number = input.read_u64(),
                Tag::LastSequence => self.last_sequence = input.read_u64(),
                Tag::PrevLogNumber => self.prev_log_number = input.read_u64(),
                Tag::CompactPointer => unimplemented!(),
                Tag::Comparator => unimplemented!(),
                Tag::DeletedFile => {
                    let level = input.read_u64();
                    let file_num = input.read_u64();
                    let file_size = input.read_u64();
                    let largest_size = input.read_u64() as usize;
                    let largest = input.read(largest_size);
                    let smallest_size = input.read_u64() as usize;
                    let smallest = input.read(smallest_size);
                    self.deleted_files.push(FileMetaData {
                        file_num: file_num,
                        file_size: file_size,
                        largest: InternalKey::from(largest),
                        smallest: InternalKey::from(smallest),
                        level: level,
                    });
                }
                Tag::NewFile => {
                    let level = input.read_u64();
                    let file_num = input.read_u64();
                    let file_size = input.read_u64();
                    let largest_size = input.read_u64() as usize;
                    let largest = input.read(largest_size);
                    let smallest_size = input.read_u64() as usize;
                    let smallest = input.read(smallest_size);
                    self.files.push(FileMetaData {
                        file_num: file_num,
                        file_size: file_size,
                        largest: InternalKey::from(largest),
                        smallest: InternalKey::from(smallest),
                        level: level,
                    });
                }
            }
        }
    }

    pub fn encode_to<T: Write>(&self, writer: &mut LogWriter<T>) {
        let mut res = BytesMut::with_capacity(BLOCK_SIZE);

        if self.log_number != 0 {
            res.put_u8(Tag::LogNumber as u8);
            res.put_u64_le(self.log_number as u64);
        }

        if self.prev_log_number != 0 {
            res.put_u8(Tag::PrevLogNumber as u8);
            res.put_u64_le(self.prev_log_number as u64);
        }

        if self.next_file_number != 0 {
            res.put_u8(Tag::NextFileNumber as u8);
            res.put_u64_le(self.next_file_number as u64);
        }

        if self.last_sequence != 0 {
            res.put_u8(Tag::LastSequence as u8);
            res.put_u64_le(self.last_sequence as u64);
        }

        for meta in self.files.iter() {
            res.put_u8(Tag::NewFile as u8);
            res.put_u64_le(meta.level);
            res.put_u64_le(meta.file_num);
            res.put_u64_le(meta.file_size);
            res.put_u64_le(meta.largest().len() as u64);
            res.put_slice(meta.largest().as_ref());
            res.put_u64_le(meta.smallest().len() as u64);
            res.put_slice(meta.smallest().as_ref());
        }

        for meta in self.deleted_files.iter() {
            res.put_u8(Tag::DeletedFile as u8);
            res.put_u64_le(meta.level);
            res.put_u64_le(meta.file_num);
            res.put_u64_le(meta.file_size);
            res.put_u64_le(meta.largest().len() as u64);
            res.put_slice(meta.largest().as_ref());
            res.put_u64_le(meta.smallest().len() as u64);
            res.put_slice(meta.smallest().as_ref());
        }

        debug!(
            "Write data log_number={:}, prev_log_number={:?}, next_file_number={:?}, last_sequence={:?} to manifest file",
            self.log_number,
            self.prev_log_number,
            self.next_file_number,
            self.last_sequence
        );
        writer.add_record(res.freeze());
    }

    pub fn add_file(&mut self, meta: FileMetaData) {
        self.files.push(meta);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log_record::LogReader;
    use std::io::{BufReader, BufWriter, Cursor};

    #[test]
    fn version_edit_encode_and_decode() {
        let nfile = FileMetaData {
            file_num: 1,
            file_size: 2,
            smallest: InternalKey::new("3".as_bytes(), 4),
            largest: InternalKey::new("5".as_bytes(), 6),
            level: 7,
        };

        let dfile = FileMetaData {
            file_num: 10,
            file_size: 11,
            smallest: InternalKey::new("12".as_bytes(), 13),
            largest: InternalKey::new("14".as_bytes(), 15),
            level: 16,
        };

        let ve = VersionEdit {
            files: vec![nfile],
            deleted_files: vec![dfile],
            next_file_number: 10,
            log_number: 11,
            last_sequence: 12,
            prev_log_number: 13,
        };

        let mut result: Vec<u8> = vec![];
        // encode version edit
        {
            let w = BufWriter::new(Cursor::new(&mut result));
            let mut lw = LogWriter::new(w);
            ve.encode_to(&mut lw);
        }

        let r = BufReader::new(Cursor::new(result));
        let mut reader = LogReader::new(r);
        let mut ve2 = VersionEdit::new(0);
        ve2.decode_from(reader.read_record().unwrap());

        assert_eq!(ve2.files[0], ve.files[0]);
        assert_eq!(ve2.deleted_files[0], ve.deleted_files[0]);
        assert_eq!(ve2.next_file_number, ve.next_file_number);
        assert_eq!(ve2.log_number, ve.log_number);
        assert_eq!(ve2.last_sequence, ve.last_sequence);
        assert_eq!(ve2.prev_log_number, ve.prev_log_number);
    }
}
