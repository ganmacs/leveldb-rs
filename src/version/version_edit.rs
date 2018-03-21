use bytes::{Bytes, BufMut, BytesMut, LittleEndian, ByteOrder};
use log::LogWriter;
use std::io::Write;
use self::super::FileMetaData;

enum Tag {
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    PrevLogNumber = 9,
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
    files: Vec<(FileMetaData, usize)>,
    pub next_file_number: u64,
    pub last_sequence: u64,
    pub log_number: u64,
    pub prev_log_number: u64,
}

impl VersionEdit {
    pub fn new(nex_file_num: u64) -> Self {
        VersionEdit {
            files: vec![],
            next_file_number: nex_file_num,
            log_number: 0,
            last_sequence: 0,
            prev_log_number: 0,
        }
    }

    pub fn files(&self) -> &Vec<(FileMetaData, usize)> {
        &self.files
    }

    pub fn decode_from(&mut self, record: Bytes) {
        let mut i = 0;
        let limit = record.len();

        let val = record.slice(i, i + 1);
        i += 1;
        let t = val[0] as u8;

        match Tag::from(t) {
            Tag::CompactPointer => {}
            Tag::LogNumber => {
                let val = record.slice(i, i + 8);
                i += 8;
                let v = LittleEndian::read_u64(&val);
                println!("lognumber {:?}", v);
                self.log_number = v
            }
            Tag::NextFileNumber => {
                let val = record.slice(i, i + 8);
                i += 8;
                let v = LittleEndian::read_u64(&val);
                println!("next file number {:?}", v);
                self.next_file_number = v
            }
            Tag::LastSequence => {}
            Tag::Comparator => {}
            Tag::DeletedFile => {}
            Tag::NewFile => {}
            Tag::PrevLogNumber => {}
        }
    }

    pub fn encode_to<T: Write>(&self, writer: &mut LogWriter<T>) {
        let mut res = BytesMut::new();

        if self.log_number != 0 {
            res.put_u8(Tag::LogNumber as u8);
            res.put_u64::<LittleEndian>(self.log_number as u64);
        }

        if self.prev_log_number != 0 {
            res.put_u8(Tag::PrevLogNumber as u8);
            res.put_u64::<LittleEndian>(self.prev_log_number as u64);
        }

        if self.next_file_number != 0 {
            res.put_u8(Tag::NextFileNumber as u8);
            res.put_u64::<LittleEndian>(self.next_file_number as u64);
        }

        if self.last_sequence != 0 {
            res.put_u8(Tag::LastSequence as u8);
            res.put_u64::<LittleEndian>(self.last_sequence as u64);
        }

        for &(ref meta, ref level) in self.files.iter() {
            res.put_u8(Tag::NewFile as u8);
            res.put_u64::<LittleEndian>(*level as u64);
            res.put_u64::<LittleEndian>(meta.file_num);
            res.put_slice(&meta.largest());
            res.put_slice(&meta.smallest());
        }

        println!("{:?}", res);

        writer.add_record(res.freeze());
    }

    pub fn add_file(&mut self, meta: FileMetaData, level: usize) {
        self.files.push((meta, level));
    }
}
