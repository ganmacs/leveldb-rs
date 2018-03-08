use ikey::InternalKey2;
use bytes::{BufMut, BytesMut, LittleEndian};
use log::LogWriter;
use std::io::Write;

pub struct FileMetaDataBuilder {
    file_num: Option<u64>,
    size: Option<u64>,
    largest: Option<InternalKey2>,
    smallest: Option<InternalKey2>,
}

impl FileMetaDataBuilder {
    pub fn new() -> Self {
        FileMetaDataBuilder {
            file_num: None,
            size: None,
            largest: None,
            smallest: None,
        }
    }

    pub fn file_num(&mut self, num: u64) -> &Self {
        self.file_num = Some(num);
        self
    }

    pub fn size(&mut self, size: u64) -> &Self {
        self.size = Some(size);
        self
    }

    pub fn largest(&mut self, largest: InternalKey2) -> &Self {
        self.largest = Some(largest);
        self
    }

    pub fn smallest(&mut self, smallest: InternalKey2) -> &Self {
        self.smallest = Some(smallest);
        self
    }

    pub fn build(self) -> FileMetaData {
        if self.file_num.is_none() {
            panic!("file num must be set")
        }

        if self.size.is_none() {
            panic!("size must be set")
        }

        if self.largest.is_none() {
            panic!("largest must be set")
        }

        if self.smallest.is_none() {
            panic!("smallest must be set")
        }

        FileMetaData {
            file_num: self.file_num.unwrap(),
            size: self.size.unwrap(),
            largest: self.largest.unwrap(),
            smallest: self.smallest.unwrap(),
        }
    }
}


pub struct FileMetaData {
    file_num: u64,
    size: u64,
    largest: InternalKey2,
    smallest: InternalKey2,
}

impl FileMetaData {
    // pub fn new(num: u64) -> Self {
    //     FileMetaData {
    //         file_num: num,
    //         size: 0,
    //     }
    // }

    // pub fn file_name()
}

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

pub struct VersionEdit {
    files: Vec<(FileMetaData, usize)>,
    next_file_number: u64,
    last_sequence: u64,
    log_number: u64,
    prev_log_number: u64,
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
            res.put_slice(&meta.largest);
            res.put_slice(&meta.smallest);
        }

        writer.add_record(res.freeze());
    }

    pub fn add_file(&mut self, meta: FileMetaData, level: usize) {
        self.files.push((meta, level));
    }
}
