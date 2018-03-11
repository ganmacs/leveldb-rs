use std::collections::HashMap;
use filename::FileType;
use std::fs;
use std::io::Read;
use std::io::BufReader;
use log::LogReader;
use super::{VersionEdit, FileMetaData};

pub struct VersionSet {
    dbname: String,
    file_number: u64,
    pub log_number: u64,
    pub next_file_number: u64,
    pub prev_log_number: u64,
    pub last_sequence: u64,
}

impl VersionSet {
    pub fn new(dbname: &str) -> Self {
        Self {
            dbname: dbname.to_owned(),
            file_number: 1,
            log_number: 0,
            next_file_number: 0,
            prev_log_number: 0,
            last_sequence: 0,
        }
    }

    pub fn next_file_num(&mut self) -> u64 {
        self.file_number += 1;
        self.file_number
    }

    pub fn recover(&mut self) {
        let current = FileType::Current(&self.dbname).filename();
        let mut fs = fs::File::open(current).expect("fail to open current file");
        let mut name = String::new();
        fs.read_to_string(&mut name).expect(
            "failed to read current file content",
        );

        let n = format!("{:}/{:}", &self.dbname, name);
        let reader = fs::File::open(n)
            .map(|fs| LogReader::new(BufReader::new(fs)))
            .expect("failed to read manifest");

        let mut log_number = 0;
        let mut prev_log_number = 0;
        let mut next_file_number = 0;
        let mut last_sequence = 0;
        let mut vb = VersionBuilder::new();

        for record in reader.into_iter() {
            let mut ve = VersionEdit::new(0);
            ve.decode_from(record);

            vb.apply(&ve);

            if ve.log_number != 0 {
                log_number = ve.log_number
            }

            if ve.prev_log_number != 0 {
                prev_log_number = ve.prev_log_number
            }

            if ve.next_file_number != 0 {
                next_file_number = ve.next_file_number
            }

            if ve.last_sequence != 0 {
                last_sequence = ve.last_sequence
            }
        }

        self.mark_file_num_used(log_number);
        self.mark_file_num_used(prev_log_number);

        self.next_file_number = next_file_number;
        self.prev_log_number = prev_log_number;
        self.last_sequence = last_sequence;

        let v = vb.save_to();
        self.append(v);
    }

    fn append(&mut self, v: Version) {
        // let v = self.prev.next;
        // self.prev.next = v
    }

    fn mark_file_num_used(&mut self, num: u64) {
        if self.file_number <= num {
            self.file_number = num + 1
        }
    }
}

const LEVEL: usize = 12;

pub struct Version {
    files: Vec<Vec<FileMetaData>>,
    // prev: &'a Version<'a>, // TODO
    // next: &'a Version<'a>,
}

impl Version {
    pub fn new() -> Self {
        let mut files = Vec::new();
        for _ in 0..LEVEL {
            files.push(Vec::new());
        }
        Self { files }
    }
}

pub struct VersionBuilder {
    added: Vec<Vec<FileMetaData>>,
    deleted: Vec<HashMap<u64, bool>>,
}

impl VersionBuilder {
    pub fn new() -> Self {
        let mut added = Vec::new();
        let mut deleted = Vec::new();
        for _ in 0..LEVEL {
            added.push(Vec::new());
            deleted.push(HashMap::new());
        }

        Self { added, deleted }
    }

    pub fn apply(&mut self, edit: &VersionEdit) {
        for &(ref meta, ref level) in edit.new_files() {
            self.added[*level].push(meta.clone());
        }
    }

    pub fn save_to(&mut self) -> Version {
        let v = Version::new();
        // for f in self.added {}
        v
    }
}
