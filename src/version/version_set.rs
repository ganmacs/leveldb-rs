use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::io::BufReader;
use std::io::BufWriter;

use log_record::{LogReader, LogWriter};
use filename;
use super::{VersionEdit, FileMetaData, CircularLinkedList};

pub struct VersionSet {
    dbname: String,
    manifest_file_number: u64,
    pub log_number: u64,
    pub next_file_number: u64,
    pub prev_log_number: u64,
    pub last_sequence: u64,

    // dummy_version is the head of a doubly-linked list of versions.
    // dummy_Version.prev is the current version.
    dummy_version: CircularLinkedList<Version>,
    manifest: Option<LogWriter<BufWriter<fs::File>>>,
}

impl VersionSet {
    pub fn new(dbname: &str) -> Self {
        Self {
            dbname: dbname.to_owned(),
            manifest_file_number: 0, // will be filled in recover
            log_number: 0,
            next_file_number: 2, // 1 is reserved by Manifest file?
            prev_log_number: 0,
            last_sequence: 0,
            dummy_version: CircularLinkedList::new(Version::new()),
            manifest: None,
        }
    }

    pub fn current(&self) -> Option<&Version> {
        self.dummy_version.current()
    }

    pub fn log_and_apply(&mut self, edit: &mut VersionEdit) {
        // TODO: check log versoin is consistent

        if self.manifest.is_none() {
            self.create_manifest_file();
        }

        edit.next_file_number = self.next_file_number;
        edit.last_sequence = self.last_sequence;
        if let Some(m) = self.manifest.as_mut() {
            edit.encode_to(m);
        }
        filename::set_current_file(&self.dbname, self.manifest_file_number as usize);

        let mut vb = VersionBuilder::new();
        vb.apply(edit);

        let v = {
            let c = self.current().expect("current version does not exist");
            vb.save_to(c)
        };
        self.append(v);

        if edit.log_number != 0 {
            self.log_number = edit.log_number;
        }

        if edit.prev_log_number != 0 {
            self.prev_log_number = edit.prev_log_number;
        }
    }

    fn create_manifest_file(&mut self) {
        let manifest =
            filename::FileType::Manifest(&self.dbname, self.manifest_file_number as usize)
                .filename();

        debug!("open new manifest_file {:?}", manifest);
        let mut writer = fs::File::create(manifest)
            .map(|fs| LogWriter::new(BufWriter::new(fs)))
            .expect("Failed to create writer for manifest file");

        // Save compaction pointers

        let mut edit = VersionEdit::new(0); // 0 is ok?
        if let Some(current_version) = self.current() {
            for i in 0..LEVEL {
                for meta in current_version.files[i].iter() {
                    edit.add_file(meta.clone(), i);
                }
            }
        }

        debug!("Save current version info");
        edit.encode_to(&mut writer);

        self.manifest = Some(writer);
    }

    pub fn next_file_num(&mut self) -> u64 {
        let r = self.next_file_number;
        self.next_file_number += 1;
        r
    }

    pub fn recover(&mut self) {
        let current = filename::FileType::Current(&self.dbname).filename();
        let mut fs = fs::File::open(current).expect("fail to open current file");
        let mut name = String::new();
        fs.read_to_string(&mut name).expect(
            "failed to read current file content",
        );

        let n = format!("{:}/{:}", &self.dbname, name);
        debug!("Load current manifest file {:?}", name);
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
        self.next_file_number = next_file_number;
        self.mark_file_num_used(log_number);
        self.mark_file_num_used(prev_log_number);
        self.manifest_file_number = self.next_file_num();

        self.last_sequence = last_sequence;
        self.log_number = log_number;
        self.prev_log_number = prev_log_number;

        let mut ver = Version::new();
        let v = vb.save_to(&mut ver);
        self.append(v);

        debug!(
            "Recovered VersionSet dbname={:}, manifest_file_number={:?}, log_number={:?}, prev_file_number={:?}, next_file_number={:?}, last_sequence={:?}",
            self.dbname,
            self.manifest_file_number,
            self.log_number,
            self.prev_log_number,
            self.next_file_number,
            self.last_sequence
        );

        debug!("recoverd current version");

    }

    pub fn mark_file_num_used(&mut self, num: u64) {
        if self.next_file_number <= num {
            self.next_file_number = num + 1
        }
    }

    fn append(&mut self, v: Version) {
        debug!("Append version {:?}", v);
        self.dummy_version.append(v)
    }
}

const LEVEL: usize = 12;

#[derive(Debug)]
pub struct Version {
    files: Vec<Vec<FileMetaData>>, // table type file
                                   // Add field COMPACTION_LEVEL
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
        for &(ref meta, ref level) in edit.files() {
            self.added[*level].push(meta.clone());
        }
    }

    // TODO: name
    pub fn save_to(&self, base: &Version) -> Version {
        let mut version = Version::new();

        for i in 0..LEVEL {
            let ref d = self.deleted[i];

            let ref level_files = base.files[i];
            for f in level_files {
                if *(d.get(&f.file_num).unwrap_or(&false)) {
                    continue;
                }
                version.files[i].push(f.clone())
            }

            let ref level_files = self.added[i];
            for f in level_files {
                if *(d.get(&f.file_num).unwrap_or(&false)) {
                    continue;
                }

                version.files[i].push(f.clone())
            }

            version.files[i].sort();
        }

        version
    }
}
