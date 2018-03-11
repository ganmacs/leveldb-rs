extern crate bytes;
extern crate crc;
extern crate rand;
extern crate regex;
#[macro_use]
extern crate lazy_static;

mod log;
mod batch;
mod memdb;
mod ikey;
mod filename;
mod version;
mod table;

pub use batch::WriteBatch;
use bytes::Bytes;
use log::{LogReader, LogWriter};
use memdb::MemDB;
use ikey::InternalKey;
use std::fs;
use std::io::{BufWriter, BufReader};
use std::fs::File;
use std::path::Path;
use std::str;
use filename::FileType;
use version::{VersionSet, VersionEdit};
use table::TableBuilder;
use memdb::MemDBIterator;

pub struct LevelDB {
    log: LogWriter<BufWriter<File>>,
    dbname: String,
    versions: VersionSet,
    mem: MemDB,
    imm: Option<MemDB>,
}

pub fn open(dir: &str) -> LevelDB {
    let mut db = LevelDB::new(dir);
    let mut edit = VersionEdit::new(0); // XXX
    db.recover(&mut edit);
    db
}

impl LevelDB {
    fn new(dir: &str) -> Self {
        if !Path::new(&dir).exists() {
            fs::create_dir(&dir).unwrap()
        }

        let mut v = VersionSet::new(dir);
        let fname = FileType::Log(dir, v.next_file_num()).filename();
        let fd = fs::OpenOptions::new() // add read permission?
            .write(true)
            .create(true)
            .open(fname)
            .unwrap();
        let writer = BufWriter::new(fd);
        Self {
            dbname: dir.to_owned(),
            log: LogWriter::new(writer),
            versions: v,
            mem: MemDB::new(),
            imm: None,
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let ikey = InternalKey::new(key, 0); // XXX use actual seq
        self.mem.get(&ikey).or_else(|| {
            self.imm.as_ref().and_then(|v| v.get(&ikey))
        })
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let mut b = WriteBatch::new();
        b.put(key, value);
        self.apply(b)
    }

    fn recover(&mut self, edit: &mut VersionEdit) {
        self.setup_metafile();
        self.versions.recover();

        let paths = fs::read_dir(&self.dbname).expect("Failed to read log files");
        for p in paths {
            let path = &p.unwrap().path();
            let ft = FileType::parse_name(path.to_str().unwrap());
            if ft.is_logfile() {
                self.replay_logfile(path, edit);
            }
        }
    }

    fn replay_logfile(&mut self, path: &std::path::PathBuf, edit: &mut VersionEdit) {
        let reader = fs::File::open(path)
            .map(|fs| LogReader::new(BufReader::new(fs)))
            .expect("failed to read log file");

        let v = reader.into_iter().flat_map(
            |r| WriteBatch::load_data(r).into_iter(),
        );

        let mut mem = MemDB::new();
        for (key_kind, ukey, value) in v {
            mem.add(key_kind, &ukey, &value);
            // TODO: memory usage is larger than buffer size
        }

        if !mem.empty() {
            self.write_level0_table(edit, &mut mem.into_iter()).expect(
                "failed to write write level 0 table",
            )
        }
    }

    fn write_level0_table(
        &mut self,
        edit: &mut VersionEdit,
        mem: &mut MemDBIterator,
    ) -> Result<(), &'static str> {
        let num = self.versions.next_file_num();
        let meta = TableBuilder::build(&self.dbname, mem, num)?;
        edit.add_file(meta, 0);
        Ok(())
    }

    pub fn apply(&mut self, batch: WriteBatch) {
        self.log.add_record(batch.data());

        for (key_kind, ukey, value) in batch.into_iter() {
            self.mem.add(key_kind, &ukey, &value);
        }
    }

    fn setup_metafile(&self) {
        let manifest_file_num: usize = 1;
        let current = FileType::Current(&self.dbname).filename();

        if !Path::new(&current).exists() {
            let edit = VersionEdit::new((manifest_file_num + 1) as u64);
            let manifest = FileType::Manifest(&self.dbname, manifest_file_num).filename();
            let mut writer = fs::File::create(manifest)
                .map(|fs| LogWriter::new(BufWriter::new(fs)))
                .expect("Failed to create writer for manifest file");
            edit.encode_to(&mut writer);

            filename::set_current_file(&self.dbname, manifest_file_num);
        }
    }
}
