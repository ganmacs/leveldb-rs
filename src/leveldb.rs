use std::path;
use std::fs;
use std::str;
use std::io::{BufWriter, BufReader};
use bytes::Bytes;
use env_logger;

use filename;
use ikey::InternalKey;
use memdb::{MemDBIterator, MemDB};
use batch::WriteBatch;
use log_record::{LogReader, LogWriter};
use version::{VersionSet, VersionEdit};
use table::TableBuilder;

pub fn open(dir: &str) -> LevelDB {
    env_logger::init();
    setup_level_db(dir);

    let mut db = LevelDB::new(dir);
    db.recover();
    db
}

// Create directory and files which are used by leveldb
fn setup_level_db(dbname: &str) {
    if path::Path::new(dbname).exists() {
        return;
    }

    debug!("Create directory {:?}", dbname);
    fs::create_dir(dbname).expect("failed to create directory");

    let manifest_file_num: usize = 1;
    let current = filename::FileType::Current(dbname).filename();

    if !path::Path::new(&current).exists() {
        debug!("Create current file {:?}", current);
        let edit = VersionEdit::new((manifest_file_num + 1) as u64);
        let manifest = filename::FileType::Manifest(dbname, manifest_file_num).filename();
        let mut writer = fs::File::create(manifest)
            .map(|fs| LogWriter::new(BufWriter::new(fs)))
            .expect("Failed to create writer for manifest file");
        edit.encode_to(&mut writer);

        filename::set_current_file(dbname, manifest_file_num);
    }
}

pub struct LevelDB {
    log: LogWriter<BufWriter<fs::File>>,
    dbname: String,
    versions: VersionSet,
    mem: MemDB,
    imm: Option<MemDB>,
}

impl LevelDB {
    fn new(dir: &str) -> Self {
        let mut v = VersionSet::new(dir);
        let fname = filename::FileType::Log(dir, v.next_file_num()).filename();
        debug!("Use log file {:?}", fname);
        let writer =
            fs::OpenOptions::new() // add read permission?
            .write(true)
            .create(true)
            .open(fname)
            .map( |fd| LogWriter::new(BufWriter::new(fd))).expect("failed to create LogWriter");

        Self {
            dbname: dir.to_owned(),
            log: writer,
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
        debug!("Set key={:?}, value={:?}", key, value);
        let mut b = WriteBatch::new();
        b.put(key, value);
        self.apply(b)
    }

    fn recover(&mut self) {
        debug!("Start recovering phase");
        self.versions.recover();

        let mut edit = VersionEdit::new(0);
        let paths = fs::read_dir(&self.dbname).expect("Failed to read directory");

        for p in paths {
            let path = &p.unwrap().path();
            let ft = filename::FileType::parse_name(path.to_str().unwrap());
            if ft.is_logfile() {
                self.replay_logfile(path, &mut edit);
            }
        }
    }

    fn replay_logfile(&mut self, path: &path::PathBuf, edit: &mut VersionEdit) {
        debug!("Replay data from log file {:?}", path);
        let reader = fs::File::open(path)
            .map(|fs| LogReader::new(BufReader::new(fs)))
            .expect("failed to read log file");

        let v = reader.into_iter().flat_map(
            |r| WriteBatch::load_data(r).into_iter(),
        );

        let mut mem = MemDB::new();
        for (key_kind, ukey, value) in v {
            debug!("Add data to memdb key={:?}, value={:?}", ukey, value);
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
        debug!("Write to level0 talble");
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
}
