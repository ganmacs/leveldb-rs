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
    log: Option<LogWriter<BufWriter<fs::File>>>,
    dbname: String,
    versions: VersionSet,
    mem: MemDB,
    imm: Option<MemDB>,
    log_nubmer: u64,
}

impl LevelDB {
    fn new(dir: &str) -> Self {
        Self {
            dbname: dir.to_owned(),
            log: None,
            versions: VersionSet::new(dir),
            mem: MemDB::new(),
            imm: None,
            log_nubmer: 0,
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
        let mut log_paths = vec![];
        for p in paths {
            if let Some(path) = p.unwrap().path().to_str() {
                match filename::FileType::parse_name(path) {
                    filename::FileType::Log(_, num) => {
                        log_paths.push(filename::SimpleName::new(num, path))
                    }
                    _ => (),        // nothing
                }
            }
        }

        log_paths.sort();
        for path in log_paths {
            let m = self.replay_logfile(&path.name, &mut edit) as u64;
            self.versions.mark_file_num_used(path.num);
            if self.versions.last_sequence < m {
                debug!("max_seq_num is {:?}", m);
                self.versions.last_sequence = m;
            }
        }

        edit.next_file_number = self.versions.next_file_num();
        self.log_nubmer = edit.next_file_number;
        let fname = filename::FileType::Log(&self.dbname, self.log_nubmer).filename();
        debug!("Use log file {:?}", fname);
        self.log = Some(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(fname)
                .map(|fd| LogWriter::new(BufWriter::new(fd)))
                .expect("failed to create LogWriter"), // should be validate here
        );

        self.versions.log_and_apply(&mut edit);

        self.delete_obsolete_file()
    }

    fn delete_obsolete_file(&self) {
        let live_files = self.versions.live_files();

        let paths = fs::read_dir(&self.dbname).expect("Failed to read directory");
        for p in paths {
            if let Some(path) = p.unwrap().path().to_str() {
                let keep = match filename::FileType::parse_name(path) {
                    filename::FileType::Log(_, num) => num >= self.versions.log_number, // XXX
                    filename::FileType::Manifest(_, num) => {
                        num >= (self.versions.manifest_file_number as usize)
                    }
                    filename::FileType::Table(_, num) => {
                        live_files.iter().find(|&&v| v == num).is_some()
                    }
                    _ => true,
                };

                if !keep {
                    debug!("Delete obsolete file {:?}", path);
                    fs::remove_file(path).unwrap() // TODO: error mssage
                }
            }
        }
    }

    fn replay_logfile(&mut self, path: &str, edit: &mut VersionEdit) -> usize {
        debug!("Replay data from log file {:?}", path);
        let reader = fs::File::open(path)
            .map(|fs| LogReader::new(BufReader::new(fs)))
            .expect("failed to read log file");

        let mut max_seq = 0;
        let mut mem = MemDB::new();

        for r in reader.into_iter() {
            let batch = WriteBatch::load_data(r);

            let num_seq = batch.seq() + batch.count();
            if max_seq < num_seq {
                max_seq = num_seq;
            }

            for (key_kind, ukey, value) in batch.into_iter() {
                debug!("Add data to memdb key={:?}, value={:?}", ukey, value);
                mem.add(key_kind, &ukey, &value);
                // TODO: memory usage is larger than buffer size
            }

        }

        if !mem.empty() {
            self.write_level0_table(edit, &mut mem.into_iter()).expect(
                "failed to write write level 0 table",
            )
        }

        return max_seq;
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
        self.log.as_mut().map(|l| l.add_record(batch.data()));

        for (key_kind, ukey, value) in batch.into_iter() {
            self.mem.add(key_kind, &ukey, &value);
        }
    }
}
