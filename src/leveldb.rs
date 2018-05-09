use std::{fs, mem, path, str};
use std::io::{BufReader, BufWriter};
use bytes::Bytes;
use env_logger;

use filename;
use ikey::InternalKey;
use memdb::{MemDB, MemDBIterator};
use batch::WriteBatch;
use log_record::{LogReader, LogWriter};
use version::{VersionEdit, VersionSet};
use configure;
use table;
use random_access_file::MmapRandomAccessFile;

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
    table_cache: table::TableCache<MmapRandomAccessFile>,
    configure: configure::Configure,
    // Should have log file?
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
            table_cache: table::TableCache::new(dir),
            configure: Default::default(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Bytes> {
        let snapshot = self.versions.last_sequence;
        let ikey = InternalKey::new(key.as_bytes(), snapshot);

        debug!("snapshot id: {:}", snapshot);
        let ret = self.mem
            .get(&ikey)
            .or_else(|| self.imm.as_ref().and_then(|v| v.get(&ikey)));

        if ret.is_none() {
            let mut cache = &mut self.table_cache;
            self.versions.current().and_then(|v| v.get(&ikey, cache))
        } else {
            ret
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
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
        let min_log = self.log_nubmer;
        for p in paths {
            if let Some(path) = p.unwrap().path().to_str() {
                match filename::FileType::parse_name(path) {
                    filename::FileType::Log(_, num) => {
                        if num >= min_log {
                            log_paths.push(filename::SimpleName::new(num, path))
                        }
                    }
                    _ => (), // nothing
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

        edit.log_number = self.versions.next_file_num();
        self.log_nubmer = edit.log_number;
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

            let nseq = batch.seq();
            let num_seq = nseq + batch.count();
            if max_seq < num_seq {
                max_seq = num_seq;
            }

            let mut iter = batch.into_iter();
            for i in nseq..num_seq {
                let (key_kind, ukey, value) = iter.next().expect("batch size is invalid");
                let ikey = InternalKey::new_with_kind(&ukey, i as u64, key_kind);
                mem.add(&ikey, &value);
            }
        }

        if !mem.empty() {
            self.write_level0_table(edit, &mut mem.iter())
                .expect("failed to write write level 0 table")
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
        let meta = table::bulid(&self.dbname, mem, num)?;
        if meta.file_size == 0 {
            debug!("Skip adding table file to edit version, because file size is 0");
        } else {
            debug!("Add metadata to version edit: {:?}", meta);
            edit.add_file(meta, 0);
        }
        Ok(())
    }

    pub fn apply(&mut self, mut batch: WriteBatch) -> Result<(), String> {
        self.make_room_for_write(true)?; // for debug

        let seq = self.versions.last_sequence;
        batch.set_seq(seq + 1);
        self.versions.set_last_sequence(seq + batch.count() as u64);

        self.log.as_mut().map(|l| l.add_record(batch.data()));

        for (key_kind, ukey, value) in batch.into_iter() {
            let ikey = InternalKey::new_with_kind(&ukey, seq as u64, key_kind);
            self.mem.add(&ikey, &value);
        }

        Ok(())
    }

    // For now, single thread model
    fn make_room_for_write(&mut self, force: bool) -> Result<(), String> {
        if !force && self.configure.write_buffer_size > self.mem.approximately_size() {
            return Ok(());
        }
        debug!("Make rom for write!");

        self.log_nubmer = self.versions.next_file_num();
        let fname = filename::FileType::Log(&self.dbname, self.log_nubmer).filename();
        debug!("Use log file {:?}", fname);
        self.log = Some(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(fname)
            .map(|fd| LogWriter::new(BufWriter::new(fd)))
            .map_err(|_| "failed to open file")?);

        let old = mem::replace(&mut self.mem, MemDB::new());
        self.imm = Some(old);
        self.maybe_compaction();
        Ok(())
    }

    fn maybe_compaction(&mut self) {
        if self.imm.is_some() {
            self.compact_memtable()
        }
    }

    fn compact_memtable(&mut self) {
        if let Some(mem) = mem::replace(&mut self.imm, None) {
            if mem.empty() {
                debug!("Skip to compact memtable since memtable is empty");
                return;
            }

            debug!("Start memtable compactoin");
            let mut edit = VersionEdit::new(0);
            if let Err(msg) = self.write_level0_table(&mut edit, &mut mem.iter()) {
                self.imm = Some(mem); // put it back
                error!("during compaction, write_level0_table is failed: {:?}", msg);
                return;
            };

            edit.log_number = self.log_nubmer;
            self.versions.log_and_apply(&mut edit);

            self.delete_obsolete_file()
        }
    }
}
