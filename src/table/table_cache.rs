use std::collections::HashMap;
use filename;
use super::table;
use bytes::Bytes;
use memmap::Mmap;

pub struct TableCache {
    cache: HashMap<u64, TableAndFile>, // TODO: use more smart cache
    db_name: String,
}

pub struct TableAndFile {
    table: table::Table<Mmap>,
    // file?
}

impl TableCache {
    pub fn new(name: &str) -> Self {
        Self {
            cache: HashMap::new(),
            db_name: name.to_owned(),
        }
    }

    pub fn get(&mut self, key: &Bytes, file_number: u64, size: u64) -> Option<Bytes> {
        let table = self.find_or_create_table(file_number, size);
        table.get(key)
    }

    pub fn find_or_create_table(&mut self, file_number: u64, size: u64) -> &mut table::Table<Mmap> {
        let db_name = &self.db_name;
        &mut self.cache
            .entry(file_number)
            .or_insert_with(|| {
                let name = filename::FileType::Table(db_name, file_number).filename();
                TableAndFile {
                    table: table::open(&name, size),
                }
            })
            .table
    }
}
