use std::collections::HashMap;
use filename;
use super::table::Table;
use slice::Slice;

pub struct TableCache {
    cache: HashMap<u64, TableAndFile>, // TODO: use more smart cache
    db_name: String,
}

pub struct TableAndFile {
    table: Table,
    // file?
}

impl TableCache {
    pub fn new(name: &str) -> Self {
        Self {
            cache: HashMap::new(),
            db_name: name.to_owned(),
        }
    }

    pub fn get(&mut self, key: &Slice, file_number: u64, size: u64) -> Slice {
        let table = self.find_or_create_table(file_number, size);
        table.get(key)
    }

    pub fn find_or_create_table(&mut self, file_number: u64, size: u64) -> &Table {
        let db_name = &self.db_name;
        &self.cache
            .entry(file_number)
            .or_insert_with(|| {
                let name = filename::FileType::Table(db_name, file_number).filename();
                TableAndFile {
                    table: Table::open(&name, size),
                }
            })
            .table
    }
}
