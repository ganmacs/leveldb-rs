use std::collections::HashMap;
use bytes::Bytes;

use filename;
use super::table::{Table, TableIterator};
use random_access_file::RandomAccessFile;

pub struct TableCache<T> {
    cache: HashMap<u64, TableAndFile<T>>, // TODO: use more smart cache
    db_name: String,
}

pub struct TableAndFile<T> {
    table: Table<T>,
    // file?
}

impl<T> TableCache<T> {
    pub fn new(name: &str) -> Self {
        Self {
            cache: HashMap::new(),
            db_name: name.to_owned(),
        }
    }
}

impl<T: RandomAccessFile> TableCache<T> {
    pub fn find_or_create_table(&mut self, file_number: u64, size: usize) -> &mut Table<T> {
        let db_name = self.db_name.clone();
        &mut self.cache
            .entry(file_number)
            .or_insert_with(|| {
                let name = filename::FileType::Table(&db_name, file_number).filename();
                TableAndFile {
                    table: Table::open(size, T::open(&name)),
                }
            })
            .table
    }

    pub fn find_table(&self, file_number: u64) -> Option<&Table<T>> {
        let db_name = &self.db_name;
        self.cache.get(&file_number).map(|v| &v.table)
    }

    pub fn get(&mut self, key: &Bytes, file_number: u64, size: u64) -> Option<Bytes> {
        let table = self.find_or_create_table(file_number, size as usize);
        table.get(key)
    }
}

pub struct TableCacheIter<T> {
    inner: TableIterator<T>,
}

// impl Iterator for TableCacheIter {
//     type Item = Bytes;

//     fn next(&mut self) -> Option<Self::Item> {

//         // self.read_record()
//     }
// }
