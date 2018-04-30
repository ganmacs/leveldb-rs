mod table_builder;
mod table_writer;
mod block;
mod block_builder;
mod format;
mod table_cache;
mod table;

use version::{FileMetaData, FileMetaDataBuilder};
use memdb::MemDBIterator;
use filename;
use self::table_builder::TableBuilder;
use ikey;
use slice::Bytes;

enum Compression {
    No,
}

impl From<u8> for Compression {
    fn from(v: u8) -> Self {
        match v {
            0 => Compression::No,
            _ => unreachable!(),
        }
    }
}

pub fn bulid(
    dbname: &str,
    iterator: &mut MemDBIterator,
    num: u64,
) -> Result<FileMetaData, &'static str> {
    let mut meta_builder = FileMetaDataBuilder::new();
    meta_builder.file_num(num);

    let fname = filename::FileType::Table(dbname, num).filename();
    let mut builder = TableBuilder::new(&fname);
    let mut largest = Bytes::new(); // XXX

    for (i, (k, v)) in iterator.enumerate() {
        if i == 0 {
            meta_builder.smallest(ikey::InternalKey::from(k.clone()));
        }

        largest = k.clone();
        builder.add(&k, &v);
    }

    meta_builder.largest(ikey::InternalKey::from(largest));
    builder.build();

    meta_builder.file_size(builder.size() as u64);
    meta_builder.build()
}

pub use self::table_cache::TableCache;
