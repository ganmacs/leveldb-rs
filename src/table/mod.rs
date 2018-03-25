mod builder;
mod table_writer;
mod block_builder;

use version::{FileMetaDataBuilder, FileMetaData};
use memdb::MemDBIterator;
use filename;
use self::builder::TableBuilder;

enum Compression {
    No,
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

    for (i, (k, v)) in iterator.enumerate() {
        if i == 0 {
            meta_builder.smallest(k.clone());
        }

        builder.add(&k, &v);
        meta_builder.largest(k); // must clone
    }

    builder.build();

    meta_builder.file_size(builder.size() as u64);
    meta_builder.build()
}
