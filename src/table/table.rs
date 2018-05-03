use std::fs;
use std::sync::Arc;
use std::ops::Deref;
use memmap::{Mmap, MmapOptions};

use super::format::{Footer, FOOTER_MAX_LENGTH};
use super::{block, format};
use super::block::{Block, BlockIterator};
use slice::Bytes;

pub struct Table<T> {
    index_block: Block,
    inner: Arc<T>,
}

pub fn open(fname: &str, file_size: u64) -> Table<Mmap> {
    debug!("Open Table file {:?} for read", fname);
    let file = fs::OpenOptions::new().read(true).open(fname).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).expect("memmap failed") };
    Table::open(file_size as usize, mmap)
}

impl<T> Table<T> {
    pub fn iter(&self) -> TableIterator<T> {
        TableIterator {
            index_block: self.index_block.iter(),
            data_block: None,
            inner: self.inner.clone(),
        }
    }
}

impl<T: Deref<Target = [u8]>> Table<T> {
    pub fn open(size: usize, inner: T) -> Self {
        if FOOTER_MAX_LENGTH > size {
            error!("Size is too samll {:?} for footer", size);
        }

        let offset = size - FOOTER_MAX_LENGTH;
        let footer = Footer::decode(&inner[offset..offset + FOOTER_MAX_LENGTH]);
        println!(
            "Read footer data index_block(offset={:?}, size={:?}), metaindex(offset={:?}, size={:?})",
            footer.index_block_handle.offset(),
            footer.index_block_handle.size(),
            footer.metaindex_block_handle.offset(),
            footer.metaindex_block_handle.size()
        );

        let index_block =
            format::read_block2(&inner, &footer.index_block_handle).expect("block need");

        Self {
            index_block: index_block,
            inner: Arc::new(inner),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        if let Some(ref index_value) = self.index_block.iter().seek(key).as_ref() {
            let mut b = block::read2(&*self.inner, index_value);
            b.iter().seek(key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::table_builder::TableBuilder;
    use std::io::{BufWriter, Cursor};
    use bytes::Bytes;

    fn built_table_value(size: usize) -> (Vec<u8>, Vec<(Bytes, Bytes)>) {
        let mut value: Vec<u8> = vec![];
        let dic: Vec<(Bytes, Bytes)> = (0..size)
            .into_iter()
            .map(|v| {
                (
                    Bytes::from(format!("key{:02?}", v).as_bytes()),
                    Bytes::from(format!("value{:02?}", v).as_bytes()),
                )
            })
            .collect();

        {
            let mut b = TableBuilder::new(BufWriter::new(Cursor::new(&mut value)));

            for &(ref k, ref v) in &dic {
                b.add(k, v)
            }

            b.build();
        }
        (value, dic)
    }

    #[test]
    fn test_table() {
        let (value, dic) = built_table_value(30);
        let t = Table::open(value.len(), value);

        for (k, v) in dic {
            assert_eq!(Some(v), t.get(&k))
        }
    }
}
