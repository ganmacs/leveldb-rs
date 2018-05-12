use std::sync::Arc;

use super::format::{Footer, FOOTER_MAX_LENGTH};
use super::{block, format};
use super::block::{Block, BlockIterator};
use slice::Bytes;
use random_access_file::RandomAccessFile;

pub struct Table<T> {
    index_block: Block,
    inner: Arc<T>,
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

impl<T: RandomAccessFile> Table<T> {
    pub fn open(size: usize, inner: T) -> Self {
        if FOOTER_MAX_LENGTH > size {
            error!("Size is too samll {:?} for footer", size);
        }

        let footer = inner
            .read(size - FOOTER_MAX_LENGTH, FOOTER_MAX_LENGTH)
            .map(|v| Footer::decode(v))
            .unwrap();

        debug!(
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

pub struct TableIterator<T> {
    index_block: BlockIterator,
    data_block: Option<BlockIterator>,
    inner: Arc<T>,
}

impl<T: RandomAccessFile> Iterator for TableIterator<T> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.data_block
            .as_mut()
            .and_then(|dblock| dblock.next())
            .or_else(|| {
                self.index_block.next().and_then(|(_, index_value)| {
                    self.data_block = Some(block::read2(&*self.inner, &index_value).iter());
                    self.data_block.as_mut().and_then(|block| block.next())
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::table_builder::TableBuilder;
    use random_access_file::RandomAccessFile;
    use std::io::{BufWriter, Cursor};
    use bytes::Bytes;

    fn built_table_value() -> (Vec<u8>, Vec<(Bytes, Bytes)>) {
        let mut value: Vec<u8> = vec![];
        let dic: Vec<(Bytes, Bytes)> = (0..30)
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

    struct TestRandomAccessFile {
        inner: Vec<u8>,
    }

    impl RandomAccessFile for TestRandomAccessFile {
        fn open(fname: &str) -> Self {
            let (v, _) = built_table_value();
            TestRandomAccessFile { inner: v }
        }

        fn read(&self, offset: usize, size: usize) -> Result<&[u8], String> {
            let lim = offset + size;
            if lim > self.inner.len() {
                Err("invalid index".to_owned())
            } else {
                Ok(&self.inner[offset..lim])
            }
        }
    }

    #[test]
    fn test_table() {
        let (value, dic) = built_table_value();
        let t = Table::open(value.len(), TestRandomAccessFile::open("dummy"));

        for (k, v) in dic {
            assert_eq!(Some(v), t.get(&k))
        }
    }

    #[test]
    fn test_table_iter() {
        let (value, dic) = built_table_value();
        let mut titer = Table::open(value.len(), TestRandomAccessFile::open("dummy")).iter();

        for exp in dic {
            let t = titer.next().unwrap();
            assert_eq!(exp, t);
        }
    }
}
