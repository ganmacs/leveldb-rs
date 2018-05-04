use std::fs;
use memmap::{Mmap, MmapOptions};

pub trait RandomAccessFile {
    fn open(fname: &str) -> Self;
    fn read(&self, offset: usize, size: usize) -> Result<&[u8], String>;
}

pub struct MmapRandomAccessFile {
    inner: Mmap,
}

impl RandomAccessFile for MmapRandomAccessFile {
    fn open(fname: &str) -> Self {
        let file = fs::File::open(fname).expect(&format!("fail to open file: {:?}", fname));
        let inner = unsafe {
            MmapOptions::new()
                .map(&file)
                .expect("failed to map when mmap")
        };
        MmapRandomAccessFile { inner }
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

// pub struct BufRandomAccessFile {
//     // Use RefCell to achive inner mutability
//     inner: cell::RefCell<io::BufReader<fs::File>>,
// }

// impl RandomAccessFile for BufRandomAccessFile {
//     fn open(fname: &str) -> Self {
//         let file = fs::File::open(fname).expect(&format!("fail to open file: {:?}", fname));
//         let inner = cell::RefCell::new(io::BufReader::new(file));
//         BufRandomAccessFile { inner }
//     }

//     fn read(&self, offset: usize, size: usize) -> Result<&[u8], String> {
//         use std::io::{Read, Seek};
//         let mut reader = self.inner.borrow_mut();
//         let mut ret: Vec<u8> = vec![0; size];
//         reader
//             .seek(io::SeekFrom::Start(offset as u64))
//             .map(|_| {
//                 reader.read(&mut ret);
//                 ret.as_ref()
//             })
//             .map_err(|_| "failed to seeking file".to_owned())
//     }
// }
