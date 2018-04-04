use std::fs;
use std::io;
use std::io::{BufReader, Seek};
use std::io::Read;

pub struct Table {}

impl Table {
    pub fn open(fname: &str, file_size: u64) -> Self {
        debug!("Open Table file {:?} for read", fname);
        let fd = fs::OpenOptions::new() // add read permission?
            .read(true)
            .open(fname)
            .unwrap();

        let mut reader = BufReader::new(fd);
        reader.seek(io::SeekFrom::Start(file_size - 30));
        let mut v = [0; 30];
        reader.read_exact(&mut v);

        Self {}
    }
}
