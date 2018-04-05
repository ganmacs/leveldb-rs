use std::fs;
use std::io;
use std::io::{BufReader, Seek};
use std::io::Read;

use super::block_format::{Footer, FOOTER_MAX_LENGTH};

pub struct Table {}

impl Table {
    pub fn open(fname: &str, file_size: u64) -> Self {
        debug!("Open Table file {:?} for read", fname);
        let fd = fs::OpenOptions::new() // add read permission?
            .read(true)
            .open(fname)
            .unwrap();

        if (FOOTER_MAX_LENGTH as u64) > file_size {
            error!("file size is too samll {:?}", file_size);
        }

        let mut reader = BufReader::new(fd);

        let offset = file_size - FOOTER_MAX_LENGTH as u64;
        reader.seek(io::SeekFrom::Start(offset));
        let mut _footer = [0; FOOTER_MAX_LENGTH];
        reader
            .read_exact(&mut _footer)
            .expect(&format!("Failed to read footer from {:?}", fname));
        let footer = Footer::decode(&_footer);

        Self {}
    }
}
