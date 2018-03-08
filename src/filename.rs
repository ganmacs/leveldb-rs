use regex::Regex;
use std::fs;
use std::fs::File;
use std::io::Write;

pub enum FileType<'a> {
    Log(&'a str, u64),
    Current(&'a str),
    Table(&'a str, u64),
    Manifest(&'a str, usize),
    TempFileName(&'a str, usize),
}

lazy_static!{
    static ref CURRENT_FILE_REGEX: Regex = {
        Regex::new(r"([\w]+)/CURRENT").unwrap()
    };

    static ref LOG_FILE_REGEX: Regex = {
        Regex::new(r"([\w]+)/([\d]{7})\.log").unwrap()
    };

    static ref TABLE_FILE_REGEX: Regex = {
        Regex::new(r"([\w]+)/([\d]{7})\.ldb").unwrap()
    };

    static ref MANIFEST_FILE_REGEX: Regex = {
        Regex::new(r"([\w]+)/MANIFEST-[\d]{7}").unwrap()
    };

    static ref CURRENT_TMP_REGEX: Regex = {
        Regex::new(r"([\w]+)/CURRENT.[\d]{7}").unwrap()
    };
}

pub fn set_current_file(dbname: &str, num: usize) {
    let current_name = FileType::Current(dbname).filename();
    let tmp_name = FileType::TempFileName(dbname, num).filename();

    fs::File::create(&tmp_name)
        .and_then(|mut file| {
            let content = format!("MANIFEST-{:06}", num);
            file.write_all(content.as_bytes())
        })
        .and_then(|_| fs::rename(&tmp_name, &current_name))
        .expect("Failed to set current file");
}

impl<'a> FileType<'a> {
    pub fn parse_name(filename: &'a str) -> Self {
        if CURRENT_FILE_REGEX.is_match(filename) {
            let v = CURRENT_FILE_REGEX.captures(filename).unwrap();
            FileType::Current(v.get(0).unwrap().as_str())
        } else if LOG_FILE_REGEX.is_match(filename) {
            let v = LOG_FILE_REGEX.captures(filename).unwrap();
            let num = v.get(1).unwrap().as_str(); // TODO
            FileType::Log(v.get(0).unwrap().as_str(), 000)
        } else if TABLE_FILE_REGEX.is_match(filename) {
            let v = TABLE_FILE_REGEX.captures(filename).unwrap();
            let num = v.get(1).unwrap().as_str(); // XXX
            FileType::Table(v.get(0).unwrap().as_str(), 000)
        } else if MANIFEST_FILE_REGEX.is_match(filename) {
            let v = MANIFEST_FILE_REGEX.captures(filename).unwrap();
            let num = v.get(1).unwrap().as_str().parse().unwrap();
            FileType::Manifest(v.get(0).unwrap().as_str(), num)
        } else {
            unimplemented!()
        }
    }

    pub fn is_logfile(&self) -> bool {
        match self {
            &FileType::Log(_, _) => true,
            _ => false,
        }

    }

    pub fn filename(&self) -> String {
        match self {
            &FileType::Log(name, num) => format!("{:}/{:07}.log", name, num),
            &FileType::Current(name) => format!("{:}/CURRENT", name),
            &FileType::Table(name, num) => format!("{:}/{:07}.ldb", name, num),
            &FileType::Manifest(name, num) => format!("{:}/MANIFEST-{:07}", name, num),
            &FileType::TempFileName(name, num) => format!("{:}/CURRENT.{:07}", name, num),
        }
    }
}
