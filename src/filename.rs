use regex;
use std::fs;
use std::io::Write;

pub enum FileType<'a> {
    Log(&'a str, u64),
    Current(&'a str),
    Table(&'a str, u64),
    Manifest(&'a str, usize),
    TempFileName(&'a str, usize),
}

lazy_static!{
    static ref CURRENT_FILE_REGEX: regex::Regex = {
        regex::Regex::new(r"([\w]+)/CURRENT").unwrap()
    };

    static ref LOG_FILE_REGEX: regex::Regex = {
        regex::Regex::new(r"([\w]+)/([\d]{7})\.log").unwrap()
    };

    static ref TABLE_FILE_REGEX: regex::Regex = {
        regex::Regex::new(r"([\w]+)/([\d]{7})\.ldb").unwrap()
    };

    static ref MANIFEST_FILE_REGEX: regex::Regex = {
        regex::Regex::new(r"([\w]+)/MANIFEST-([\d]{7})").unwrap()
    };

    static ref CURRENT_TMP_REGEX: regex::Regex = {
        regex::Regex::new(r"([\w]+)/CURRENT.([\d]{7})").unwrap()
    };
}

pub fn set_current_file(dbname: &str, num: usize) {
    let current_name = FileType::Current(dbname).filename();
    let tmp_name = FileType::TempFileName(dbname, num).filename();

    fs::File::create(&tmp_name)
        .and_then(|mut file| {
            let content = format!("MANIFEST-{:07}", num);
            debug!("Set current manifest {:?} to current file", content);
            file.write_all(content.as_bytes())
        })
        .and_then(|_| fs::rename(&tmp_name, &current_name))
        .expect("Failed to set current file");
}

impl<'a> FileType<'a> {
    pub fn parse_name(filename: &'a str) -> Self {
        if CURRENT_FILE_REGEX.is_match(filename) {
            let v = CURRENT_FILE_REGEX.captures(filename).expect(
                "current file regex",
            );
            let name = v.get(1).map(|v| v.as_str()).expect(
                "current file regex name",
            );
            FileType::Current(name)
        } else if LOG_FILE_REGEX.is_match(filename) {
            if let Some(v) = LOG_FILE_REGEX.captures(filename) {
                let name = v.get(1).map(|v| v.as_str()).expect("log file regex name");
                let num = v.get(2).and_then(|v| v.as_str().parse().ok()).expect(
                    "log file regex num",
                );
                FileType::Log(name, num)
            } else {
                panic!("log file name is invalid")
            }
        } else if TABLE_FILE_REGEX.is_match(filename) {
            if let Some(v) = TABLE_FILE_REGEX.captures(filename) {
                let name = v.get(1).map(|v| v.as_str()).expect("table file regex name");
                let num = v.get(2).and_then(|v| v.as_str().parse().ok()).expect(
                    "log file regex num",
                );
                FileType::Table(name, num)
            } else {
                panic!("table file name is invalid")
            }
        } else if MANIFEST_FILE_REGEX.is_match(filename) {
            if let Some(v) = MANIFEST_FILE_REGEX.captures(filename) {
                let name = v.get(1).map(|v| v.as_str()).expect(
                    "manifest file regex name",
                );
                let num = v.get(2).and_then(|v| v.as_str().parse().ok()).expect(
                    "manifest file regex num",
                );
                FileType::Manifest(name, num)
            } else {
                panic!("manifest file name is invalid")
            }
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
