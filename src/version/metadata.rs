use ikey::InternalKey;
use std::cmp::Ordering;

pub struct FileMetaDataBuilder {
    file_num: Option<u64>,
    file_size: Option<u64>,
    largest: Option<InternalKey>,
    smallest: Option<InternalKey>,
}

impl FileMetaDataBuilder {
    pub fn new() -> Self {
        FileMetaDataBuilder {
            file_num: None,
            file_size: None,
            largest: None,
            smallest: None,
        }
    }

    pub fn file_num(&mut self, num: u64) -> &Self {
        self.file_num = Some(num);
        self
    }

    pub fn file_size(&mut self, size: u64) -> &Self {
        self.file_size = Some(size);
        self
    }

    pub fn largest(&mut self, largest: InternalKey) -> &Self {
        self.largest = Some(largest);
        self
    }

    pub fn smallest(&mut self, smallest: InternalKey) -> &Self {
        self.smallest = Some(smallest);
        self
    }

    pub fn build(self) -> Result<FileMetaData, &'static str> {
        if self.file_num.is_none() {
            return Err("file num must be set");
        }

        if self.file_size.is_none() {
            return Err("size must be set");
        }

        if self.largest.is_none() {
            return Err("largest must be set");
        }

        if self.smallest.is_none() {
            return Err("smallest must be set");
        }

        Ok(FileMetaData {
            file_num: self.file_num.unwrap(),
            file_size: self.file_size.unwrap(),
            largest: self.largest.unwrap(),
            smallest: self.smallest.unwrap(),
        })
    }
}

#[derive(Clone, Eq, Ord, Debug)]
pub struct FileMetaData {
    pub file_num: u64,
    pub file_size: u64,
    largest: InternalKey,
    smallest: InternalKey,
}

impl FileMetaData {
    pub fn largest(&self) -> InternalKey {
        self.largest.clone()
    }

    pub fn smallest(&self) -> InternalKey {
        self.smallest.clone()
    }
}

impl PartialOrd for FileMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.file_num.cmp(&other.file_num))
    }
}

impl PartialEq for FileMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.file_num == other.file_num
    }
}
