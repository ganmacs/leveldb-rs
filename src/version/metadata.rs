use ikey::InternalKey2;
use std::cmp::Ordering;

pub struct FileMetaDataBuilder {
    file_num: Option<u64>,
    size: Option<u64>,
    largest: Option<InternalKey2>,
    smallest: Option<InternalKey2>,
}

impl FileMetaDataBuilder {
    pub fn new() -> Self {
        FileMetaDataBuilder {
            file_num: None,
            size: None,
            largest: None,
            smallest: None,
        }
    }

    pub fn file_num(&mut self, num: u64) -> &Self {
        self.file_num = Some(num);
        self
    }

    pub fn size(&mut self, size: u64) -> &Self {
        self.size = Some(size);
        self
    }

    pub fn largest(&mut self, largest: InternalKey2) -> &Self {
        self.largest = Some(largest);
        self
    }

    pub fn smallest(&mut self, smallest: InternalKey2) -> &Self {
        self.smallest = Some(smallest);
        self
    }

    pub fn build(self) -> Result<FileMetaData, &'static str> {
        if self.file_num.is_none() {
            return Err("file num must be set");
        }

        if self.size.is_none() {
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
            size: self.size.unwrap(),
            largest: self.largest.unwrap(),
            smallest: self.smallest.unwrap(),
        })
    }
}

#[derive(Clone, Eq, Ord, Debug)]
pub struct FileMetaData {
    pub file_num: u64,
    size: u64,
    largest: InternalKey2,
    smallest: InternalKey2,
}

impl FileMetaData {
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn largest(&self) -> InternalKey2 {
        self.largest.clone()
    }

    pub fn smallest(&self) -> InternalKey2 {
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
