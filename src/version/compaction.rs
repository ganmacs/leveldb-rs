use super::FileMetaData;

const LEVEL: usize = 12;

pub struct Compaction {
    level: usize,
    pub inputs: Vec<Vec<FileMetaData>>,
}

impl Compaction {
    pub fn new(level: usize) -> Self {
        Self {
            level: level,
            inputs: vec![Vec::new(); LEVEL],
        }
    }
}
