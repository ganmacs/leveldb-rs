mod version_edit;
mod version_set;
mod metadata;
mod linked_list;

pub use self::version_set::{VersionSet, Version};
pub use self::version_edit::VersionEdit;
pub use self::metadata::{FileMetaDataBuilder, FileMetaData};
use self::linked_list::CircularLinkedList;

const BLOCK_SIZE: usize = 2 << 15; // duplicated
