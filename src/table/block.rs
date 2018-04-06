use slice::Slice;

pub struct Block {
    inner: Slice,
}

impl Block {
    pub fn new(inner: Slice) -> Self {
        Block { inner }
    }
}
