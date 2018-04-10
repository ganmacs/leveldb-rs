use slice;

#[derive(Debug)]
pub struct Block {
    inner: slice::Slice,
    size: usize,
    restart_offset: usize,
}

impl Block {
    pub fn new(inner: slice::Slice) -> Self {
        let size = inner.len();
        let mut b = Block {
            size: size,
            inner: inner,
            restart_offset: 0,
        };

        b.restart_offset = if size < slice::U32_BYTE_SIZE {
            panic!("TODO: fix")
        } else {
            size - ((b.restart_count() + 1) * slice::U32_BYTE_SIZE)
        };

        debug!("restart_offset={:?}, size={:?}", b.restart_offset, b.size);
        b
    }

    pub fn restart_count(&self) -> usize {
        self.inner
            .get_u32(self.size - slice::U32_BYTE_SIZE)
            .expect("restart point is not found") as usize
    }

    pub fn iter(&self) -> BlockItertor {
        BlockItertor::new(
            self.inner.clone(),
            self.restart_offset,
            self.restart_count(),
        )
    }
}

pub struct BlockItertor {
    inner: slice::Slice,
    restart_offset: usize,
    restart_num: usize,
    key: Option<slice::Slice>,
    value: Option<slice::Slice>,
}

impl BlockItertor {
    pub fn new(inner: slice::Slice, restart_offset: usize, restart_num: usize) -> Self {
        debug!(
            "inner={:?}, restart_offset={:?}, restart_num={:?}",
            inner, restart_offset, restart_num
        );
        Self {
            inner,
            restart_offset,
            restart_num,
            key: None,
            value: None,
        }
    }

    fn restart_point(&self, idx: usize) -> Option<usize> {
        self.inner
            .get_u32(idx * slice::U32_BYTE_SIZE + self.restart_offset)
            .map(|v| v as usize)
    }

    pub fn seek(&mut self, key: &slice::Slice) -> Option<slice::Slice> {
        let mut left = 0;
        let mut right = self.restart_num - 1;

        while left < right {
            let mid = (left + right) / 2;
            let rpoint = self.restart_point(mid).expect("Invalid restart point");
            let (shared, not_shared, value_length, offset) = decode_block(&self.inner, rpoint);
            let index_key = self.inner.get(offset, not_shared).expect("data!!!");

            debug!(
                "shared={:?},not_shared={:?},value_length={:?},key={:?}",
                shared, not_shared, value_length, index_key
            );

            if &index_key < key {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        let mut offset = self.restart_point(left).expect("invalid restart pont");
        while let Some(next_offset) = self.parse_key(offset) {
            if let Some(k) = self.key.as_ref() {
                if k >= key {
                    break;
                }
                offset = next_offset;
            } else {
                break;
            }
        }

        self.value.clone()
    }

    pub fn parse_key(&mut self, offset: usize) -> Option<usize> {
        let limit = self.restart_offset;
        if limit <= offset {
            return None; // End of data
        }

        let (shared, not_shared, value_length, next_offset) = decode_block(&self.inner, offset);
        let k = self.inner.get(next_offset, not_shared).map(|index_key| {
            if let Some(last_key) = self.key.as_ref().as_mut() {
                let mut k: slice::Slice = last_key.clone();
                k.resize(shared as usize);
                k.put_slice(&index_key);
                k
            } else {
                slice::Slice::from(&index_key)
            }
        });

        let v = self.inner
            .get(next_offset + not_shared, value_length)
            .map(|v| slice::Slice::from(&v));

        if k.is_none() {
            error!("Not found key");
            return None;
        }

        if v.is_none() {
            error!("Not found value");
            return None;
        }

        debug!("key={:?}, value={:?}", k, v);
        self.key = k;
        self.value = v;
        Some(next_offset + not_shared + value_length)
    }
}

// shared, not_shared, value.len()
fn decode_block(slice: &slice::Slice, offset: usize) -> (usize, usize, usize, usize) {
    let shared = slice.get_u32(offset).expect("invalid shared data");
    let not_shared = slice
        .get_u32(offset + slice::U32_BYTE_SIZE)
        .expect("invalid not_shared data");
    let value_length = slice
        .get_u32(offset + slice::U32_BYTE_SIZE * 2)
        .expect("invalid value length data");

    (
        shared as usize,
        not_shared as usize,
        value_length as usize,
        offset + slice::U32_BYTE_SIZE * 3,
    )
}
