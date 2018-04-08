use std::cmp;
use slice::Slice;

// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

const RESTART_INTERVAL: usize = 16; // TODO: set by user
const U32_ADDR_SIZE: usize = 4;

pub struct BlockBuilder {
    buff: Slice,
    counter: usize,
    restarts: Vec<u32>,
    last_key: Slice,
    finished: bool,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            buff: Slice::with_capacity(1024),
            counter: 0,
            restarts: vec![],
            last_key: Slice::new(),
            finished: false,
        }
    }

    pub fn estimated_current_size(&self) -> usize {
        self.buff.len() + (U32_ADDR_SIZE * (1 + self.restarts.len()))
    }

    pub fn add(&mut self, key: &Slice, value: &Slice) {
        if self.finished {
            panic!("Adding item to built BlockBuilder")
        }

        let mut shared = 0;

        if self.counter < RESTART_INTERVAL {
            let min_size = cmp::min(key.len(), self.last_key.len());
            for i in 0..min_size {
                if key.get_u8(i) == self.last_key.get_u8(i) {
                    shared = i + 1;
                } else {
                    break;
                }
            }
            self.counter += 1;
        } else {
            self.counter = 0;
            self.restarts.push(self.buff.len() as u32)
        }

        let not_shared = key.len() - shared;
        self.buff.put_u32(shared as u32);
        self.buff.put_u32(not_shared as u32);
        self.buff.put_u32(value.len() as u32);
        self.buff.put_slice(&key[shared..key.len()]);
        self.buff.put(value);

        self.last_key = key.clone();
    }

    pub fn build(&mut self) -> Slice {
        let ref r = self.restarts;
        for i in r {
            self.buff.put_u32(*i);
        }
        self.buff.put_u32(r.len() as u32);
        self.finished = true;
        self.buff.clone()
    }

    pub fn empty(&self) -> bool {
        self.buff.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use slice::Slice;
    use super::{BlockBuilder, U32_ADDR_SIZE};

    #[test]
    fn test_block_builder() {
        let mut bb = BlockBuilder::new();

        for i in 0..2 {
            bb.add(
                &Slice::from(format!("key{:?}", i).as_bytes()),
                &Slice::from(b"value"),
            );
        }

        let v = bb.build();
        assert_eq!(
            v.as_ref().to_vec(),
            b"\0\0\0\0\x04\0\0\0\x05\0\0\0key0value\x03\0\0\0\x01\0\0\0\x05\0\0\01value\0\0\0\0"
                .to_vec()
        );
    }

    #[test]
    fn test_block_builder2() {
        let mut bb = BlockBuilder::new();

        for i in 0..16 {
            bb.add(
                &Slice::from(format!("key{:?}", i).as_bytes()),
                &Slice::from(b"v"),
            );
        }

        let s = bb.estimated_current_size() - (U32_ADDR_SIZE);
        bb.add(&Slice::from(b"key16"), &Slice::from(b"v"));
        bb.add(&Slice::from(b"key17"), &Slice::from(b"v"));

        let mut v = bb.build();
        v.split_on(s);

        let vv: Vec<u8> = vec![
            0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 54, 118, 4, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 55, 118, 228, 0, 0, 0, 1, 0, 0, 0,
        ];
        assert_eq!(v.as_ref().to_vec(), vv);
    }
}
