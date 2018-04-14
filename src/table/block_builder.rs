use std::cmp;
use slice::{sop, Slice2};

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
    buff: Slice2,
    counter: usize,
    restarts: Vec<u32>,
    last_key: Slice2,
    finished: bool,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            buff: Slice2::with_capacity(1024),
            counter: 0,
            restarts: vec![0],
            last_key: Slice2::new(),
            finished: false,
        }
    }

    pub fn estimated_current_size(&self) -> usize {
        self.buff.len() + (U32_ADDR_SIZE * (1 + self.restarts.len()))
    }

    pub fn add(&mut self, key: &Slice2, value: &Slice2) {
        if self.finished {
            panic!("Adding item to built BlockBuilder")
        }

        let mut shared = 0;

        if self.counter < RESTART_INTERVAL {
            let min_size = cmp::min(key.len(), self.last_key.len());
            for i in 0..min_size {
                if sop::get_u8(key, i) == sop::get_u8(&self.last_key, i) {
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
        sop::put_u32(&mut self.buff, shared as u32);
        sop::put_u32(&mut self.buff, not_shared as u32);
        sop::put_u32(&mut self.buff, value.len() as u32);
        sop::put_slice(&mut self.buff, &key[shared..key.len()]);
        sop::put_slice(&mut self.buff, value.as_ref());

        self.last_key = key.clone();
    }

    pub fn build(&mut self) -> Slice2 {
        let ref r = self.restarts;
        for i in r {
            sop::put_u32(&mut self.buff, *i)
        }
        sop::put_u32(&mut self.buff, r.len() as u32);
        self.finished = true;
        self.buff.clone()
    }

    pub fn empty(&self) -> bool {
        self.buff.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use slice::Slice2;
    use super::{BlockBuilder, U32_ADDR_SIZE};

    #[test]
    fn test_block_builder() {
        let mut bb = BlockBuilder::new();

        for i in 0..2 {
            bb.add(
                &Slice2::from(format!("key{:?}", i).as_bytes()),
                &Slice2::from("value".as_bytes()),
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
                &Slice2::from(format!("key{:?}", i).as_bytes()),
                &Slice2::from("v".as_bytes()),
            );
        }

        let s = bb.estimated_current_size() - (U32_ADDR_SIZE);
        bb.add(
            &Slice2::from("key16".as_bytes()),
            &Slice2::from("v".as_bytes()),
        );
        bb.add(
            &Slice2::from("key17".as_bytes()),
            &Slice2::from("v".as_bytes()),
        );

        let mut v = bb.build();
        let (_, v2) = v.split_at(s);

        let vv: Vec<u8> = vec![
            0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 54, 118, 4, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 55, 118, 228, 0, 0, 0, 1, 0, 0, 0,
        ];
        assert_eq!(v2.to_vec(), vv);
    }
}
