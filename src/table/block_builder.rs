use slice::{ByteWrite, Bytes, BytesMut};
use std::cmp;

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
    buff: BytesMut,
    counter: usize,
    restarts: Vec<u32>,
    last_key: Bytes,
    finished: bool,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            buff: BytesMut::with_capacity(1024),
            counter: 0,
            restarts: vec![0],
            last_key: Bytes::new(),
            finished: false,
        }
    }

    pub fn estimated_current_size(&self) -> usize {
        self.buff.len() + (U32_ADDR_SIZE * (1 + self.restarts.len()))
    }

    pub fn add(&mut self, key: &Bytes, value: &Bytes) {
        if self.finished {
            panic!("Adding item to built BlockBuilder")
        }

        let mut shared = 0;

        if self.counter < RESTART_INTERVAL {
            let min_size = cmp::min(key.len(), self.last_key.len());
            for i in 0..min_size {
                if key[i] == self.last_key[i] {
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
        self.buff.write_u32(shared as u32);
        self.buff.write_u32(not_shared as u32);
        self.buff.write_u32(value.len() as u32);
        self.buff.write_slice(&key[shared..key.len()]);
        self.buff.write_slice(value.as_ref());

        self.last_key = key.clone();
    }

    pub fn build(&mut self) -> Bytes {
        let ref r = self.restarts;
        for i in r {
            self.buff.write_u32(*i)
        }
        self.buff.write_u32(r.len() as u32);
        self.finished = true;

        self.buff.clone().freeze()
    }

    pub fn empty(&self) -> bool {
        self.buff.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder() {
        let mut bb = BlockBuilder::new();

        for i in 0..2 {
            bb.add(
                &Bytes::from(format!("key{:?}", i).as_bytes()),
                &Bytes::from("value".as_bytes()),
            );
        }

        let v = bb.build();
        assert_eq!(
            v.as_ref().to_vec(),
            b"\0\0\0\0\x04\0\0\0\x05\0\0\0key0value\x03\0\0\0\x01\0\0\0\x05\0\0\01value\0\0\0\0\x01\0\0\0"
                .to_vec()
        );
    }

    #[test]
    fn test_block_builder2() {
        let mut bb = BlockBuilder::new();

        for i in 0..16 {
            bb.add(
                &Bytes::from(format!("key{:?}", i).as_bytes()),
                &Bytes::from("v".as_bytes()),
            );
        }

        let s = bb.estimated_current_size() - (U32_ADDR_SIZE * (1 + bb.restarts.len()));
        bb.add(
            &Bytes::from("key16".as_bytes()),
            &Bytes::from("v".as_bytes()),
        );
        bb.add(
            &Bytes::from("key17".as_bytes()),
            &Bytes::from("v".as_bytes()),
        );

        let result = bb.build();
        let (_, r2) = result.split_at(s);

        assert_eq!(r2.to_vec(), b"\0\0\0\0\x05\0\0\0\x01\0\0\0key16v\x04\0\0\0\x01\0\0\0\x01\0\0\07v\0\0\0\0\xe4\0\0\0\x02\0\0\0".to_vec());
    }
}
