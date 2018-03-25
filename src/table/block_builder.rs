use std::cmp;
use bytes::{BytesMut, Bytes, LittleEndian, BufMut};

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
            restarts: vec![],
            last_key: Bytes::new(),
            finished: false,
        }
    }

    pub fn current_size_estimate(&self) -> usize {
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
        self.buff.put_u32::<LittleEndian>(shared as u32);
        self.buff.put_u32::<LittleEndian>(not_shared as u32);
        self.buff.put_u32::<LittleEndian>(value.len() as u32);
        self.buff.extend_from_slice(key[shared..key.len()].as_ref());
        self.buff.extend_from_slice(value);

        self.last_key = key.to_owned();
    }

    pub fn build(&mut self) -> Bytes {
        let ref r = self.restarts;
        for i in r {
            self.buff.put_u32::<LittleEndian>(*i)
        }
        self.buff.put_u32::<LittleEndian>(r.len() as u32);
        self.finished = true;
        Bytes::from(self.buff.as_ref())
    }

    pub fn empty(&self) -> bool {
        self.buff.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockBuilder, U32_ADDR_SIZE, Bytes};

    #[test]
    fn test_block_builder() {
        let mut bb = BlockBuilder::new();

        for i in 0..2 {
            bb.add(&Bytes::from(format!("key{:?}", i)), &Bytes::from("value"));
        }

        let v = bb.build();
        assert_eq!(
            &v,
            "\0\0\0\0\x04\0\0\0\x05\0\0\0key0value\x03\0\0\0\x01\0\0\0\x05\0\0\01value\0\0\0\0"
        );
    }

    #[test]
    fn test_block_builder2() {
        let mut bb = BlockBuilder::new();

        for i in 0..16 {
            bb.add(&Bytes::from(format!("key{:?}", i)), &Bytes::from("v"));
        }

        let s = bb.current_size_estimate() - (U32_ADDR_SIZE);
        bb.add(&Bytes::from("key16"), &Bytes::from("v"));
        bb.add(&Bytes::from("key17"), &Bytes::from("v"));

        let v = bb.build().slice_from(s);
        let vv: Vec<u8> = vec![
            0,
            0,
            0,
            0,
            5,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            107,
            101,
            121,
            49,
            54,
            118,
            4,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            55,
            118,
            228,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
        ];
        assert_eq!(v.as_ref(), vv.as_ref() as &[u8]);
    }
}
