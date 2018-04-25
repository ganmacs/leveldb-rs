use std::{cmp, u8};
use bytes::BufMut;
use byteorder::{ByteOrder, LittleEndian};
pub use bytes::{Bytes, BytesMut};

use std::mem;

pub const U64_BYTE_SIZE: usize = mem::size_of::<u64>();
pub const I64_BYTE_SIZE: usize = mem::size_of::<i64>();
pub const U32_BYTE_SIZE: usize = mem::size_of::<u32>();
pub const U16_BYTE_SIZE: usize = mem::size_of::<u16>();
pub const U8_BYTE_SIZE: usize = mem::size_of::<u8>();

pub trait ByteRead {
    fn gets(&self, offset: usize, size: usize) -> Self;
    fn get_u8<'a>(&self, offset: usize) -> u8;
    fn get_u16(&self, offset: usize) -> u16;
    fn get_u32(&self, offset: usize) -> u32;
    fn get_u64(&self, offset: usize) -> u64;
    fn read(&mut self, size: usize) -> Self;
    fn read_u8(&mut self) -> u8;
    fn read_u16(&mut self) -> u16;
    fn read_u32(&mut self) -> u32;
    fn read_u64(&mut self) -> u64;
    fn read_i64(&mut self) -> i64;
}

impl ByteRead for Bytes {
    fn gets(&self, offset: usize, size: usize) -> Self {
        self.slice(offset, offset + size)
    }

    fn get_u8<'a>(&self, offset: usize) -> u8 {
        self[offset]
    }

    fn get_u16(&self, offset: usize) -> u16 {
        let buf = &self[offset..offset + U16_BYTE_SIZE];
        LittleEndian::read_u16(buf)
    }

    fn get_u32(&self, offset: usize) -> u32 {
        let buf = &self[offset..offset + U32_BYTE_SIZE];
        LittleEndian::read_u32(buf)
    }

    fn get_u64(&self, offset: usize) -> u64 {
        let buf = &self[offset..offset + U64_BYTE_SIZE];
        LittleEndian::read_u64(buf)
    }

    fn read(&mut self, size: usize) -> Self {
        self.split_to(size)
    }

    fn read_u8(&mut self) -> u8 {
        self.split_to(U8_BYTE_SIZE)[0]
    }

    fn read_u16(&mut self) -> u16 {
        let buf = &self.split_to(U16_BYTE_SIZE)[0..U16_BYTE_SIZE];
        LittleEndian::read_u16(buf)
    }

    fn read_u32(&mut self) -> u32 {
        let buf = &self.split_to(U32_BYTE_SIZE)[0..U32_BYTE_SIZE];
        LittleEndian::read_u32(buf)
    }

    fn read_u64(&mut self) -> u64 {
        let buf = &self.split_to(U64_BYTE_SIZE)[0..U64_BYTE_SIZE];
        LittleEndian::read_u64(buf)
    }

    fn read_i64(&mut self) -> i64 {
        let buf = &self.split_to(I64_BYTE_SIZE)[0..I64_BYTE_SIZE];
        LittleEndian::read_i64(buf)
    }
}

impl ByteRead for BytesMut {
    fn gets(&self, offset: usize, size: usize) -> Self {
        BytesMut::from(self[offset..offset + size].to_vec()) // XXX
    }

    fn get_u8<'a>(&self, offset: usize) -> u8 {
        self[offset]
    }

    fn get_u16(&self, offset: usize) -> u16 {
        let buf = &self[offset..offset + U16_BYTE_SIZE];
        LittleEndian::read_u16(buf)
    }

    fn get_u32(&self, offset: usize) -> u32 {
        let buf = &self[offset..offset + U32_BYTE_SIZE];
        LittleEndian::read_u32(buf)
    }

    fn get_u64(&self, offset: usize) -> u64 {
        let buf = &self[offset..offset + U64_BYTE_SIZE];
        LittleEndian::read_u64(buf)
    }

    fn read_u8(&mut self) -> u8 {
        self.split_to(U8_BYTE_SIZE)[0]
    }

    fn read_u16(&mut self) -> u16 {
        let buf = &self.split_to(U16_BYTE_SIZE)[0..U16_BYTE_SIZE];
        LittleEndian::read_u16(buf)
    }

    fn read_u32(&mut self) -> u32 {
        let buf = &self.split_to(U32_BYTE_SIZE)[0..U32_BYTE_SIZE];
        LittleEndian::read_u32(buf)
    }

    fn read_u64(&mut self) -> u64 {
        let buf = &self.split_to(U64_BYTE_SIZE)[0..U64_BYTE_SIZE];
        LittleEndian::read_u64(buf)
    }

    fn read_i64(&mut self) -> i64 {
        let buf = &self.split_to(I64_BYTE_SIZE)[0..I64_BYTE_SIZE];
        LittleEndian::read_i64(buf)
    }

    fn read(&mut self, size: usize) -> Self {
        self.split_to(size)
    }
}

pub trait ByteWrite {
    fn write_u8(&mut self, n: u8);
    fn write_u16(&mut self, n: u16);
    fn write_u32(&mut self, n: u32);
    fn write_u64(&mut self, n: u64);
    fn write_i64(&mut self, n: i64);
    fn write(&mut self, n: &Bytes);
    fn write_slice(&mut self, n: &[u8]);
}

impl ByteWrite for BytesMut {
    fn write_u8(&mut self, n: u8) {
        self.put_u8(n);
    }

    fn write_u16(&mut self, n: u16) {
        self.put_u16_le(n);
    }

    fn write_u32(&mut self, n: u32) {
        self.put_u32_le(n);
    }

    fn write_u64(&mut self, n: u64) {
        self.put_u64_le(n);
    }

    fn write_i64(&mut self, n: i64) {
        self.put_i64_le(n);
    }

    fn write(&mut self, n: &Bytes) {
        self.extend(n);
    }

    fn write_slice(&mut self, n: &[u8]) {
        self.extend_from_slice(n);
    }
}

pub fn short_successor(val: &mut BytesMut) {
    let l = val.len();

    for i in 0..l {
        let k = val[i];
        if k != u8::MAX {
            val[i] += 1;
            return;
        }
    }
}

pub fn shortest_separator(key: &mut BytesMut, limit: &Bytes) {
    let min_size = cmp::min(key.len(), limit.len());

    for i in 0..min_size {
        if key[i] != limit[i] {
            let val = key[i];
            if val != u8::MAX {
                key[i] += 1;
                return;
            }
        }
    }
}
