use std::ptr;
use bytes::{BufMut, ByteOrder, Bytes, LittleEndian};

const U64_BYTE_SIZE: usize = 8;
const U32_BYTE_SIZE: usize = 4;
const U16_BYTE_SIZE: usize = 2;
const U8_BYTE_SIZE: usize = 1;

struct ImmSlice {
    inner: Vec<u8>,
}

impl ImmSlice {
    pub fn with_capacity(size: usize) -> Self {
        Self {
            inner: Vec::with_capacity(size),
        }
    }

    pub fn from(inner: &[u8]) -> Self {
        Self {
            inner: Vec::from(inner),
        }
    }

    pub fn get_u8(&self, offset: usize) -> Option<u8> {
        self.inner.get(offset).map(|v| *v)
    }

    pub fn get_u16(&self, offset: usize) -> Option<u16> {
        let lim = offset + U16_BYTE_SIZE;
        if self.inner.len() > lim {
            let mut buf = [0; U16_BYTE_SIZE];
            buf.copy_from_slice(&self.inner[offset..lim]);
            Some(LittleEndian::read_u16(&buf))
        } else {
            None
        }
    }

    pub fn get_u32(&self, offset: usize) -> Option<u32> {
        let lim = offset + U32_BYTE_SIZE;
        if self.inner.len() > lim {
            let mut buf = [0; U32_BYTE_SIZE];
            buf.copy_from_slice(&self.inner[offset..lim]);
            Some(LittleEndian::read_u32(&buf))
        } else {
            None
        }
    }

    pub fn get_u64(&self, offset: usize) -> Option<u64> {
        let lim = offset + U64_BYTE_SIZE;
        if self.inner.len() > lim {
            let mut buf = [0; U64_BYTE_SIZE];
            buf.copy_from_slice(&self.inner[offset..lim]);
            Some(LittleEndian::read_u64(&buf))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct SliceMut {
    inner: Vec<u8>,
}

impl SliceMut {
    pub fn with_capacity(size: usize) -> Self {
        Self {
            inner: Vec::with_capacity(size),
        }
    }

    pub fn from(inner: &[u8]) -> Self {
        Self {
            inner: Vec::from(inner),
        }
    }

    // TODO: delete
    pub fn from_bytes(bytes: &Bytes) -> Self {
        Self {
            inner: Vec::from(bytes.as_ref()),
        }
    }

    // TODO: delete
    pub fn to_bytes(self) -> Bytes {
        Bytes::from(self.inner)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn put_u8(&mut self, n: u8) -> usize {
        self.inner.put_u8(n);
        1
    }

    pub fn put_u16(&mut self, n: u16) -> usize {
        self.inner.put_u16::<LittleEndian>(n);
        U16_BYTE_SIZE
    }

    pub fn put_u32(&mut self, n: u32) -> usize {
        self.inner.put_u32::<LittleEndian>(n);
        U32_BYTE_SIZE
    }

    pub fn put_u64(&mut self, n: u64) -> usize {
        self.inner.put_u64::<LittleEndian>(n);
        U64_BYTE_SIZE
    }

    pub fn put_i64(&mut self, n: i64) -> usize {
        self.inner.put_i64::<LittleEndian>(n);
        U64_BYTE_SIZE + 1 // XXX
    }

    pub fn put_str(&mut self, n: &str) -> usize {
        let s = n.len();
        self.inner.put_slice(n.as_bytes());
        s
    }

    pub fn resize(&mut self, size: usize) {
        self.inner.resize(size, 0)
    }

    pub fn put(&mut self, n: &Self) -> usize {
        let s = n.len();
        self.inner.put(n.inner.clone());
        s
    }

    pub fn put_slice(&mut self, n: &[u8]) -> usize {
        let s = n.len();
        self.inner.put(n);
        s
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        if self.inner.len() >= U8_BYTE_SIZE {
            let buf = self.split_off(U8_BYTE_SIZE);
            Some(buf[0])
        } else {
            None
        }
    }

    pub fn read_u16(&mut self) -> Option<u16> {
        if self.inner.len() >= U16_BYTE_SIZE {
            let buf = self.split_off(U16_BYTE_SIZE);
            Some(LittleEndian::read_u16(&buf))
        } else {
            None
        }
    }

    pub fn read_u32(&mut self) -> Option<u32> {
        let s = self.inner.len();
        if s >= U32_BYTE_SIZE {
            let buf = self.split_off(U32_BYTE_SIZE);
            Some(LittleEndian::read_u32(&buf))
        } else {
            None
        }
    }

    pub fn read_u64(&mut self) -> Option<u64> {
        let s = self.inner.len();
        if s >= U64_BYTE_SIZE {
            let buf = self.split_off(U64_BYTE_SIZE);
            Some(LittleEndian::read_u64(&buf))
        } else {
            None
        }
    }

    fn split_off(&mut self, at: usize) -> Vec<u8> {
        assert!(at <= self.inner.len(), "`at` out of bounds");

        let other_len = self.inner.len() - at;
        let mut other = Vec::with_capacity(at);

        unsafe {
            other.set_len(at);
            ptr::copy_nonoverlapping(self.inner.as_ptr(), other.as_mut_ptr(), other.len());

            ptr::copy_nonoverlapping(
                self.inner.as_ptr().offset(at as isize),
                self.inner.as_mut_ptr(),
                other_len,
            );
            self.inner.set_len(other_len);
        }
        other
    }
}

impl AsRef<[u8]> for SliceMut {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::SliceMut;

    #[test]
    fn write_full_record() {
        let mut slice = SliceMut::with_capacity(100);
        slice.put_u8(1);
        assert_eq!(slice.len(), 1);
        slice.put_u16(2);
        assert_eq!(slice.len(), 1 + 2);

        slice.put_u32(3);
        assert_eq!(slice.len(), 1 + 2 + 4);
        slice.put_u64(4);
        assert_eq!(slice.len(), 1 + 2 + 4 + 8);

        assert_eq!(slice.read_u8(), Some(1));
        assert_eq!(slice.len(), 2 + 4 + 8);
        assert_eq!(slice.read_u16(), Some(2));
        assert_eq!(slice.len(), 4 + 8);
        assert_eq!(slice.read_u32(), Some(3));
        assert_eq!(slice.len(), 8);
        assert_eq!(slice.read_u64(), Some(4));
        assert_eq!(slice.len(), 0);
    }

}
