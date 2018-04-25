pub struct Configure {
    pub write_buffer_size: usize,
}

impl Default for Configure {
    fn default() -> Configure {
        Configure {
            write_buffer_size: 1000,
        }
    }
}
