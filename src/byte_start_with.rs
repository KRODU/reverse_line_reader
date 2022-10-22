use bytes::BytesMut;

pub struct ByteStartWith {
    bytes: BytesMut,
    start_with: usize,
}

impl ByteStartWith {
    pub fn new(bytes: BytesMut, start_with: usize) -> Self {
        Self { bytes, start_with }
    }
}

impl AsRef<[u8]> for ByteStartWith {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.bytes[self.start_with..]
    }
}
