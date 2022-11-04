use bytes::BytesMut;
use ouroboros::self_referencing;
use std::ops::Deref;
use std::slice::SliceIndex;

#[self_referencing]
pub struct BytesTrim {
    bytes: BytesMut,
    #[borrows(bytes)]
    bytes_view: &'this [u8],
}

impl BytesTrim {
    /// BytesMut를 사용하여 ByteTrimWith 초기화.
    pub fn new_with_bytes(bytes: BytesMut) -> Self {
        BytesTrim::new(bytes, |bytes| bytes)
    }

    /// BytesMut를 사용하여 ByteTrimWith 초기화. 슬라이스를 사용하여 뷰어로 사용
    pub fn new_with_slice<I>(bytes: BytesMut, index: I) -> BytesTrim
    where
        I: SliceIndex<[u8], Output = [u8]>,
    {
        BytesTrim::new(bytes, |bytes| &bytes[index])
    }

    pub fn self_slice<I>(&mut self, index: I)
    where
        I: SliceIndex<[u8], Output = [u8]>,
    {
        self.with_bytes_view_mut(|view| *view = &view[index]);
    }
}

impl AsRef<[u8]> for BytesTrim {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.borrow_bytes_view()
    }
}

impl<'a> From<&'a BytesTrim> for &'a [u8] {
    #[inline]
    fn from(s: &'a BytesTrim) -> Self {
        s.as_ref()
    }
}

impl Deref for BytesTrim {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl Default for BytesTrim {
    fn default() -> Self {
        BytesTrim::new(Default::default(), |bytes| bytes)
    }
}

impl PartialEq for BytesTrim {
    fn eq(&self, other: &Self) -> bool {
        self.borrow_bytes_view() == other.borrow_bytes_view()
    }
}

impl Eq for BytesTrim {}

impl PartialEq<&[u8]> for BytesTrim {
    fn eq(&self, other: &&[u8]) -> bool {
        self.borrow_bytes_view() == other
    }
}

impl std::fmt::Debug for BytesTrim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesTrim")
            .field("bytes_view", self.borrow_bytes_view())
            .field(
                "bytes_view_str",
                &String::from_utf8_lossy(self.borrow_bytes_view()),
            )
            .finish()
    }
}
