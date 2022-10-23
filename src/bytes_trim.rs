use bytes::BytesMut;
use ouroboros::self_referencing;
use std::rc::Rc;
use std::slice::SliceIndex;

#[self_referencing]
pub struct BytesTrim {
    bytes: Rc<BytesMut>,
    #[borrows(bytes)]
    bytes_view: &'this [u8],
}

impl BytesTrim {
    /// BytesMut를 사용하여 ByteTrimWith 초기화.
    pub fn new_with_bytes(bytes: BytesMut) -> Self {
        BytesTrim::new(Rc::new(bytes), |bytes| bytes)
    }

    /// BytesMut를 사용하여 ByteTrimWith 초기화. 슬라이스를 사용하여 뷰어로 사용
    pub fn new_with_slice<I>(bytes: BytesMut, index: I) -> BytesTrim
    where
        I: SliceIndex<[u8], Output = [u8]>,
    {
        BytesTrim::new(Rc::new(bytes), |bytes| &bytes[index])
    }

    /// self로부터 슬라이싱하여 새로운 뷰어를 생성.
    pub fn slice<I>(&self, index: I) -> BytesTrim
    where
        I: SliceIndex<[u8], Output = [u8]>,
    {
        BytesTrim::new(self.borrow_bytes().clone(), |bytes| &bytes[index])
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

impl std::ops::Deref for BytesTrim {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
