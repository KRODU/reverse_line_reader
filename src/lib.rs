mod byte_start_with;

use byte_start_with::ByteStartWith;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct ReverseLineReader<R> {
    reader: R,
    cursor: u64,
    buf_size: usize,
    remain_buf: Option<ByteStartWith>,
}

impl<R> ReverseLineReader<R>
where
    R: AsyncReadExt + AsyncSeekExt + std::marker::Unpin,
{
    pub async fn open_file_with_buffer_size(
        path: impl AsRef<Path>,
        buf_size: usize,
    ) -> io::Result<ReverseLineReader<File>> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;

        let meta = f.metadata().await?;

        Ok(ReverseLineReader {
            reader: f,
            cursor: meta.len(),
            buf_size,
            remain_buf: None,
        })
    }

    pub async fn read_rev_line(&mut self) -> io::Result<Option<BytesMut>> {
        let mut full_line_buffer: VecDeque<BytesMut> = VecDeque::new();

        loop {
            if self.cursor == 0 {
                break;
            }

            let new_cursor = self.cursor.saturating_sub(self.buf_size as u64);
            self.reader.seek(io::SeekFrom::Start(new_cursor)).await?;

            let real_buf_size = self.cursor - new_cursor;
            let mut buffer = BytesMut::with_capacity(real_buf_size as usize);
            self.cursor = new_cursor;

            self.reader.read_buf(&mut buffer).await?;
            let find_result = buffer.iter().enumerate().rev().find(|(_, c)| **c == b'\n');

            let Some((pos, _)) = find_result else {
                full_line_buffer.push_front(buffer);
                continue;
            };
        }

        let line_size = full_line_buffer.iter().fold(0, |acc, v| acc + v.len());
        let mut ret = BytesMut::with_capacity(line_size);

        while let Some(buf) = full_line_buffer.pop_back() {
            ret.extend_from_slice(&buf);
        }

        Ok(Some(ret))
    }
}
