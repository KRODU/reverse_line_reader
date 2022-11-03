mod bytes_trim;

use bytes::BytesMut;
use bytes_trim::BytesTrim;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct ReverseLineReader<R> {
    reader: R,
    reader_cursor: u64,
    buf_size: usize,
    remain_buf: Option<BytesTrim>,
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
            reader_cursor: meta.len(),
            buf_size,
            remain_buf: None,
        })
    }

    pub async fn read_rev_line(&mut self) -> io::Result<Option<BytesTrim>> {
        let mut full_line_buffer: Vec<BytesTrim> = Vec::new();
        let mut last_buffer: Option<(BytesTrim, usize)> = None;

        loop {
            if self.reader_cursor == 0 {
                break;
            }

            let buffer_viewer: BytesTrim;

            // remain_buf가 남아있는 경우 그것부터 먼저 처리.
            // 남아있지 않은 경우 reader로부터 읽어들임.
            if let Some(remain_buf) = self.remain_buf.take() {
                buffer_viewer = remain_buf;
            } else {
                // 커서 위치에서 buf_size를 뺌. 이 값이 0 미만인 경우 0이 됨.
                let new_cursor = self.reader_cursor.saturating_sub(self.buf_size as u64);
                self.reader.seek(io::SeekFrom::Start(new_cursor)).await?;

                let real_buf_size = self.reader_cursor - new_cursor; // 실제 읽어들여야 할 크기
                let mut buffer = BytesMut::with_capacity(real_buf_size as usize);
                self.reader_cursor = new_cursor;

                self.reader.read_buf(&mut buffer).await?;
                buffer_viewer = BytesTrim::new_with_bytes(buffer);
            }

            let find_result = buffer_viewer
                .iter()
                .enumerate()
                .rev()
                .find(|(_, c)| **c == b'\n');

            // 줄바꿈을 찾은 경우 저장해놓고 break
            // 찾지 못한 경우 현재 버퍼를 full_line_buffer에 저장해놓고 계속 탐색
            if let Some((pos, _)) = find_result {
                last_buffer = Some((buffer_viewer, pos));
                break;
            } else {
                full_line_buffer.push(buffer_viewer);
            }
        }

        // 버퍼 목록에 쌓여있는 데이터 직렬화
        // 먼저 버퍼 사이즈를 계산한 뒤 처리
        let mut line_size = full_line_buffer.iter().fold(0, |acc, v| acc + v.len());
        if let Some((last_buf, pos)) = &last_buffer {
            line_size += last_buf.len() - pos;
        }
        let mut ret = BytesMut::with_capacity(line_size);

        if let Some((mut last_buf, pos)) = last_buffer {
            ret.extend_from_slice(&last_buf[pos..]);
            last_buf.self_slice(..pos);
            self.remain_buf = Some(last_buf);
        }

        for buf in full_line_buffer.iter().rev() {
            ret.extend_from_slice(buf);
        }

        // 반드시 일치할 필요는 없지만 재할당 방지 및 메모리 낭비 방지용
        debug_assert_eq!(line_size, ret.len());

        Ok(Some(BytesTrim::new_with_bytes(ret)))
    }
}
