mod bytes_trim;

use bytes::BytesMut;
use bytes_trim::BytesTrim;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub struct ReverseLineReader<R>
where
    R: AsyncReadExt + AsyncSeekExt + std::marker::Unpin,
{
    reader: R,
    reader_cursor: u64,
    buf_size: NonZeroUsize,
    remain_buf: Option<(BytesTrim, usize)>,
}

impl<R> ReverseLineReader<R>
where
    R: AsyncReadExt + AsyncSeekExt + std::marker::Unpin,
{
    pub async fn read_next_rev_line(&mut self) -> Option<io::Result<BytesTrim>> {
        match self.read_next_rev_line_in().await {
            Ok(ok) => ok.map(Ok),
            Err(err) => Some(Err(err)),
        }
    }

    async fn read_next_rev_line_in(&mut self) -> io::Result<Option<BytesTrim>> {
        let mut full_line_buffer: Vec<BytesTrim> = Vec::new();
        let mut last_buffer: Option<(BytesTrim, usize)> = None;

        loop {
            // 더 이상 읽을 내용이 없는 경우 break
            if self.reader_cursor == 0 && self.remain_buf.is_none() {
                break;
            }

            let buffer_viewer: BytesTrim;

            // remain_buf가 남아있는 경우 그것부터 먼저 처리.
            // 남아있지 않은 경우 reader로부터 읽어들임.
            if let Some((remain_buf, _)) = self.remain_buf.take() {
                buffer_viewer = remain_buf;
            } else {
                // 커서 위치에서 buf_size를 뺌. 이 값이 0 미만인 경우 0이 됨.
                let new_cursor = self
                    .reader_cursor
                    .saturating_sub(self.buf_size.get() as u64);
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

        if full_line_buffer.is_empty() && last_buffer.is_none() {
            return Ok(None);
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
            self.remain_buf = Some((last_buf, pos));
        }

        while let Some(buf) = full_line_buffer.pop() {
            ret.extend_from_slice(&buf);
        }

        // 반드시 일치할 필요는 없지만 재할당 방지 및 메모리 낭비 방지용
        debug_assert_eq!(line_size, ret.len());

        // 결과값에서 \r 또는 \n 제거
        let mut ret = BytesTrim::new_with_bytes(ret);
        if let Some(first) = ret.first() {
            if *first == b'\n' {
                ret.self_slice(1..);
            }
        }
        if let Some(last) = ret.last() {
            if *last == b'\r' {
                ret.self_slice(..ret.len() - 1);
            }
        }

        Ok(Some(ret))
    }

    pub fn current_cursor(&self) -> u64 {
        let mut cursor = self.reader_cursor;

        if let Some((_, pos)) = self.remain_buf {
            cursor += pos as u64;
        }

        cursor
    }

    pub fn into_reader(self) -> R {
        self.reader
    }
}

impl ReverseLineReader<File> {
    pub async fn open_file_with_buffer_size(
        path: impl AsRef<Path>,
        buf_size: NonZeroUsize,
    ) -> io::Result<ReverseLineReader<File>> {
        let f = OpenOptions::new().read(true).write(true).open(path).await?;

        let meta = f.metadata().await?;

        Ok(ReverseLineReader {
            reader: f,
            reader_cursor: meta.len(),
            buf_size,
            remain_buf: None,
        })
    }

    pub async fn new_with_opened_file(
        f: File,
        buf_size: NonZeroUsize,
    ) -> io::Result<ReverseLineReader<File>> {
        let meta = f.metadata().await?;

        Ok(ReverseLineReader {
            reader: f,
            reader_cursor: meta.len(),
            buf_size,
            remain_buf: None,
        })
    }

    /// 파일을 읽은만큼 truncate
    pub async fn file_truncate(&self) -> io::Result<()> {
        self.reader.set_len(self.current_cursor()).await
    }
}

#[cfg(test)]
async fn test_file_open_retry(path: &str, buf_size: usize) -> ReverseLineReader<File> {
    for _ in 1..10 {
        let reader = ReverseLineReader::open_file_with_buffer_size(
            path,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await;
        if let Ok(reader) = reader {
            return reader;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    panic!("FAIL_FILE_OPEN");
}

#[cfg(test)]
#[tokio::test]
async fn read_test() {
    for buf_size in 1..=10 {
        let mut reader = test_file_open_retry("test\\test1", buf_size).await;

        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"f"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"e"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"d"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"c"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"b"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"a"
        );
        assert!(reader.read_next_rev_line().await.is_none());
    }

    for buf_size in 1..=10 {
        let mut reader = test_file_open_retry("test\\test2", buf_size).await;

        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"e"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"dd"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"ccc"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"bbbb"
        );
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"aaaaa"
        );
        assert!(reader.read_next_rev_line().await.is_none());
    }
}

#[cfg(test)]
#[tokio::test]
async fn file_truncate_test() {
    const TARGET_PATH: &str = "test\\truncate";
    for buf_size in 1..=10 {
        std::fs::copy("test\\test1", TARGET_PATH).unwrap();
        let mut reader = test_file_open_retry(TARGET_PATH, buf_size).await;

        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"f"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"e"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"d"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"c"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"b"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"a"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert!(reader.read_next_rev_line().await.is_none());
    }

    for buf_size in 1..=10 {
        std::fs::copy("test\\test2", TARGET_PATH).unwrap();
        let mut reader = test_file_open_retry(TARGET_PATH, buf_size).await;

        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"e"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"dd"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"ccc"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"bbbb"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.read_next_rev_line().await.unwrap().unwrap().as_ref(),
            b"aaaaa"
        );
        reader.file_truncate().await.unwrap();
        reader = ReverseLineReader::open_file_with_buffer_size(
            TARGET_PATH,
            NonZeroUsize::new(buf_size).unwrap(),
        )
        .await
        .unwrap();
        assert!(reader.read_next_rev_line().await.is_none());
    }
}
