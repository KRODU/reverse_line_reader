# reverse_line_reader

Rust 프로그램으로, 파일을 뒤에서부터 읽되 줄 바꿈을 만날 때까지 한 줄씩 비동기적으로 읽습니다.

또한 파일을 읽은만큼 truncate 할 수 있습니다. 만약 앞에서부터 파일을 읽을경우 읽은만큼 truncate가 불가능합니다. (파일을 앞에서부터 truncate하려면 전체를 복사해야 함.)


## 사용 예시

```rust
#[tokio::main]
async fn main() {
    let mut reader =
        ReverseLineReader::open_file_with_buffer_size("read.txt", NonZeroUsize::new(4096).unwrap())
            .await
            .unwrap();

    let rev_line = reader.read_next_rev_line().await.unwrap().unwrap();
    reader.file_truncate().await.unwrap();
    println!("{:?}", rev_line);
}

```
