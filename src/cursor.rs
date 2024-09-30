use crate::event::NostrEvent;
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, ZstdDecoder};
use async_stream::try_stream;
use std::collections::HashSet;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio_stream::Stream;

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
struct EventId([u8; 32]);

/// A director cursor over 1 or more JSON-L files
///
/// Skips duplicate events
///
/// Works with compressed files too
pub struct NostrCursor {
    ids: HashSet<EventId>,
    dir: PathBuf,
}

impl NostrCursor {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            ids: HashSet::new(),
        }
    }

    pub fn walk(&mut self) -> impl Stream<Item=Result<NostrEvent, anyhow::Error>> + '_
    {
        try_stream! {
            let mut dir_reader = tokio::fs::read_dir(&self.dir).await?;
            while let Ok(Some(path)) = dir_reader.next_entry().await {
                if path.file_type().await?.is_dir() {
                    continue;
                }
                let path = path.path();
                println!("Reading: {}", path.to_str().unwrap());
                let file = self.open_file(path).await?;

                let mut file = BufReader::new(file);
                let mut line = String::new();
                while let Ok(size) = file.read_line(&mut line).await {
                    if size == 0 {
                        break;
                    }

                    if let Ok(event) = serde_json::from_str::<NostrEvent>(&line[..size]) {
                        let ev_id = EventId(hex::decode(&event.id)?.as_slice().try_into()?);
                        if self.ids.insert(ev_id) {
                            yield event
                        }
                    }
                    line.clear();
                }
            }
        }
    }

    async fn open_file(&self, path: PathBuf) -> Result<Pin<Box<dyn AsyncRead>>, anyhow::Error>
    {
        let f = BufReader::new(File::open(path.clone()).await?);
        match path.extension() {
            Some(ext) => {
                match ext.to_str().unwrap() {
                    "json" => Ok(Box::pin(f)),
                    "jsonl" => Ok(Box::pin(f)),
                    "gz" => Ok(Box::pin(GzipDecoder::new(f))),
                    "zst" => Ok(Box::pin(ZstdDecoder::new(f))),
                    "bz2" => Ok(Box::pin(BzDecoder::new(f))),
                    _ => anyhow::bail!("Unknown extension")
                }
            }
            None => anyhow::bail!("Could not determine archive format")
        }
    }
}