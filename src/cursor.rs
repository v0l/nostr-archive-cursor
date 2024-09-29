use crate::event::NostrEvent;
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, ZstdDecoder};
use std::collections::HashSet;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

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

    pub async fn walk<T>(&mut self, mut fx: T) -> Result<(), anyhow::Error>
    where
        T: FnMut(&NostrEvent),
    {
        let mut dir_reader = tokio::fs::read_dir(&self.dir).await?;
        while let Ok(Some(path)) = dir_reader.next_entry().await {
            let path = path.path();
            println!("Reading: {}", path.to_str().unwrap());
            let file: Pin<Box<dyn AsyncRead>> = match path.extension() {
                Some(ext) => {
                    let buf_reader = BufReader::new(File::open(path.clone()).await?);
                    match ext.to_str().unwrap() {
                        "json" => Box::pin(buf_reader),
                        "gz" => Box::pin(GzipDecoder::new(buf_reader)),
                        "zstd" => Box::pin(ZstdDecoder::new(buf_reader)),
                        "bz2" => Box::pin(BzDecoder::new(buf_reader)),
                        _ => anyhow::bail!("Unknown extension")
                    }
                }
                None => anyhow::bail!("Could not determine archive format")
            };
            let mut file = BufReader::new(file);
            let mut line = String::new();
            while let Ok(size) = file.read_line(&mut line).await {
                if size == 0 {
                    break;
                }

                if let Ok(event) = serde_json::from_str::<NostrEvent>(&line[..size]) {
                    let ev_id = EventId(hex::decode(&event.id)?.as_slice().try_into()?);
                    if self.ids.insert(ev_id) {
                        fx(&event);
                    }
                }
                line.clear();
            }
        }

        Ok(())
    }
}