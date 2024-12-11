use async_compression::tokio::write::ZstdEncoder;
use clap::{Parser, ValueEnum};
use nostr_cursor::cursor::NostrCursor;
use nostr_cursor::event::NostrEvent;
use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use log::{error, info};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, Semaphore};
use tokio_stream::StreamExt;
use url::Url;

#[derive(ValueEnum, Debug, Clone)]
enum ArgsOperation {
    Combine,
    MediaReport,
}

#[derive(Parser)]
#[command(about, version)]
struct Args {
    #[arg(long)]
    pub dir: String,

    #[arg(long)]
    pub operation: ArgsOperation,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let dir: PathBuf = args.dir.parse()?;
    info!("Reading data from: {}", dir.to_str().unwrap());
    match args.operation {
        ArgsOperation::Combine => {
            combine(dir).await?;
        }
        ArgsOperation::MediaReport => {
            media_report(dir).await?;
        }
    }

    Ok(())
}

async fn combine(dir: PathBuf) -> Result<(), anyhow::Error> {
    let out_dir = &dir.join("out");
    tokio::fs::create_dir_all(out_dir).await?;

    let mut fout = ZstdEncoder::new(File::create(out_dir.join("combined.jsonl.zst")).await?);
    let mut event_dates: HashMap<u64, u64> = HashMap::new();
    let mut event_kinds: HashMap<u32, u64> = HashMap::new();
    let mut binding = NostrCursor::new(dir);
    let mut cursor = Box::pin(binding.walk());
    while let Some(Ok(e)) = cursor.next().await {
        let day = e.created_at / (60 * 60 * 24);
        if let Some(x) = event_dates.get_mut(&day) {
            *x += 1u64;
        } else {
            event_dates.insert(day, 1);
        }
        if let Some(x) = event_kinds.get_mut(&e.kind) {
            *x += 1u64;
        } else {
            event_kinds.insert(e.kind, 1);
        }
        let json = serde_json::to_string(&e)?;
        fout.write_all(json.as_bytes()).await?;
        fout.write_all(b"\n").await?;
    }
    fout.flush().await?;

    write_csv(&out_dir.join("kinds.csv"), &event_kinds).await?;
    write_csv(&out_dir.join("days.csv"), &event_dates).await?;
    Ok(())
}

async fn media_report(dir: PathBuf) -> Result<(), anyhow::Error> {
    let report = Arc::new(RwLock::new(MediaReport::default()));

    let mut binding = NostrCursor::new(dir.clone());
    let mut cursor = Box::pin(binding.walk());
    let link_heads = Arc::new(RwLock::new(HashMap::<Url, bool>::new()));
    let sem = Arc::new(Semaphore::new(50));
    let mut notes = 0u64;
    while let Some(Ok(e)) = cursor.next().await {
        if e.kind != 1 {
            continue;
        }

        let sem = sem.clone();
        let permit = sem.acquire_owned().await?;
        let links = link_heads.clone();
        let report = report.clone();
        tokio::spawn(async move {
            if let Err(e) = process_note(e, report, links).await {
                error!("Failed to process note: {}", e);
            }
            drop(permit);
        });
        notes += 1;
    }

    info!("Processed {} notes, writing report!", notes);
    let report = report.read().await;
    let mut fout = File::create(dir.join("media_report.json")).await?;
    fout.write_all(serde_json::to_vec(&report.deref())?.as_slice())
        .await?;

    Ok(())
}

async fn process_note(e: NostrEvent, report: Arc<RwLock<MediaReport>>, link_heads: Arc<RwLock<HashMap<Url, bool>>>) -> Result<(), anyhow::Error> {
    let media_regex = Regex::new(
        r"https?://(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9\(\)]{1,6}\b(?:[-a-zA-Z0-9\(\)!@:%_\+.~#?&\/\/=]*)",
    )?;
    let file_ext = Regex::new(r"\.[a-zA-Z]{1,5}$")?;

    for text in media_regex.find_iter(e.content.as_str()) {
        let text = text.as_str().trim();

        if let Ok(u) = Url::parse(text) {
            let ext = match file_ext.find(u.path()) {
                Some(ext) => ext.as_str(),
                None => continue,
            };
            let host = match u.host_str() {
                Some(host) => host,
                None => continue,
            };

            {
                let mut report = report.write().await;
                inc_map(&mut report.hosts_count, host, 1);
                inc_map(&mut report.extensions, ext, 1);

                if let Some(imeta) = e.tags.iter().find(|e| e[0] == "imeta") {
                    if let Some(size) = imeta.iter().find(|a| a.starts_with("size")) {
                        if let Ok(size_n) = size.split(" ").last().unwrap().parse::<u64>() {
                            inc_map(&mut report.hosts_size, host, size_n);
                        }
                    }
                    inc_map(&mut report.hosts_imeta, host, 1);
                } else {
                    inc_map(&mut report.hosts_no_imeta, host, 1);
                }
            }

            let hr = {
                let links = link_heads.read().await;
                if let Some(hr) = links.get(&u) {
                    Some(*hr)
                } else {
                    None
                }
            };
            if let Some(hr) = hr {
                if hr {
                    let mut report = report.write().await;
                    inc_map(&mut report.hosts_dead, host, 1);
                }
            } else {
                info!("Testing link: {text}");
                let cli = reqwest::Client::new();
                loop {
                    let u = u.clone();
                    match cli.head(text)
                        .timeout(Duration::from_secs(5))
                        .send().await {
                        Ok(rsp) => {
                            if rsp.status() == 429 {
                                info!("Rate limited by {}, waiting", u.host().unwrap());
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                continue;
                            }

                            let mut report = report.write().await;
                            let mut link_heads = link_heads.write().await;
                            if rsp.status().as_u16() > 300 {
                                inc_map(&mut report.hosts_dead, host, 1);
                                link_heads.insert(u, true);
                            } else {
                                link_heads.insert(u, false);
                            }
                            break;
                        }
                        Err(_) => {
                            let mut report = report.write().await;
                            let mut link_heads = link_heads.write().await;
                            inc_map(&mut report.hosts_dead, host, 1);
                            link_heads.insert(u, true);
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn inc_map(map: &mut HashMap<String, u64>, key: &str, n: u64) {
    if let Some(v) = map.get_mut(key) {
        *v += n;
    } else {
        map.insert(key.to_string(), n);
    }
}

async fn write_csv<K, V>(dst: &PathBuf, data: &HashMap<K, V>) -> Result<(), anyhow::Error>
where
    K: ToString,
    V: ToString,
{
    let mut fout = File::create(dst).await?;
    for (k, v) in data {
        fout.write_all(format!("\"{}\",\"{}\"\n", k.to_string(), v.to_string()).as_bytes())
            .await?;
    }
    fout.flush().await?;
    Ok(())
}

#[derive(Serialize, Default)]
struct MediaReport {
    pub hosts_count: HashMap<String, u64>,
    pub hosts_dead: HashMap<String, u64>,
    pub hosts_size: HashMap<String, u64>,
    pub hosts_imeta: HashMap<String, u64>,
    pub hosts_no_imeta: HashMap<String, u64>,
    pub extensions: HashMap<String, u64>,
}
