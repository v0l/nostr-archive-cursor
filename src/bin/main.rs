use async_compression::tokio::write::ZstdEncoder;
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use nostr_archive_utils::cursor::NostrCursor;

#[derive(Parser)]
#[command(about, version)]
struct Args {
    #[arg(long)]
    pub dir: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let dir: PathBuf = args.dir.parse()?;
    println!("Reading data from: {}", dir.to_str().unwrap());

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
        let json = serde_json::to_vec(&e)?;
        fout.write(json.as_slice()).await?;
        fout.write("\n".as_bytes()).await?;
    }
    fout.flush().await?;

    write_csv(&out_dir.join("kinds.csv"), &event_kinds).await?;
    write_csv(&out_dir.join("days.csv"), &event_dates).await?;
    Ok(())
}

async fn write_csv<K, V>(dst: &PathBuf, data: &HashMap<K, V>) -> Result<(), anyhow::Error>
where
    K: ToString,
    V: ToString,
{
    let mut fout = File::create(dst).await?;
    for (k, v) in data {
        fout.write_all(format!("\"{}\",\"{}\"\n", k.to_string(), v.to_string()).as_bytes()).await?;
    }
    fout.flush().await?;
    Ok(())
}
