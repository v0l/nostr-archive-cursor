mod event;
mod cursor;

use std::collections::HashMap;
use std::path::PathBuf;
use clap::Parser;
use crate::cursor::NostrCursor;

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

    let mut event_dates: HashMap<u64, u64> = HashMap::new();
    let mut cursor = NostrCursor::new(dir);
    cursor.walk(|e| {
        let day = e.created_at / (60 * 60 * 24);
        if let Some(x) = event_dates.get_mut(&day) {
            *x += 1u64;
        } else {
            event_dates.insert(day, 1);
        }
    }).await?;
    for (day, count) in event_dates {
        println!("{}: {}", day, count);
    }
    Ok(())
}
