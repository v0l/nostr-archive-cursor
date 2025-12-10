use anyhow::Result;
use log::info;
use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::{Client, Filter, RelayPoolNotification, Timestamp};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let dir = PathBuf::from("./test");
    let db = DefaultJsonFilesDatabase::new(&dir)?;

    let client = Client::builder().database(db.clone()).build();

    for r in [
        "wss://relay.snort.social",
        "wss://relay.primal.net",
        "wss://relay.damus.io",
        "wss://nos.lol",
    ] {
        client.add_relay(r).await?;
    }
    client.connect().await;

    client
        .subscribe(Filter::new().since(Timestamp::now()), None)
        .await?;
    let mut e = client.notifications();
    while let Ok(msg) = e.recv().await {
        match msg {
            RelayPoolNotification::Event { event, .. } => {
                info!("Got event: kind={}, id={}", event.kind, event.id);
            }
            RelayPoolNotification::Message { .. } => {}
            RelayPoolNotification::Shutdown => {}
        }
    }

    Ok(())
}
