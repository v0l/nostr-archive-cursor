use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct NostrEvent {
    pub id: String,
    pub created_at: u64,
    pub kind: u32,
    pub pubkey: String,
    pub sig: String,
    pub content: String,
    pub tags: Vec<Vec<String>>,
}