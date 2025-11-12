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

#[derive(Serialize, Deserialize, Debug)]
pub struct NostrEventBorrowed<'a> {
    #[serde(borrow)]
    pub id: &'a str,
    pub created_at: u64,
    pub kind: u32,
    #[serde(borrow)]
    pub pubkey: &'a str,
    #[serde(borrow)]
    pub sig: &'a str,
    #[serde(borrow)]
    pub content: &'a str,
    #[serde(borrow)]
    pub tags: Vec<Vec<&'a str>>,
}

impl<'a> NostrEventBorrowed<'a> {
    fn to_owned(&self) -> NostrEvent {
        NostrEvent {
            id: self.id.to_string(),
            pubkey: self.pubkey.to_string(),
            created_at: self.created_at,
            kind: self.kind,
            sig: self.sig.to_string(),
            content: self.content.to_string(),
            tags: self
                .tags
                .iter()
                .map(|tag| tag.iter().map(|s| s.to_string()).collect())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_borrowed_basic() {
        let json = r#"{"id":"abc123","pubkey":"def456","created_at":1234567890,"kind":1,"tags":[],"content":"Hello world","sig":"xyz789"}"#;
        let event: NostrEventBorrowed = serde_json::from_str(json).unwrap();

        assert_eq!(event.id, "abc123");
        assert_eq!(event.pubkey, "def456");
        assert_eq!(event.created_at, 1234567890);
        assert_eq!(event.kind, 1);
        assert_eq!(event.content, "Hello world");
        assert_eq!(event.sig, "xyz789");
        assert_eq!(event.tags.len(), 0);

        // Verify zero-copy: slices point into the original string
        assert!(json.as_ptr() <= event.id.as_ptr());
        assert!(event.id.as_ptr() < unsafe { json.as_ptr().add(json.len()) });
    }

    #[test]
    fn test_serde_borrowed_with_tags() {
        let json = r#"{"id":"abc","pubkey":"def","created_at":1234567890,"kind":1,"tags":[["e","event123"],["p","pubkey456","relay"]],"content":"Reply","sig":"sig"}"#;
        let event: NostrEventBorrowed = serde_json::from_str(json).unwrap();

        assert_eq!(event.tags.len(), 2);
        assert_eq!(event.tags[0], vec!["e", "event123"]);
        assert_eq!(event.tags[1], vec!["p", "pubkey456", "relay"]);
    }

    #[test]
    fn test_serde_borrowed_utf8() {
        let json = r#"{"id":"abc","pubkey":"def","created_at":1234567890,"kind":1,"tags":[],"content":"Hello 世界 🌍 café","sig":"sig"}"#;
        let event: NostrEventBorrowed = serde_json::from_str(json).unwrap();

        assert_eq!(event.content, "Hello 世界 🌍 café");
    }
}
