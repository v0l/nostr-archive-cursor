# Nostr backup processor

Process JSON-L backups and compute some stats


## Example

```rust
let mut binding = NostrCursor::new("./backups".parse()?);
let mut cursor = Box::pin(binding.walk());
while let Some(Ok(e)) = cursor.next().await {
    // do something with `e`
}
```