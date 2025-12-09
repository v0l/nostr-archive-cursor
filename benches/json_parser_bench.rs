use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nostr_archive_cursor::{NostrEvent, NostrEventBorrowed};

const SIMPLE_EVENT: &str = r#"{"id":"abc123def456","pubkey":"def456abc123","created_at":1234567890,"kind":1,"tags":[],"content":"Hello world","sig":"xyz789"}"#;

const EVENT_WITH_TAGS: &str = r#"{"id":"abc123def456","pubkey":"def456abc123","created_at":1234567890,"kind":1,"tags":[["e","event123"],["p","pubkey456","wss://relay.example.com"],["t","nostr"],["t","benchmark"]],"content":"This is a reply with multiple tags","sig":"xyz789sig123"}"#;

const UTF8_EVENT: &str = r#"{"id":"abc123def456","pubkey":"def456abc123","created_at":1234567890,"kind":1,"tags":[["t","日本語"],["t","emoji"]],"content":"Hello 世界 🌍 Testing UTF-8: café, naïve, 你好, שלום, مرحبا","sig":"xyz789sig123"}"#;

const LARGE_CONTENT_EVENT: &str = r#"{"id":"abc123def456","pubkey":"def456abc123","created_at":1234567890,"kind":1,"tags":[["e","event1"],["e","event2"],["e","event3"],["p","pubkey1"],["p","pubkey2"]],"content":"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.","sig":"xyz789sig123"}"#;

fn benchmark_simple_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_event");

    group.bench_function("serde_json", |b| {
        b.iter(|| {
            let _event: NostrEvent = serde_json::from_str(black_box(SIMPLE_EVENT)).unwrap();
        });
    });

    group.bench_function("serde_borrowed", |b| {
        b.iter(|| {
            let _event: NostrEventBorrowed = serde_json::from_str(black_box(SIMPLE_EVENT)).unwrap();
        });
    });

    group.finish();
}

fn benchmark_event_with_tags(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_with_tags");

    group.bench_function("serde_json", |b| {
        b.iter(|| {
            let _event: NostrEvent = serde_json::from_str(black_box(EVENT_WITH_TAGS)).unwrap();
        });
    });

    group.bench_function("serde_borrowed", |b| {
        b.iter(|| {
            let _event: NostrEventBorrowed =
                serde_json::from_str(black_box(EVENT_WITH_TAGS)).unwrap();
        });
    });

    group.finish();
}

fn benchmark_utf8_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("utf8_event");

    group.bench_function("serde_json", |b| {
        b.iter(|| {
            let _event: NostrEvent = serde_json::from_str(black_box(UTF8_EVENT)).unwrap();
        });
    });

    group.bench_function("serde_borrowed", |b| {
        b.iter(|| {
            let _event: NostrEventBorrowed = serde_json::from_str(black_box(UTF8_EVENT)).unwrap();
        });
    });

    group.finish();
}

fn benchmark_large_content(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_content_event");

    group.bench_function("serde_json", |b| {
        b.iter(|| {
            let _event: NostrEvent = serde_json::from_str(black_box(LARGE_CONTENT_EVENT)).unwrap();
        });
    });

    group.bench_function("serde_borrowed", |b| {
        b.iter(|| {
            let _event: NostrEventBorrowed =
                serde_json::from_str(black_box(LARGE_CONTENT_EVENT)).unwrap();
        });
    });

    group.finish();
}

fn benchmark_batch_parsing(c: &mut Criterion) {
    let events = vec![
        SIMPLE_EVENT,
        EVENT_WITH_TAGS,
        UTF8_EVENT,
        LARGE_CONTENT_EVENT,
    ];

    let mut group = c.benchmark_group("batch_parsing");

    group.bench_with_input(
        BenchmarkId::new("serde_json", events.len()),
        &events,
        |b, events| {
            b.iter(|| {
                for event_json in events {
                    let _event: NostrEvent = serde_json::from_str(black_box(event_json)).unwrap();
                }
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("serde_borrowed", events.len()),
        &events,
        |b, events| {
            b.iter(|| {
                for event_json in events {
                    let _event: NostrEventBorrowed =
                        serde_json::from_str(black_box(event_json)).unwrap();
                }
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    benchmark_simple_event,
    benchmark_event_with_tags,
    benchmark_utf8_event,
    benchmark_large_content,
    benchmark_batch_parsing
);
criterion_main!(benches);
