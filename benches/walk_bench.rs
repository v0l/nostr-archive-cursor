use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use nostr_archive_cursor::{NostrCursor, NostrEvent};
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Creates a temporary directory with dummy JSON-L files for benchmarking
async fn create_dummy_files(dir: &PathBuf, num_files: usize, events_per_file: usize) {
    tokio::fs::create_dir_all(dir).await.unwrap();

    let mut rng = rand::rng();

    for file_idx in 0..num_files {
        let file_path = dir.join(format!("events_{}.jsonl", file_idx));
        let mut file = File::create(&file_path).await.unwrap();

        for _event_idx in 0..events_per_file {
            // Generate random event ID (64 hex chars = 32 bytes)
            // This ensures proper distribution across DashMap's shards
            let random_bytes: [u8; 32] = rng.r#gen();
            let event_id = hex::encode(random_bytes);

            // Create a dummy event with unique ID but same content
            let event = NostrEvent {
                id: event_id,
                created_at: 1700000000,
                kind: 1,
                pubkey: "0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                sig: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
                content: "This is a benchmark test event with some content to make it realistic"
                    .to_string(),
                tags: vec![
                    vec!["e".to_string(), "ref123".to_string()],
                    vec!["p".to_string(), "pubkey123".to_string()],
                ],
            };

            let json = serde_json::to_string(&event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }

        file.flush().await.unwrap();
    }
}

/// Benchmark walk_with with different parallelism levels
fn bench_walk_with(c: &mut Criterion) {
    let mut group = c.benchmark_group("walk_with_parallelism");

    // Configure benchmark parameters - match max parallelism value
    let max_parallelism = 8;
    let num_files = max_parallelism;
    let events_per_file = 500_000;

    // Create test files once at the start
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = std::env::temp_dir().join(format!("nostr_bench_{}", std::process::id()));

    // Clean up any existing files and create new ones
    runtime.block_on(async {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        create_dummy_files(&temp_dir, num_files, events_per_file).await;
    });

    for parallelism in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(parallelism),
            &parallelism,
            |b, &parallelism| {
                let temp_dir = temp_dir.clone();
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(move || {
                        let temp_dir = temp_dir.clone();
                        async move {
                            // Run the benchmark using pre-created files
                            let counter = Arc::new(AtomicU64::new(0));
                            let counter_clone = counter.clone();

                            let cursor = NostrCursor::new(temp_dir).with_parallelism(parallelism);

                            cursor
                                .walk_with(move |_event| {
                                    let counter = counter_clone.clone();
                                    async move {
                                        counter.fetch_add(1, Ordering::Relaxed);
                                    }
                                })
                                .await;

                            let total = counter.load(Ordering::Relaxed);
                            assert_eq!(total, (num_files * events_per_file) as u64);
                        }
                    });
            },
        );
    }

    group.finish();

    // Cleanup test files after all benchmarks complete
    runtime.block_on(async {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    });
}

criterion_group!(benches, bench_walk_with);
criterion_main!(benches);
