use anyhow::Result;
use serde::Serialize;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use zstd::Encoder;
use zstd::stream::AutoFinishEncoder;

/// A ZSTD compressed JSON-L appender
pub struct CompressedJsonLFile {
    stream: AutoFinishEncoder<'static, File>,
}

impl CompressedJsonLFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::options().create(true).append(true).open(path)?;

        Ok(CompressedJsonLFile {
            stream: Encoder::new(file, 3)?.auto_finish(),
        })
    }

    pub fn write_event<O: Serialize>(&mut self, event: &O) -> Result<()> {
        let json = serde_json::to_string(event)?;
        let b_write = json.as_bytes();
        self.stream.write_all(b_write)?;
        self.stream.write(b"\n")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.stream.flush()?;
        Ok(())
    }
}
