#[cfg(feature = "sync")]
pub(crate) mod sync {
    use std::io::BufRead;

    /// Synchronous JSON reader that uses BufRead's internal buffer directly.
    ///
    /// Tracks the byte offset of every object it returns, so an indexer can
    /// record where each event lives in the (decompressed) stream.
    pub struct SyncChunkedJsonReader<R: BufRead> {
        reader: R,
        /// Bytes consumed from the underlying stream so far.
        pos: u64,
        /// Offset of the first byte of the most recently returned object.
        obj_start: u64,
        /// Last byte consumed. Truncation recovery needs to know whether the
        /// byte before the current buffer was a newline, which is invisible
        /// once the reader refills.
        prev_byte: u8,
    }

    /// Is the byte before position `i` a newline? At `i == 0` that byte lives
    /// in the previous buffer, so the caller passes what it consumed last.
    fn preceded_by_newline(available: &[u8], i: usize, prev_byte: u8) -> bool {
        if i > 0 {
            available[i - 1] == b'\n'
        } else {
            prev_byte == b'\n'
        }
    }

    enum ScanResult {
        /// Found complete object, start and end positions (exclusive)
        Complete { start: usize, end: usize },
        /// Hit newline at position, line was truncated - skip and retry
        Truncated(usize),
        /// Need more data, consumed entire buffer
        NeedMore { start: Option<usize> },
    }

    impl<R: BufRead> SyncChunkedJsonReader<R> {
        pub fn new(reader: R) -> Self {
            Self {
                reader,
                pos: 0,
                obj_start: 0,
                prev_byte: 0,
            }
        }

        /// Offset of the object returned by the last successful
        /// [`read_json_object`](Self::read_json_object) call.
        pub fn last_object_offset(&self) -> u64 {
            self.obj_start
        }

        /// Total bytes consumed from the stream.
        #[allow(dead_code)]
        pub fn position(&self) -> u64 {
            self.pos
        }

        /// `BufRead::consume` plus offset bookkeeping. Every consume in this
        /// reader must go through here or offsets drift.
        fn consume(&mut self, n: usize) {
            self.reader.consume(n);
            self.pos += n as u64;
        }

        /// Consume `n` bytes, remembering the last one so truncation recovery
        /// still works when a line break falls on a buffer boundary.
        fn consume_tracked(&mut self, n: usize) -> std::io::Result<()> {
            if n > 0 {
                let available = self.reader.fill_buf()?;
                if let Some(&b) = available.get(n - 1) {
                    self.prev_byte = b;
                }
            }
            self.consume(n);
            Ok(())
        }

        /// Read a complete JSON object into the buffer.
        /// Assumes JSON-L format: newline outside string means end of object.
        /// Returns the length of the JSON object, or 0 if EOF.
        pub fn read_json_object(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
            buffer.clear();

            let mut depth = 0i32;
            let mut in_string = false;
            let mut escaped = false;

            loop {
                let prev_byte = self.prev_byte;
                let (result, buf_len) = {
                    let available = self.reader.fill_buf()?;
                    if available.is_empty() {
                        return Ok(buffer.len());
                    }

                    let mut result = ScanResult::NeedMore { start: None };
                    let mut obj_start: Option<usize> = None;

                    for (i, &byte) in available.iter().enumerate() {
                        // Looking for opening brace
                        if depth == 0 {
                            if byte == b'{' {
                                obj_start = Some(i);
                                depth = 1;
                            } else if !byte.is_ascii_whitespace() {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Expected JSON object to start with '{'",
                                ));
                            }
                            continue;
                        }

                        if escaped {
                            escaped = false;
                            continue;
                        }

                        match byte {
                            b'\\' if in_string => escaped = true,
                            b'"' => in_string = !in_string,
                            b'{' if !in_string => depth += 1,
                            b'}' if !in_string => {
                                depth -= 1;
                                if depth == 0 {
                                    // Found complete object in this single buffer
                                    result = ScanResult::Complete {
                                        start: obj_start.unwrap_or(0),
                                        end: i + 1,
                                    };
                                    break;
                                }
                            }
                            // JSON-L: newline outside string means truncated/malformed line
                            b'\n' if !in_string && depth > 0 => {
                                result = ScanResult::Truncated(i + 1);
                                break;
                            }
                            // Detect truncation: newline followed by '{' while inside a string
                            // This happens when a line is cut off mid-string and next object starts
                            b'{' if in_string => {
                                // Look back for newline - if we just saw \n{, this is truncated.
                                // The newline may be the last byte of the *previous* buffer, so
                                // this must not be a plain `available[i - 1]` check: missing that
                                // case swallows the valid event following a truncated line.
                                if preceded_by_newline(available, i, prev_byte) {
                                    // Truncated: skip to current position (the new '{')
                                    // and restart parsing from here
                                    result = ScanResult::Truncated(i);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }

                    // Set start position for NeedMore if we found opening brace
                    if matches!(result, ScanResult::NeedMore { .. }) {
                        result = ScanResult::NeedMore { start: obj_start };
                    }

                    (result, available.len())
                };

                match result {
                    ScanResult::Complete { start, end } => {
                        // Re-borrow to copy the complete object
                        let available = self.reader.fill_buf()?;
                        buffer.extend_from_slice(&available[start..end]);
                        self.obj_start = self.pos + start as u64;
                        self.consume_tracked(end)?;
                        return Ok(buffer.len());
                    }
                    ScanResult::Truncated(pos) => {
                        // Discard truncated line and reset
                        self.consume_tracked(pos)?;
                        buffer.clear();
                        depth = 0;
                        in_string = false;
                        escaped = false;
                    }
                    ScanResult::NeedMore { start } => {
                        // Object spans multiple buffers - need to accumulate
                        if let Some(s) = start {
                            // We found opening brace, copy from there
                            let available = self.reader.fill_buf()?;
                            buffer.extend_from_slice(&available[s..]);
                            self.obj_start = self.pos + s as u64;
                            self.consume_tracked(buf_len)?;
                            // Now continue with accumulation mode
                            return self.continue_reading_object(buffer, depth, in_string, escaped);
                        } else if depth > 0 {
                            // Already accumulating (shouldn't happen on first iteration)
                            let available = self.reader.fill_buf()?;
                            buffer.extend_from_slice(available);
                            self.consume_tracked(buf_len)?;
                        } else {
                            // Still looking for opening brace, just consume whitespace
                            self.consume_tracked(buf_len)?;
                        }
                    }
                }
            }
        }

        /// Continue reading an object that spans multiple buffers.
        /// At this point we're committed - if we hit a truncation, we have to discard
        /// what we've accumulated and start over.
        fn continue_reading_object(
            &mut self,
            buffer: &mut Vec<u8>,
            mut depth: i32,
            mut in_string: bool,
            mut escaped: bool,
        ) -> std::io::Result<usize> {
            loop {
                let prev_byte = self.prev_byte;
                let (complete_at, truncated_at, buf_len) = {
                    let available = self.reader.fill_buf()?;
                    if available.is_empty() {
                        return Ok(buffer.len());
                    }

                    let mut complete_at = None;
                    let mut truncated_at = None;

                    for (i, &byte) in available.iter().enumerate() {
                        if escaped {
                            escaped = false;
                            continue;
                        }

                        match byte {
                            b'\\' if in_string => escaped = true,
                            b'"' => in_string = !in_string,
                            b'{' if !in_string => depth += 1,
                            b'}' if !in_string => {
                                depth -= 1;
                                if depth == 0 {
                                    complete_at = Some(i + 1);
                                    break;
                                }
                            }
                            b'\n' if !in_string && depth > 0 => {
                                truncated_at = Some(i + 1);
                                break;
                            }
                            // Detect truncation: newline followed by '{' while inside a
                            // string (the newline can be in the previous buffer).
                            b'{' if in_string => {
                                if preceded_by_newline(available, i, prev_byte) {
                                    truncated_at = Some(i);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }

                    (complete_at, truncated_at, available.len())
                };

                if let Some(pos) = complete_at {
                    let available = self.reader.fill_buf()?;
                    buffer.extend_from_slice(&available[..pos]);
                    self.consume_tracked(pos)?;
                    return Ok(buffer.len());
                }

                if let Some(pos) = truncated_at {
                    // Truncated while accumulating - discard and restart
                    self.consume_tracked(pos)?;
                    buffer.clear();
                    return self.read_json_object(buffer);
                }

                // Need more data
                let available = self.reader.fill_buf()?;
                buffer.extend_from_slice(available);
                self.consume_tracked(buf_len)?;

                // Safety check: skip if buffer grows beyond 50MB (likely malformed input)
                if buffer.len() > 50 * 1024 * 1024 {
                    log::error!(
                        "JSON object exceeded 50MB, skipping to next line (malformed input). Starts with: {:?}",
                        String::from_utf8_lossy(&buffer[..buffer.len().min(200)])
                    );
                    self.skip_to_newline()?;
                    buffer.clear();
                    return self.read_json_object(buffer);
                }
            }
        }

        /// Discard bytes until (and including) the next newline. Used to recover
        /// from malformed/oversized input.
        fn skip_to_newline(&mut self) -> std::io::Result<()> {
            loop {
                let available = self.reader.fill_buf()?;
                if available.is_empty() {
                    return Ok(());
                }
                match available.iter().position(|&b| b == b'\n') {
                    Some(pos) => {
                        self.consume_tracked(pos + 1)?;
                        return Ok(());
                    }
                    None => {
                        let len = available.len();
                        self.consume_tracked(len)?;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "async")]
pub(crate) mod not_sync {
    use tokio::io::{AsyncBufRead, AsyncBufReadExt};

    /// Is the byte before position `i` a newline? At `i == 0` that byte lives
    /// in the previous buffer, so the caller passes what it consumed last.
    fn preceded_by_newline(available: &[u8], i: usize, prev_byte: u8) -> bool {
        if i > 0 {
            available[i - 1] == b'\n'
        } else {
            prev_byte == b'\n'
        }
    }

    /// Async JSON reader that uses the underlying BufRead's internal buffer
    pub struct ChunkedJsonReader<R: AsyncBufRead + Unpin> {
        reader: R,
        /// Last byte consumed; see the sync reader for why this is needed.
        prev_byte: u8,
    }

    enum ScanResult {
        /// Found complete object, start and end positions (exclusive)
        Complete { start: usize, end: usize },
        /// Hit newline at position, line was truncated - skip and retry
        Truncated(usize),
        /// Need more data, consumed entire buffer
        NeedMore { start: Option<usize> },
    }

    impl<R: AsyncBufRead + Unpin> ChunkedJsonReader<R> {
        pub fn new(reader: R) -> Self {
            Self {
                reader,
                prev_byte: 0,
            }
        }

        /// Consume `n` bytes, remembering the last one so truncation recovery
        /// still works when a line break falls on a buffer boundary.
        async fn consume_tracked(&mut self, n: usize) -> std::io::Result<()> {
            if n > 0 {
                let available = self.reader.fill_buf().await?;
                if let Some(&b) = available.get(n - 1) {
                    self.prev_byte = b;
                }
            }
            self.reader.consume(n);
            Ok(())
        }

        /// Read a complete JSON object into the buffer.
        /// Assumes JSON-L format: newline outside string means end of object.
        /// Returns the length of the JSON object, or 0 if EOF.
        pub async fn read_json_object(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
            buffer.clear();

            let mut depth = 0i32;
            let mut in_string = false;
            let mut escaped = false;

            loop {
                let prev_byte = self.prev_byte;
                let (result, buf_len) = {
                    let available = self.reader.fill_buf().await?;
                    if available.is_empty() {
                        return Ok(buffer.len());
                    }

                    let mut result = ScanResult::NeedMore { start: None };
                    let mut obj_start: Option<usize> = None;

                    for (i, &byte) in available.iter().enumerate() {
                        // Looking for opening brace
                        if depth == 0 {
                            if byte == b'{' {
                                obj_start = Some(i);
                                depth = 1;
                            } else if !byte.is_ascii_whitespace() {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Expected JSON object to start with '{'",
                                ));
                            }
                            continue;
                        }

                        if escaped {
                            escaped = false;
                            continue;
                        }

                        match byte {
                            b'\\' if in_string => escaped = true,
                            b'"' => in_string = !in_string,
                            b'{' if !in_string => depth += 1,
                            b'}' if !in_string => {
                                depth -= 1;
                                if depth == 0 {
                                    // Found complete object in this single buffer
                                    result = ScanResult::Complete {
                                        start: obj_start.unwrap_or(0),
                                        end: i + 1,
                                    };
                                    break;
                                }
                            }
                            // JSON-L: newline outside string means truncated/malformed line
                            b'\n' if !in_string && depth > 0 => {
                                result = ScanResult::Truncated(i + 1);
                                break;
                            }
                            // Detect truncation: newline followed by '{' while inside a string
                            b'{' if in_string => {
                                // The newline may be the last byte of the previous
                                // buffer; missing that swallows the valid event
                                // following a truncated line.
                                if preceded_by_newline(available, i, prev_byte) {
                                    result = ScanResult::Truncated(i);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }

                    // Set start position for NeedMore if we found opening brace
                    if matches!(result, ScanResult::NeedMore { .. }) {
                        result = ScanResult::NeedMore { start: obj_start };
                    }

                    (result, available.len())
                };

                match result {
                    ScanResult::Complete { start, end } => {
                        // Re-borrow to copy the complete object
                        let available = self.reader.fill_buf().await?;
                        buffer.extend_from_slice(&available[start..end]);
                        self.consume_tracked(end).await?;
                        return Ok(buffer.len());
                    }
                    ScanResult::Truncated(pos) => {
                        // Discard truncated line and reset
                        self.consume_tracked(pos).await?;
                        buffer.clear();
                        depth = 0;
                        in_string = false;
                        escaped = false;
                    }
                    ScanResult::NeedMore { start } => {
                        // Object spans multiple buffers - need to accumulate
                        if let Some(s) = start {
                            // We found opening brace, copy from there
                            let available = self.reader.fill_buf().await?;
                            buffer.extend_from_slice(&available[s..]);
                            self.consume_tracked(buf_len).await?;
                            // Now continue with accumulation mode
                            return self
                                .continue_reading_object(buffer, depth, in_string, escaped)
                                .await;
                        } else if depth > 0 {
                            // Already accumulating (shouldn't happen on first iteration)
                            let available = self.reader.fill_buf().await?;
                            buffer.extend_from_slice(available);
                            self.consume_tracked(buf_len).await?;
                        } else {
                            // Still looking for opening brace, just consume whitespace
                            self.consume_tracked(buf_len).await?;
                        }
                    }
                }
            }
        }

        /// Continue reading an object that spans multiple buffers.
        /// At this point we're committed - if we hit a truncation, we have to discard
        /// what we've accumulated and start over.
        async fn continue_reading_object(
            &mut self,
            buffer: &mut Vec<u8>,
            mut depth: i32,
            mut in_string: bool,
            mut escaped: bool,
        ) -> std::io::Result<usize> {
            loop {
                let prev_byte = self.prev_byte;
                let (complete_at, truncated_at, buf_len) = {
                    let available = self.reader.fill_buf().await?;
                    if available.is_empty() {
                        return Ok(buffer.len());
                    }

                    let mut complete_at = None;
                    let mut truncated_at = None;

                    for (i, &byte) in available.iter().enumerate() {
                        if escaped {
                            escaped = false;
                            continue;
                        }

                        match byte {
                            b'\\' if in_string => escaped = true,
                            b'"' => in_string = !in_string,
                            b'{' if !in_string => depth += 1,
                            b'}' if !in_string => {
                                depth -= 1;
                                if depth == 0 {
                                    complete_at = Some(i + 1);
                                    break;
                                }
                            }
                            b'\n' if !in_string && depth > 0 => {
                                truncated_at = Some(i + 1);
                                break;
                            }
                            // Detect truncation: newline followed by '{' while inside a string
                            b'{' if in_string => {
                                if preceded_by_newline(available, i, prev_byte) {
                                    truncated_at = Some(i);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }

                    (complete_at, truncated_at, available.len())
                };

                if let Some(pos) = complete_at {
                    let available = self.reader.fill_buf().await?;
                    buffer.extend_from_slice(&available[..pos]);
                    self.consume_tracked(pos).await?;
                    return Ok(buffer.len());
                }

                if let Some(pos) = truncated_at {
                    // Truncated while accumulating - discard and restart
                    self.consume_tracked(pos).await?;
                    buffer.clear();
                    return Box::pin(self.read_json_object(buffer)).await;
                }

                // Need more data
                let available = self.reader.fill_buf().await?;
                buffer.extend_from_slice(available);
                self.consume_tracked(buf_len).await?;

                // Safety check: skip if buffer grows beyond 50MB (likely malformed input)
                if buffer.len() > 50 * 1024 * 1024 {
                    log::error!(
                        "JSON object exceeded 50MB, skipping to next line (malformed input). Starts with: {:?}",
                        String::from_utf8_lossy(&buffer[..buffer.len().min(200)])
                    );
                    self.skip_to_newline().await?;
                    buffer.clear();
                    return Box::pin(self.read_json_object(buffer)).await;
                }
            }
        }

        /// Discard bytes until (and including) the next newline. Used to recover
        /// from malformed/oversized input.
        async fn skip_to_newline(&mut self) -> std::io::Result<()> {
            loop {
                let available = self.reader.fill_buf().await?;
                if available.is_empty() {
                    return Ok(());
                }
                match available.iter().position(|&b| b == b'\n') {
                    Some(pos) => {
                        self.consume_tracked(pos + 1).await?;
                        return Ok(());
                    }
                    None => {
                        let len = available.len();
                        self.consume_tracked(len).await?;
                    }
                }
            }
        }
    }
}
#[cfg(all(test, feature = "sync"))]
mod sync_tests {
    use super::sync::SyncChunkedJsonReader;
    use std::io::BufReader;

    /// Build a JSON-L stream of `n` objects of `size` bytes each (plus newline),
    /// returning the bytes and the true offset of every object.
    fn stream(n: usize, size: usize) -> (Vec<u8>, Vec<u64>) {
        let mut buf = Vec::new();
        let mut offsets = Vec::new();
        for i in 0..n {
            offsets.push(buf.len() as u64);
            let head = format!("{{\"id\":\"{i:064x}\",\"content\":\"",);
            let tail = "\"}";
            let pad = size.saturating_sub(head.len() + tail.len());
            buf.extend_from_slice(head.as_bytes());
            buf.extend(std::iter::repeat_n(b'x', pad));
            buf.extend_from_slice(tail.as_bytes());
            buf.push(b'\n');
        }
        (buf, offsets)
    }

    /// A line truncated *inside a string* must not swallow the valid event that
    /// follows it - including when the newline between them is the last byte of
    /// a buffer, which hides it from a plain `available[i - 1]` look-back.
    ///
    /// Regression: this silently dropped ~1 event per truncated line in a real
    /// nostr.band archive (9 of 500k), always at a buffer boundary.
    #[test]
    fn truncated_line_does_not_swallow_the_next_event() {
        const CAP: usize = 4096;
        for pad in 0..8usize {
            let mut data = Vec::new();
            // Filler so the truncated line ends exactly on the boundary.
            let good = |i: usize| format!("{{\"id\":\"{i:064x}\",\"content\":\"ok\"}}\n");
            while data.len() < CAP / 2 {
                data.extend_from_slice(good(0).as_bytes());
            }
            // A line cut off mid-string, so the parser is `in_string` at the \n.
            let head = b"{\"id\":\"deadbeef\",\"content\":\"truncated";
            let fill = CAP - 1 - (data.len() + head.len()) % CAP + pad;
            data.extend_from_slice(head);
            data.extend(std::iter::repeat_n(b'y', fill));
            data.push(b'\n');
            // ...followed by a perfectly good event.
            let victim_offset = data.len() as u64;
            data.extend_from_slice(good(0xabc).as_bytes());
            data.extend_from_slice(good(0xdef).as_bytes());

            let mut reader = SyncChunkedJsonReader::new(BufReader::with_capacity(CAP, &data[..]));
            let mut ids = Vec::new();
            let mut offsets = Vec::new();
            let mut buffer = Vec::new();
            while let Ok(n) = reader.read_json_object(&mut buffer) {
                if n == 0 {
                    break;
                }
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&buffer) {
                    ids.push(v["id"].as_str().unwrap_or_default().to_string());
                    offsets.push(reader.last_object_offset());
                }
            }
            let want = format!("{:064x}", 0xabc);
            let at = ids.iter().position(|i| *i == want);
            assert!(
                at.is_some(),
                "pad {pad}: event after a truncated line was swallowed"
            );
            assert_eq!(
                offsets[at.unwrap()],
                victim_offset,
                "pad {pad}: wrong offset for the event after a truncated line"
            );
            assert!(
                ids.iter().any(|i| *i == format!("{:064x}", 0xdef)),
                "pad {pad}: lost the event after that"
            );
        }
    }

    /// Objects must be returned complete, with correct offsets, no matter where
    /// they fall relative to the reader's buffer refills.
    #[test]
    fn offsets_are_correct_across_buffer_boundaries() {
        // 488 bytes is the size of the real kind-7 events that exposed this;
        // several sizes to land objects on every offset modulo the buffer.
        for size in [61, 488, 4093, 8192] {
            let (data, want) = stream(2000, size);
            // Small BufReader capacity forces many refills, like a decompressor
            // handing out fixed-size chunks.
            let mut reader = SyncChunkedJsonReader::new(BufReader::with_capacity(4096, &data[..]));
            let mut got = Vec::new();
            let mut buffer = Vec::new();
            loop {
                match reader.read_json_object(&mut buffer) {
                    Ok(0) => break,
                    Ok(_) => got.push((reader.last_object_offset(), buffer.clone())),
                    Err(e) => panic!("size {size}: read failed: {e}"),
                }
            }
            assert_eq!(got.len(), want.len(), "size {size}: lost objects");
            for (i, (offset, body)) in got.iter().enumerate() {
                assert_eq!(*offset, want[i], "size {size}: wrong offset for object {i}");
                assert_eq!(
                    &data[*offset as usize..*offset as usize + body.len()],
                    &body[..],
                    "size {size}: object {i} does not match the bytes at its offset"
                );
            }
        }
    }
}
