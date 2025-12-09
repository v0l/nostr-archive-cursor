#[cfg(feature = "sync")]
pub(crate) mod sync {
    use std::io::BufRead;

    /// Synchronous JSON reader that uses BufRead's internal buffer directly
    pub struct SyncChunkedJsonReader<R: BufRead> {
        reader: R,
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
            Self { reader }
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
                                // Look back for newline - if we just saw \n{, this is truncated
                                if i > 0 && available[i - 1] == b'\n' {
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
                        self.reader.consume(end);
                        return Ok(buffer.len());
                    }
                    ScanResult::Truncated(pos) => {
                        // Discard truncated line and reset
                        self.reader.consume(pos);
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
                            self.reader.consume(buf_len);
                            // Now continue with accumulation mode
                            return self.continue_reading_object(buffer, depth, in_string, escaped);
                        } else if depth > 0 {
                            // Already accumulating (shouldn't happen on first iteration)
                            let available = self.reader.fill_buf()?;
                            buffer.extend_from_slice(available);
                            self.reader.consume(buf_len);
                        } else {
                            // Still looking for opening brace, just consume whitespace
                            self.reader.consume(buf_len);
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
                            // Detect truncation: newline followed by '{' while inside a string
                            b'{' if in_string => {
                                if i > 0 && available[i - 1] == b'\n' {
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
                    self.reader.consume(pos);
                    return Ok(buffer.len());
                }

                if let Some(pos) = truncated_at {
                    // Truncated while accumulating - discard and restart
                    self.reader.consume(pos);
                    buffer.clear();
                    return self.read_json_object(buffer);
                }

                // Need more data
                let available = self.reader.fill_buf()?;
                buffer.extend_from_slice(available);
                self.reader.consume(buf_len);

                // Safety check: panic if buffer grows beyond 50MB (likely runaway)
                if buffer.len() > 50 * 1024 * 1024 {
                    panic!(
                        "JSON object buffer exceeded 50MB - likely malformed input. Buffer starts with: {:?}",
                        String::from_utf8_lossy(&buffer[..buffer.len().min(200)])
                    );
                }
            }
        }
    }
}

#[cfg(feature = "async")]
pub(crate) mod not_sync {
    use tokio::io::{AsyncBufRead, AsyncBufReadExt};

    /// Async JSON reader that uses the underlying BufRead's internal buffer
    pub struct ChunkedJsonReader<R: AsyncBufRead + Unpin> {
        reader: R,
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
            Self { reader }
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
                                if i > 0 && available[i - 1] == b'\n' {
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
                        self.reader.consume(end);
                        return Ok(buffer.len());
                    }
                    ScanResult::Truncated(pos) => {
                        // Discard truncated line and reset
                        self.reader.consume(pos);
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
                            self.reader.consume(buf_len);
                            // Now continue with accumulation mode
                            return self
                                .continue_reading_object(buffer, depth, in_string, escaped)
                                .await;
                        } else if depth > 0 {
                            // Already accumulating (shouldn't happen on first iteration)
                            let available = self.reader.fill_buf().await?;
                            buffer.extend_from_slice(available);
                            self.reader.consume(buf_len);
                        } else {
                            // Still looking for opening brace, just consume whitespace
                            self.reader.consume(buf_len);
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
                                if i > 0 && available[i - 1] == b'\n' {
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
                    self.reader.consume(pos);
                    return Ok(buffer.len());
                }

                if let Some(pos) = truncated_at {
                    // Truncated while accumulating - discard and restart
                    self.reader.consume(pos);
                    buffer.clear();
                    return Box::pin(self.read_json_object(buffer)).await;
                }

                // Need more data
                let available = self.reader.fill_buf().await?;
                buffer.extend_from_slice(available);
                self.reader.consume(buf_len);

                // Safety check: panic if buffer grows beyond 50MB (likely runaway)
                if buffer.len() > 50 * 1024 * 1024 {
                    panic!(
                        "JSON object buffer exceeded 50MB - likely malformed input. Buffer starts with: {:?}",
                        String::from_utf8_lossy(&buffer[..buffer.len().min(200)])
                    );
                }
            }
        }
    }
}