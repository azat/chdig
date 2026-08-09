use chrono::{DateTime, Local};
use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::sync::Mutex;

#[derive(Clone)]
pub struct LogEntry {
    pub host_name: String,
    pub display_host_name: Option<String>,
    pub event_time_microseconds: DateTime<Local>,
    pub thread_id: u64,
    pub level: String,
    pub message: String,
    pub query_id: Option<String>,
    pub logger_name: Option<String>,
}

// Entries are kept in an anonymous temp file instead of memory (the view polls
// the server indefinitely, so in-memory storage grows unboundedly, see #242).
// Only the index (offset+len per entry, in logical display order) and a small
// window of decoded entries stay in memory.
pub struct LogStore {
    backing: Backing,
    // Logical display order; insert_at() splices here while the file only
    // appends, so logical order != file order in descending mode.
    index: Vec<IndexEntry>,
    // Reads happen from draw() which takes &self (and cursive requires View to
    // be Sync, so this has to be a Mutex even though access is single-threaded).
    cache: Mutex<WindowCache>,
}

#[derive(Clone, Copy)]
struct IndexEntry {
    offset: u64,
    len: u32,
}

enum Backing {
    // Nothing pushed yet, the temp file is created lazily.
    Unopened,
    File { file: File, end: u64 },
    // Fallback if the temp file cannot be created (behaves like the old Vec).
    Memory(Vec<LogEntry>),
}

const CACHE_WINDOW: usize = 256;

#[derive(Default)]
struct WindowCache {
    start: usize,
    entries: Vec<LogEntry>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    pub fn new() -> Self {
        LogStore {
            backing: Backing::Unopened,
            index: Vec::new(),
            cache: Mutex::new(WindowCache::default()),
        }
    }

    pub fn len(&self) -> usize {
        match &self.backing {
            Backing::Memory(entries) => entries.len(),
            _ => self.index.len(),
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, batch: Vec<LogEntry>) {
        match self.write_batch(&batch) {
            Some(index) => self.index.extend(index),
            None => {
                if let Backing::Memory(entries) = &mut self.backing {
                    entries.extend(batch);
                }
            }
        }
    }

    pub fn insert_at(&mut self, pos: usize, batch: Vec<LogEntry>) {
        match self.write_batch(&batch) {
            Some(index) => {
                let pos = pos.min(self.index.len());
                {
                    let mut cache = self.cache.lock().unwrap();
                    if pos <= cache.start {
                        // Cached indices shift by the amount inserted in front
                        cache.start += index.len();
                    } else if pos < cache.start + cache.entries.len() {
                        // The insert lands inside the cached window
                        cache.entries.clear();
                    }
                }
                self.index.splice(pos..pos, index);
            }
            None => {
                if let Backing::Memory(entries) = &mut self.backing {
                    let pos = pos.min(entries.len());
                    entries.splice(pos..pos, batch);
                }
            }
        }
    }

    pub fn with_entry<R>(&self, idx: usize, f: impl FnOnce(&LogEntry) -> R) -> Option<R> {
        let file = match &self.backing {
            Backing::Memory(entries) => return entries.get(idx).map(f),
            Backing::Unopened => return None,
            Backing::File { file, .. } => file,
        };
        if idx >= self.index.len() {
            return None;
        }

        {
            let cache = self.cache.lock().unwrap();
            if idx >= cache.start && idx < cache.start + cache.entries.len() {
                return Some(f(&cache.entries[idx - cache.start]));
            }
        }

        // Miss: load a window centered on idx, so both forward scans and
        // reverse scans hit the cache for the neighbouring entries.
        let start = idx.saturating_sub(CACHE_WINDOW / 2);
        let end = usize::min(start + CACHE_WINDOW, self.index.len());
        let mut entries = Vec::with_capacity(end - start);
        let mut buf = Vec::new();
        for entry in &self.index[start..end] {
            buf.resize(entry.len as usize, 0);
            if let Err(e) = read_exact_at(file, entry.offset, &mut buf) {
                log::error!("LogStore: failed to read log entry: {}", e);
                return None;
            }
            match decode_entry(&buf) {
                Some(entry) => entries.push(entry),
                None => {
                    log::error!("LogStore: failed to decode log entry");
                    return None;
                }
            }
        }
        let mut cache = self.cache.lock().unwrap();
        *cache = WindowCache { start, entries };
        Some(f(&cache.entries[idx - cache.start]))
    }

    // Returns the index entries for the batch, or None for the Memory backing
    // (including the case when it has just degraded to it).
    fn write_batch(&mut self, batch: &[LogEntry]) -> Option<Vec<IndexEntry>> {
        if let Backing::Unopened = self.backing {
            self.backing = match tempfile::tempfile() {
                Ok(file) => Backing::File { file, end: 0 },
                Err(e) => {
                    log::error!(
                        "LogStore: cannot create temporary file, keeping logs in memory: {}",
                        e
                    );
                    Backing::Memory(Vec::new())
                }
            };
        }
        let Backing::File { file, end } = &mut self.backing else {
            return None;
        };

        let mut index = Vec::with_capacity(batch.len());
        let mut buf = Vec::new();
        let result = (|| -> io::Result<()> {
            file.seek(SeekFrom::Start(*end))?;
            let mut out = BufWriter::new(&*file);
            for entry in batch {
                buf.clear();
                encode_entry(entry, &mut buf);
                out.write_all(&buf)?;
                index.push(IndexEntry {
                    offset: *end,
                    len: buf.len() as u32,
                });
                *end += buf.len() as u64;
            }
            out.flush()
        })();
        if let Err(e) = result {
            // No fallback here: a write failure (e.g. disk full) would likely
            // repeat for any further batches, do not start eating memory instead.
            log::error!("LogStore: failed to write log entries (dropped): {}", e);
            index.truncate(0);
        }
        Some(index)
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(not(unix))]
fn read_exact_at(mut file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    use std::io::Read;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

// The format is private, ephemeral and single-process, hence no
// versioning/compatibility concerns (and no serde machinery needed).
fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

// None is encoded as u32::MAX length (a single string cannot realistically
// reach 4GiB, ClickHouse log messages are far smaller).
fn put_opt_str(buf: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => put_str(buf, s),
        None => buf.extend_from_slice(&u32::MAX.to_le_bytes()),
    }
}

fn encode_entry(entry: &LogEntry, buf: &mut Vec<u8>) {
    buf.extend_from_slice(
        &entry
            .event_time_microseconds
            .timestamp_micros()
            .to_le_bytes(),
    );
    buf.extend_from_slice(&entry.thread_id.to_le_bytes());
    put_str(buf, &entry.host_name);
    put_str(buf, &entry.level);
    put_str(buf, &entry.message);
    put_opt_str(buf, entry.display_host_name.as_deref());
    put_opt_str(buf, entry.query_id.as_deref());
    put_opt_str(buf, entry.logger_name.as_deref());
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.buf.split_at_checked(n)?;
        self.buf = tail;
        Some(head)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn str(&mut self) -> Option<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.bytes(len)?.to_vec()).ok()
    }

    fn opt_str(&mut self) -> Option<Option<String>> {
        if self.buf.len() >= 4 && self.buf[..4] == u32::MAX.to_le_bytes() {
            let _ = self.bytes(4);
            return Some(None);
        }
        Some(Some(self.str()?))
    }
}

fn decode_entry(buf: &[u8]) -> Option<LogEntry> {
    let mut r = Reader { buf };
    let micros = i64::from_le_bytes(r.bytes(8)?.try_into().unwrap());
    let thread_id = u64::from_le_bytes(r.bytes(8)?.try_into().unwrap());
    Some(LogEntry {
        event_time_microseconds: DateTime::from_timestamp_micros(micros)?.with_timezone(&Local),
        thread_id,
        host_name: r.str()?,
        level: r.str()?,
        message: r.str()?,
        display_host_name: r.opt_str()?,
        query_id: r.opt_str()?,
        logger_name: r.opt_str()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(n: u64) -> LogEntry {
        LogEntry {
            host_name: format!("host{}", n),
            display_host_name: n.is_multiple_of(2).then(|| format!("h{}", n)),
            event_time_microseconds: DateTime::from_timestamp_micros(1700000000000000 + n as i64)
                .unwrap()
                .with_timezone(&Local),
            thread_id: n,
            level: "Trace".to_string(),
            message: format!("message {} с юникодом", n),
            query_id: n.is_multiple_of(3).then(|| format!("query-{}", n)),
            logger_name: None,
        }
    }

    fn assert_entry_eq(a: &LogEntry, b: &LogEntry) {
        assert_eq!(a.host_name, b.host_name);
        assert_eq!(a.display_host_name, b.display_host_name);
        assert_eq!(a.event_time_microseconds, b.event_time_microseconds);
        assert_eq!(a.thread_id, b.thread_id);
        assert_eq!(a.level, b.level);
        assert_eq!(a.message, b.message);
        assert_eq!(a.query_id, b.query_id);
        assert_eq!(a.logger_name, b.logger_name);
    }

    #[test]
    fn codec_roundtrip() {
        for n in 0..6 {
            let e = entry(n);
            let mut buf = Vec::new();
            encode_entry(&e, &mut buf);
            assert_entry_eq(&decode_entry(&buf).unwrap(), &e);
        }
        // Empty strings and all-None options
        let e = LogEntry {
            host_name: String::new(),
            display_host_name: None,
            event_time_microseconds: DateTime::from_timestamp_micros(0)
                .unwrap()
                .with_timezone(&Local),
            thread_id: 0,
            level: String::new(),
            message: String::new(),
            query_id: None,
            logger_name: None,
        };
        let mut buf = Vec::new();
        encode_entry(&e, &mut buf);
        assert_entry_eq(&decode_entry(&buf).unwrap(), &e);
        // Truncated input must not panic
        for cut in 0..buf.len() {
            decode_entry(&buf[..cut]);
        }
    }

    #[test]
    fn append_order() {
        let mut store = LogStore::new();
        assert!(store.is_empty());
        assert!(store.with_entry(0, |_| ()).is_none());

        store.append((0..10).map(entry).collect());
        store.append((10..20).map(entry).collect());
        assert_eq!(store.len(), 20);
        for i in 0..20 {
            assert_entry_eq(
                &store.with_entry(i, |e| e.clone()).unwrap(),
                &entry(i as u64),
            );
        }
        assert!(store.with_entry(20, |_| ()).is_none());
    }

    #[test]
    fn insert_at_order() {
        let mut store = LogStore::new();
        // Descending mode: each fetch is newest-first and goes in front...
        store.insert_at(0, (20..30).map(entry).collect());
        store.insert_at(0, (0..10).map(entry).collect());
        // ...but later blocks of the same fetch go after the earlier ones
        store.insert_at(10, (10..20).map(entry).collect());
        assert_eq!(store.len(), 30);
        for i in 0..30 {
            assert_entry_eq(
                &store.with_entry(i, |e| e.clone()).unwrap(),
                &entry(i as u64),
            );
        }
    }

    #[test]
    fn window_boundaries() {
        let count = CACHE_WINDOW * 3 + 17;
        let mut store = LogStore::new();
        store.append((0..count as u64).map(entry).collect());
        // Jump around to force cache misses at both directions and boundaries
        for &i in &[count - 1, 0, count / 2, count - 1, 1, count / 2 + 1] {
            assert_eq!(store.with_entry(i, |e| e.thread_id).unwrap(), i as u64);
        }
        // Backward scan (reverse search pattern)
        for i in (0..count).rev() {
            assert_eq!(store.with_entry(i, |e| e.thread_id).unwrap(), i as u64);
        }
    }

    #[test]
    fn memory_fallback() {
        let mut store = LogStore::new();
        store.backing = Backing::Memory(Vec::new());
        store.append((5..10).map(entry).collect());
        store.insert_at(0, (0..5).map(entry).collect());
        assert_eq!(store.len(), 10);
        for i in 0..10 {
            assert_entry_eq(
                &store.with_entry(i, |e| e.clone()).unwrap(),
                &entry(i as u64),
            );
        }
    }
}
