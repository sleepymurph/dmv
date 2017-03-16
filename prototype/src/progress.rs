use human_readable::human_bytes;
use std::fmt;
use std::io;
use std::io::Read;
use std::io::Write;
use std::io::stderr;
use std::sync::*;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub struct ProgressCounter {
    desc: String,
    start: Instant,
    total: u64,
    count: RwLock<u64>,
}

impl ProgressCounter {
    pub fn new<S>(desc: S, total: u64) -> Self
        where S: Into<String>
    {
        ProgressCounter {
            desc: desc.into(),
            start: Instant::now(),
            total: total,
            count: RwLock::new(0),
        }
    }
    pub fn add(&self, progress: u64) {
        *self.count.write().unwrap() += progress
    }
    pub fn read(&self) -> ProgressReport {
        let count = *self.count.read().unwrap();
        ProgressReport {
            desc: self.desc.as_str(),
            total: self.total,
            count: count,
            finished: count >= self.total,
            elapsed: Instant::now().duration_since(self.start),
        }
    }
}

pub struct ProgressReport<'a> {
    desc: &'a str,
    total: u64,
    count: u64,
    finished: bool,
    elapsed: Duration,
}

impl<'a> fmt::Display for ProgressReport<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:", self.desc)?;
        write!(f, " {:>10}/{:>10}",
               human_bytes(self.count),
               human_bytes(self.total))?;

        if self.total == 0 {
            return Ok(());
        }

        let secs = self.elapsed.as_secs() as f32 +
                   self.elapsed.subsec_nanos() as f32 / 1e9;
        let percent = self.count as f32 / self.total as f32 * 100_f32;
        write!(f, " {:5.1}%", percent)?;
        write!(f, " {:0.1}s", secs)?;

        if secs >= 0.5 {
            let per_sec = self.count as f32 / secs;
            let remain_secs = (self.total - self.count) as f32 / per_sec;
            write!(f, " {:>10}/s", human_bytes(per_sec as u64))?;
            if !self.finished {
                write!(f, " {:0.0}s", remain_secs)?;
            }
        }
        if self.finished {
            write!(f, " done")?;
        }
        Ok(())
    }
}

const ANSI_CLEAR_TO_END: &'static str = "\x1b[0J";
fn ansi_up_lines(n: usize) -> String { format!("\x1b[{}F", n) }


pub fn std_err_watch(p: Arc<ProgressCounter>) {
    let refresh_per_sec = 10;
    let sleep = Duration::from_millis(1000 / refresh_per_sec);
    loop {
        let p = p.read();
        write!(stderr(), "{}", ANSI_CLEAR_TO_END).unwrap();
        writeln!(stderr(), " {}", p).unwrap();
        if p.finished {
            break;
        } else {
            write!(stderr(), "{}", ansi_up_lines(1)).unwrap();
            thread::sleep(sleep);
        }
    }
}


pub struct ProgressReader<'a, R: Read> {
    p: &'a ProgressCounter,
    r: R,
}
impl<'a, R: Read> ProgressReader<'a, R> {
    pub fn new(r: R, p: &'a ProgressCounter) -> Self {
        ProgressReader { r: r, p: p }
    }
}
impl<'a, R: Read> Read for ProgressReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.r.read(buf)?;
        self.p.add(count as u64);
        Ok(count)
    }
}
