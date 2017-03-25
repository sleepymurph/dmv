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

pub struct StopWatch {
    start: Instant,
    stop: Option<Instant>,
}
impl StopWatch {
    pub fn new() -> Self {
        StopWatch {
            start: Instant::now(),
            stop: None,
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.stop.unwrap_or(Instant::now()).duration_since(self.start)
    }
    pub fn stop(&mut self) -> Duration {
        self.stop = Some(Instant::now());
        self.elapsed()
    }
    pub fn float_secs(elapsed: &Duration) -> f32 {
        elapsed.as_secs() as f32 + elapsed.subsec_nanos() as f32 / 1e9
    }
}

pub struct ProgressCounter {
    desc: String,
    start: Instant,
    estimate: u64,
    state: RwLock<(u64, bool)>,
}

impl ProgressCounter {
    pub fn arc<S>(desc: S, estimate: u64) -> Arc<Self>
        where S: Into<String>
    {
        Arc::new(ProgressCounter {
            desc: desc.into(),
            start: Instant::now(),
            estimate: estimate,
            state: RwLock::new((0, false)),
        })
    }
    pub fn add(&self, progress: u64) {
        self.state.write().unwrap().0 += progress
    }
    pub fn finish(&self) { self.state.write().unwrap().1 = true; }
    pub fn read(&self) -> ProgressReport {
        let state = *self.state.read().unwrap();
        ProgressReport {
            desc: self.desc.as_str(),
            estimate: self.estimate,
            count: state.0,
            finished: state.1,
            elapsed: Instant::now().duration_since(self.start),
        }
    }
}

pub struct ProgressReport<'a> {
    desc: &'a str,
    estimate: u64,
    count: u64,
    finished: bool,
    elapsed: Duration,
}

impl<'a> fmt::Display for ProgressReport<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:", self.desc)?;
        write!(f,
               " {:>10}/{:>10}",
               human_bytes(self.count),
               human_bytes(self.estimate))?;

        if self.estimate == 0 {
            return Ok(());
        }

        let secs = self.elapsed.as_secs() as f32 +
                   self.elapsed.subsec_nanos() as f32 / 1e9;
        let percent = self.count as f32 / self.estimate as f32 * 100_f32;
        write!(f, " {:5.1}%", percent)?;
        write!(f, " {:0.1}s", secs)?;

        if secs >= 0.5 {
            let per_sec = self.count as f32 / secs;
            let remain_secs = (self.estimate - self.count) as f32 / per_sec;
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
        writeln!(stderr(), "  {}", p).unwrap();
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


pub struct ProgressWriter<'a, W: Write> {
    p: &'a ProgressCounter,
    w: W,
}
impl<'a, W: Write> ProgressWriter<'a, W> {
    pub fn new(w: W, p: &'a ProgressCounter) -> Self {
        ProgressWriter { w: w, p: p }
    }
    pub fn into_inner(self) -> W { self.w }
}
impl<'a, W: Write> Write for ProgressWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.w.write(buf)?;
        self.p.add(count as u64);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> { self.w.flush() }
}
