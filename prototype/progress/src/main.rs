use std::fmt;
use std::io::Write;
use std::io::stderr;
use std::sync::*;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub struct ProgressCounter {
    start: Instant,
    total: u64,
    count: RwLock<u64>,
}

impl ProgressCounter {
    pub fn new(total: u64) -> Self {
        ProgressCounter {
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
            total: self.total,
            count: count,
            finished: count >= self.total,
            elapsed: Instant::now().duration_since(self.start),
        }
    }
}

pub struct ProgressReport {
    total: u64,
    count: u64,
    finished: bool,
    elapsed: Duration,
}

impl fmt::Display for ProgressReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let secs = self.elapsed.as_secs() as f32 +
                   self.elapsed.subsec_nanos() as f32 / 1e9;
        let percent = self.count as f32 / self.total as f32 * 100_f32;
        write!(f, "{:4}/{:4}", self.count, self.total)?;
        write!(f, " {:5.1}%", percent)?;
        write!(f, " {:0.1}s", secs)?;
        if secs >= 0.5 {
            let per_sec = self.count as f32 / secs;
            let remain_secs = (self.total - self.count) as f32 / per_sec;
            write!(f, " {:0.0} b/s {:0.0}s", per_sec, remain_secs)?;
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


fn main() {
    let progress = Arc::new(ProgressCounter::new(2000));
    let p2 = progress.clone();

    let worker = thread::spawn(move || {
        while !progress.read().finished {
            progress.add(20);
            thread::sleep(Duration::from_millis(20));
        }
    });

    let reporter = thread::spawn(move || std_err_watch(p2));

    worker.join().unwrap();
    reporter.join().unwrap();
}
