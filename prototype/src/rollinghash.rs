pub type RollingHashValue = u32;

pub struct RollingHash {
    value: RollingHashValue,
    window: Vec<u8>,
    window_size: usize,
    pos: usize,
    full: bool,
}

impl RollingHash {
    pub fn new(window_size: usize) -> Self {
        let mut window = Vec::with_capacity(window_size);
        window.resize(window_size, 0);
        RollingHash {
            value: 0,
            window: vec![0; window_size],
            window_size: window_size,
            pos: 0,
            full: false,
        }
    }

    pub fn reset(&mut self) {
        self.window.clear();
        self.window.resize(self.window_size, 0);
        self.pos = 0;
        self.full = false;
        self.value = 0;
    }

    pub fn slide(&mut self, byte: u8) {
        let outgoing = self.window[self.pos] as RollingHashValue;
        let incoming = byte as RollingHashValue;
        self.value = self.value - outgoing + incoming;
        self.window[self.pos] = byte;
        self.pos = (self.pos + 1) % self.window_size;
        if self.pos == 0 {
            self.full = true;
        }
    }

    pub fn full(&self) -> bool {
        self.full
    }

    pub fn value(&self) -> RollingHashValue {
        self.value
    }
}

const WINDOW_SIZE: usize = 4096;
const MATCH_BITS: RollingHashValue = 13;

pub struct ChunkFlagger {
    hasher: RollingHash,
    mask: RollingHashValue,
}

impl ChunkFlagger {
    pub fn new() -> Self {
        let mut mask: RollingHashValue = 1;
        for _ in 0..MATCH_BITS {
            mask = (mask << 1) + 1;
        }
        ChunkFlagger {
            hasher: RollingHash::new(WINDOW_SIZE),
            mask: mask,
        }
    }

    /// Adds a byte to the hash, returns true if this byte triggers a flag
    pub fn slide(&mut self, byte: u8) -> bool {
        self.hasher.slide(byte);

        if self.hasher.full() && (self.hasher.value() & self.mask) == 0 {
            self.hasher.reset();
            true
        } else {
            false
        }
    }

    /// Slides across the buffer, returns a list of flag positions
    ///
    /// Note that the positions point to the bytes that trigger the flag. These
    /// positions mark the **end** of the chunk.
    pub fn slide_over(&mut self, buf: &[u8]) -> Vec<usize> {
        let mut boundaries = Vec::new();
        for bufpos in 0..buf.len() {
            if self.slide(buf[bufpos]) {
                boundaries.push(bufpos);
            }
        }
        boundaries
    }
}

#[cfg(test)]
mod test {

    extern crate rand;
    use self::rand::{Rng, SeedableRng, XorShiftRng};

    use super::*;

    fn rng() -> XorShiftRng {
        XorShiftRng::from_seed([255,20,110,0])
    }

    #[test]
    /// This test shows that the Rabin value increases slowly after a reset
    ///
    /// It demonstrates why you need to fill the window before checking the
    /// hash.
    fn test_rolling_hash_values() {
        let mut rng = rng();

        let mut hasher = RollingHash::new(256);
        let mut hashvals: Vec<RollingHashValue> = Vec::new();
        for byte in rng.gen_iter::<u8>().take(10) {
            hasher.slide(byte);
            hashvals.push(hasher.value());
        }

        // Uncomment to see all hash values
        // assert_eq!(hashvals, []);

        assert!(hashvals[0] < hashvals[1]);
        assert!(hashvals[1] < hashvals[2]);
    }


    fn mean_std<'a, I>(input: I) -> (usize, usize)
        where I: Iterator<Item = &'a usize>
    {
        let (mut n, mut sum, mut sumsq) = (0, 0, 0);
        for &x in input {
            n += 1;
            sum += x;
            sumsq += x * x;
        }
        let mean = sum / n;
        let var = (sumsq - sum * sum / n) / (n - 1);
        let std = (var as f64).sqrt() as usize;
        (mean, std)
    }

    #[test]
    fn test_mean_std() {
        let input: &[usize] = &[2, 4, 4, 4, 5, 5, 7, 9];
        let (expected_mean, expected_std) = (5, 2);
        let (mean, std) = mean_std(input.iter());
        assert_eq!((mean, std), (expected_mean, expected_std));
    }

    const CHUNK_TARGET_SIZE: usize = 15 * 1024;

    #[test]
    fn test_chunk_target_size() {
        const CHUNK_TARGET_MIN: usize = 10 * 1024;
        const CHUNK_TARGET_MAX: usize = 25 * 1024;
        const ACCEPTABLE_DEVIATION: usize = 25 * 1024;
        const CHUNK_REPEAT: usize = 100;

        let mut rng = rng();

        let mut flagger = ChunkFlagger::new();
        let mut chunk_offsets: Vec<usize> = Vec::new();
        for (count, byte) in rng.gen_iter::<u8>()
            .take(CHUNK_TARGET_SIZE * CHUNK_REPEAT)
            .enumerate() {
            if flagger.slide(byte) {
                chunk_offsets.push(count);
            }
        }
        assert!(chunk_offsets.len() > 0,
                "Expected input to be broken in to chunks, but no chunks \
                 were found.");

        let mut chunk_sizes: Vec<usize> = Vec::new();
        chunk_sizes.push(chunk_offsets[0]);
        for i in 1..chunk_offsets.len() {
            chunk_sizes.push(chunk_offsets[i] - chunk_offsets[i - 1]);
        }

        // Uncomment to get all chunk sizes
        // assert_eq!(chunk_sizes, []);

        let (mean, std) = mean_std(chunk_sizes.iter());
        assert!(CHUNK_TARGET_MIN < mean && mean < CHUNK_TARGET_MAX,
                format!("Expected mean chunk size between {} and {}. \
                         Got {}",
                        CHUNK_TARGET_MIN,
                        CHUNK_TARGET_MAX,
                        mean));
        assert!(std < ACCEPTABLE_DEVIATION,
                format!("Expected standard deviation of chunk sizes to \
                         be less than {}. Got {}",
                        ACCEPTABLE_DEVIATION,
                        std));
    }


    #[test]
    fn test_chunk_slide_over() {
        let mut rng = rng();

        let mut data: Vec<u8> = Vec::new();
        data.extend(rng.gen_iter::<u8>()
            .take(10 * CHUNK_TARGET_SIZE));

        let mut flagger = ChunkFlagger::new();
        let chunk_offsets = flagger.slide_over(&data);

        // Uncomment to see all offsets
        // assert_eq!(chunk_offsets, [12345]);

        assert!(chunk_offsets.len() >= 4,
                format!("Expected several chunk offsets returned. Got: {:?}",
                        chunk_offsets));
    }
}
