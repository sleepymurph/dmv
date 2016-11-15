#![cfg(test)]

extern crate rand;
use self::rand::{Rng, SeedableRng, Generator, XorShiftRng};

use std::iter::IntoIterator;

pub struct RandBytes {
    rng: XorShiftRng,
}

impl RandBytes {
    pub fn new() -> Self {
        RandBytes {
            rng: XorShiftRng::from_seed([255, 20, 110, 0]),
        }
    }
}

impl<'a> IntoIterator for &'a mut RandBytes {
    type Item = u8;
    type IntoIter = Generator<'a, u8, XorShiftRng>;

    fn into_iter(self) -> Self::IntoIter {
        self.rng.gen_iter::<u8>()
    }
}


#[test]
fn test_rand_bytes_same_every_time() {
    let mut rng = RandBytes::new();
    let mut rand_bytes: Vec<u8> = Vec::new();
    rand_bytes.extend(rng.into_iter().take(10));
    assert_eq!(rand_bytes, [7, 179, 173, 173, 109, 225, 168, 201, 120, 240]);
}
