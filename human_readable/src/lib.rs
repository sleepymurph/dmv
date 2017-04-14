//! Conversions to human-readable numbers using SI prefixes

/// Converts an integer into a human-readable byte size with base-2 prefix
///
/// Numbers are given with one decimal place. Rounding is to closest tenth, as
/// with the `round()` method of float types. So 2047 bytes is rounded up to 2.0
/// KiB, and 2049 bytes is rounded down.
///
/// ```
/// use human_readable::human_bytes;
///
/// // Bytes endings are right-padded for alignment
/// assert_eq!(human_bytes(0_u8),                   "0 bytes");
/// assert_eq!(human_bytes(999_u32),              "999 bytes");
/// assert_eq!(human_bytes(1000_u32),            "1000 bytes");
///
/// assert_eq!(human_bytes(1024_u32),               "1.0 KiB");
/// assert_eq!(human_bytes((1024 + 512) as u64),    "1.5 KiB");
/// assert_eq!(human_bytes((1024.0 * 1.9) as u64),  "1.9 KiB");
///
/// // Values are rounded up or down to nearest tenth
/// assert_eq!(human_bytes(2047_u32),               "2.0 KiB");
/// assert_eq!(human_bytes(2048_u32),               "2.0 KiB");
/// assert_eq!(human_bytes(2049_u32),               "2.0 KiB");
///
/// // Sizes go up to exabytes, because larger sizes won't fit in a u64
/// assert_eq!(human_bytes(1024_u32 * 1024_u32),    "1.0 MiB");
/// assert_eq!(human_bytes(2u32 << 20),             "2.0 MiB");
/// assert_eq!(human_bytes(1u64 << 30),             "1.0 GiB");
/// assert_eq!(human_bytes(1u64 << 40),             "1.0 TiB");
/// assert_eq!(human_bytes(1u64 << 50),             "1.0 PiB");
/// assert_eq!(human_bytes(1u64 << 60),             "1.0 EiB");
///
/// // Optimal padding width is 10
/// assert_eq!(human_bytes((1023 * 1024) as u32), "1023.0 KiB");
/// assert_eq!(human_bytes((1023 * 1024) as u32).len(), 10);
/// ```
///
pub fn human_bytes<N: Into<u64>>(num: N) -> String {

    let (size, prefix) = human_bytes_f(num);
    match prefix {
        "" => format!("{:0.0} bytes", size),
        _ => format!("{:0.1} {}B", size, prefix),
    }
}

pub fn human_bytes_f<N: Into<u64>>(num: N) -> (f64, &'static str) {

    let prefixes = ["", "ki", "Mi", "Gi", "Ti", "Pi", "Ei"];
    let pindex_limit = prefixes.len() - 1;
    let num: u64 = num.into();

    let mut mant = num;
    let mut rem = 0;
    let mut pindex = 0;

    while mant >= 1024 && pindex < pindex_limit {
        rem = mant & 0b0011_1111_1111_u64;
        mant >>= 10;
        pindex += 1;
    }

    let f = (mant as f64) + (rem as f64 / 1024_f64);
    (f, prefixes[pindex])
}
