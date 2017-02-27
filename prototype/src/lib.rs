//! Library for master's thesis prototype

// error_chain uses macro with a lot of recursion
#![recursion_limit = "1024"]

// Dependencies
extern crate byteorder;
extern crate crypto;
extern crate disk_backed;
#[macro_use]
extern crate error_chain;
extern crate human_readable;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate regex;
extern crate rustc_serialize;
#[macro_use]
extern crate wrapper_struct;

// Test-only dependencies
#[cfg(test)]
#[macro_use]
extern crate testutil;
#[cfg(test)]
#[macro_use]
extern crate hamcrest;


/// Write to stderr
macro_rules! stderrln {
    ( $($arg:expr),* ) => {{
        use ::std::io::Write;
        writeln!(::std::io::stderr(), $($arg),*)
            .expect("could not write to stderr")
    }};
}


/// Create and populate a map (or any struct with an insert method)
///
/// Creates the struct using the given `$map_new` expression, then populates it
/// with the repeating key-value pairs (`$k=>$v,`) using the `insert` method.
///
/// So this macro works with any struct that has a two-parameter `insert`
/// method.
///
/// ```
/// #[macro_use]
/// extern crate prototype;
/// use std::collections::HashMap;
///
/// fn main() {
///     let map_by_macro = map!{ HashMap::new(),
///         "hello" => "world",
///         "foo" => "bar",
///     };
///
///     // Equivalent to the following
///     let mut map_normal = HashMap::new();
///     map_normal.insert("hello", "world");
///     map_normal.insert("foo", "bar");
///
///     assert_eq!(map_by_macro, map_normal);
///
///     // Can also be used to add to an existing map
///     map!{ &mut map_normal,
///         "bar" => "baz",
///     };
///
///     assert_eq!(map_normal.get("bar"), Some(&"baz"));
/// }
/// ```
#[macro_export]
macro_rules! map {
    ( $map_new:expr, $( $k:expr => $v:expr,)* ) => {{
            let mut map = $map_new;
            $( map.insert($k,$v); )*
            map
    }};
}

// Low-level code that isn't specific to the project.
// Could potentially be spun off into their own crates.
pub mod encodable;
pub mod fsutil;

// Project-specific code
pub mod error;
pub mod constants;
#[macro_use]
pub mod dag;
pub mod rolling_hash;
pub mod object_store;
#[macro_use]
pub mod cache;
pub mod ignore;
pub mod fs_transfer;
pub mod work_dir;
pub mod find_repo;
pub mod cmd;
