//! Utilities for dealing with maps

use std::cmp::Ordering;
use std::iter::Peekable;

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
/// extern crate dmv;
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


/// Multiplex two (K,V) iterators as one (K,Option<A>,Option<B>) iterator
///
/// Both iterators must run in ascending key order for the zipping to work
/// properly. So this is primarily useful for comparing two BTreeMaps.
///
pub fn mux<K, A, B, IA, IB>(a: IA, b: IB) -> Mux<K, A, B, IA, IB>
    where IA: Iterator<Item = (K, A)>,
          IB: Iterator<Item = (K, B)>,
          K: Ord
{
    Mux {
        a: a.peekable(),
        b: b.peekable(),
    }
}

pub struct Mux<K, A, B, IA, IB>
    where IA: Iterator<Item = (K, A)>,
          IB: Iterator<Item = (K, B)>,
          K: Ord
{
    a: Peekable<IA>,
    b: Peekable<IB>,
}

enum OnDeck {
    Both(Ordering),
    Left,
    Right,
    Neither,
}

impl<K, A, B, IA, IB> Iterator for Mux<K, A, B, IA, IB>
    where IA: Iterator<Item = (K, A)>,
          IB: Iterator<Item = (K, B)>,
          K: Ord
{
    type Item = (K, Option<A>, Option<B>);
    fn next(&mut self) -> Option<Self::Item> {
        use self::OnDeck::*;
        use std::cmp::Ordering::*;

        let on_deck = match (self.a.peek(), self.b.peek()) {
            (Some(&(ref ka, _)), Some(&(ref kb, _))) => Both(ka.cmp(kb)),
            (Some(_), None) => Left,
            (None, Some(_)) => Right,
            (None, None) => Neither,
        };
        match on_deck {
            Both(Equal) => {
                match (self.a.next(), self.b.next()) {
                    (Some((k, a)), Some((_, b))) => Some((k, Some(a), Some(b))),
                    _ => unreachable!(),
                }
            }
            Both(Less) | Left => {
                match self.a.next() {
                    Some((k, v)) => Some((k, Some(v), None)),
                    None => unreachable!(),
                }
            }
            Both(Greater) | Right => {
                match self.b.next() {
                    Some((k, v)) => Some((k, None, Some(v))),
                    None => unreachable!(),
                }
            }
            Neither => None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use super::*;

    #[test]
    fn test_mux() {
        let a = map!{BTreeMap::new(),
            1 => "a1",
            2 => "a2",
        };
        let b = map!{BTreeMap::new(),
            1 => "b1",
            3 => "b3",
            5 => "b5",
        };
        let muxed = mux(a.into_iter(), b.into_iter()).collect::<Vec<_>>();
        assert_eq!(muxed,
                   vec![(1, Some("a1"), Some("b1")),
                        (2, Some("a2"), None),
                        (3, None, Some("b3")),
                        (5, None, Some("b5"))]);
    }
}
