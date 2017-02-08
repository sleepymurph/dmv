/// Quickly implement the From trait for single-field structs
///
/// Works with one-field structs, or one-field tuple structs.
///
/// ```
/// #[macro_use]
/// extern crate prototype;
///
/// struct StringWrapTuple(String);
/// impl_from!(String => StringWrapTuple);
///
/// struct StringWrapStruct{inner: String}
/// impl_from!(String => StringWrapStruct, inner);
///
/// fn main() {
///     let s = "Hello world!".to_owned();
///     let wt = StringWrapTuple::from(s.clone());
///     let ws = StringWrapStruct::from(s.clone());
///     assert_eq!(wt.0, ws.inner);
/// }
/// ```
///
/// Macro hygiene note: Ideally, $into would be a type (ty), but as of Rust
/// 1.14.0, the compiler does not allow types to be used in the constructor
/// expressions inside the macros (like `$into{ $field: f}`). It will give an
/// error that it expected an expression but found the type. We compromise by
/// specifying a path instead.
///
#[macro_export]
macro_rules! impl_from {
    // For tuple structs
    ($from:ty => $into:path) => {
        impl From<$from> for $into {
            fn from(f: $from) -> Self {
                $into(f)
            }
        }
    };
    // For structs with one named field
    ($from:ty => $into:path, $field:ident) => {
        impl ::std::convert::From<$from> for $into {
            fn from(f: $from) -> Self {
                $into{ $field: f }
            }
        }
    };
}

/// Quickly implement Deref by referring to a single field
///
/// Works with structs, or tuple structs.
///
/// To also implement DerefMut, see the `impl_deref_mut!` macro.
///
/// ```
/// #[macro_use]
/// extern crate prototype;
///
/// struct StringWrapTuple(String);
/// impl_deref!(StringWrapTuple => String);
///
/// struct StringWrapStruct{inner: String}
/// impl_deref!(StringWrapStruct => String, inner);
///
/// fn main() {
///     let s = "Hello".to_owned();
///
///     let wt = StringWrapTuple(s.clone());
///     assert_eq!(*wt, s);
///
///     let ws = StringWrapStruct{inner: s.clone()};
///     assert_eq!(*ws, s);
/// }
/// ```
///
/// Macro hygiene note: Ideally, $field would be an identifier (ident), but as
/// of Rust 1.14.0, the compiler does not accept integers as identifiers, so so
/// we would not be able to use this macro with tuple structs (like
/// `impl_deref!(TwoFieldTupleStruct => String, 1);`).
///
#[macro_export]
macro_rules! impl_deref {
    // For tuple structs
    ($ptr:ty => $deref:ty) => { impl_deref_mut!($ptr => $deref, 0); };
    // For structs with a named field
    ($ptr:ty => $deref:ty, $field:tt) => {
        impl ::std::ops::Deref for $ptr {
            type Target = $deref;
            fn deref(&self) -> &Self::Target {
                &self.$field
            }
        }
    };
}

/// Quickly implement Deref and DerefMut by referring to a single field
///
/// Works with structs, or tuple structs.
///
/// ```
/// #[macro_use]
/// extern crate prototype;
///
/// struct StringWrapTuple(String);
/// impl_deref_mut!(StringWrapTuple => String);
///
/// struct StringWrapStruct{inner: String}
/// impl_deref_mut!(StringWrapStruct => String, inner);
///
/// fn main() {
///     let s = "Hello".to_owned();
///
///     let mut wt = StringWrapTuple(s.clone());
///     wt.push_str(" world!");
///     assert_eq!(wt.0, "Hello world!");
///
///     let mut ws = StringWrapStruct{inner: s.clone()};
///     ws.push_str(" world!");
///     assert_eq!(ws.inner, "Hello world!");
/// }
/// ```
///
/// Macro hygiene note: Ideally, $field would be an identifier (ident), but as
/// of Rust 1.14.0, the compiler does not accept integers as identifiers, so so
/// we would not be able to use this macro with tuple structs (like
/// `impl_deref_mut!(TwoFieldTupleStruct => String, 1);`).
///
#[macro_export]
macro_rules! impl_deref_mut {
    // For tuple structs
    ($ptr:ty => $deref:ty) => { impl_deref_mut!($ptr => $deref, 0); };
    // For structs with a named field
    ($ptr:ty => $deref:ty, $field:tt) => {
        impl_deref!($ptr => $deref, $field);

        impl ::std::ops::DerefMut for $ptr {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.$field
            }
        }
    };
}

/// Easily create simple structs that act as transparent wrappers
///
/// Define a single-field struct or single-field tuple struct inside this macro,
/// and it will automatically implement From, Deref, and DerefMut appropriately.
///
/// ```
/// #[macro_use]
/// extern crate prototype;
///
/// wrapper_struct!{
///     /// Simple string wrapper (tuple struct)
///     pub struct StringWrapTuple(String);
/// }
///
/// wrapper_struct!{
///     /// Simple string wrapper (with named field)
///     pub struct StringWrapStruct{inner: String}
/// }
///
/// fn main() {
///     let s = "Hello".to_owned();
///
///     let mut wt = StringWrapTuple::from(s.clone());
///     wt.push_str(" world!");
///     assert_eq!(wt.0, "Hello world!");
///
///     let mut ws = StringWrapStruct::from(s.clone());
///     ws.push_str(" world!");
///     assert_eq!(ws.inner, "Hello world!");
/// }
/// ```
///
#[macro_export]
macro_rules! wrapper_struct {
    ($(#[$attr:meta])* pub struct $wrapper:ident($inner:ty);) => {
        $(#[$attr])*
        pub struct $wrapper($inner);
        impl_from!($inner => $wrapper);
        impl_deref_mut!($wrapper => $inner);
    };
    ($(#[$attr:meta])* struct $wrapper:ident($inner:ty);) => {
        $(#[$attr])*
        struct $wrapper($inner);
        impl_from!($inner => $wrapper);
        impl_deref_mut!($wrapper => $inner);
    };

    ($(#[$attr:meta])* pub struct $wrapper:ident{$field:ident: $inner:ty}) => {
        $(#[$attr])*
        pub struct $wrapper{ $field: $inner }
        impl_from!($inner => $wrapper, $field);
        impl_deref_mut!($wrapper => $inner, $field);
    };
    ($(#[$attr:meta])* struct $wrapper:ident{$field:ident: $inner:ty};) => {
        $(#[$attr])*
        struct $wrapper{ $field: $inner }
        impl_from!($inner => $wrapper, $field);
        impl_deref_mut!($wrapper => $inner, $field);
    };
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;
    type StringMap = HashMap<String, String>;

    struct MapWrapTupleStruct(StringMap);
    impl_from!(StringMap => MapWrapTupleStruct);
    impl_deref_mut!(MapWrapTupleStruct => StringMap);

    #[test]
    fn test_impl_from_tuple() {
        let str_map = StringMap::new();
        let mut wrap = MapWrapTupleStruct::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
        wrap.0.insert("Hello".to_owned(), "World".to_owned());
    }

    struct MapWrapStruct {
        inner: StringMap,
    }
    impl_from!(StringMap => MapWrapStruct, inner);
    impl_deref_mut!(MapWrapStruct => StringMap, inner);

    #[test]
    fn test_impl_from_field() {
        let str_map = StringMap::new();
        let mut wrap = MapWrapStruct::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
        wrap.inner.insert("Hello".to_owned(), "World".to_owned());
    }

    wrapper_struct!{
        struct MapWrapDefinedInMacro(StringMap);
    }

    #[test]
    fn test_wrapper_struct_macro() {
        let str_map = StringMap::new();
        let mut wrap = MapWrapDefinedInMacro::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
        wrap.0.insert("Hello".to_owned(), "World".to_owned());
    }

    wrapper_struct!{
        pub struct PubMapWrapDefinedInMacro(StringMap);
    }

    #[test]
    fn test_wrapper_struct_macro_pub() {
        let str_map = StringMap::new();
        let mut wrap = PubMapWrapDefinedInMacro::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
        wrap.0.insert("Hello".to_owned(), "World".to_owned());
    }

    wrapper_struct!{
        /// Wrapper struct defined by macro with comments and attributes
        #[derive(Eq,PartialEq,Debug)]
        pub struct PubMapWrapWithCommentsAndAttributes(StringMap);
    }

    #[test]
    fn test_wrapper_struct_macro_pub_comments() {
        let str_map = StringMap::new();
        let mut wrap = PubMapWrapWithCommentsAndAttributes::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
        wrap.0.insert("Hello".to_owned(), "World".to_owned());
    }

    wrapper_struct!{
        /// Wrapper struct defined by macro with comments and attributes
        #[derive(Eq,PartialEq,Debug)]
        pub struct MapWrapWithField{map: StringMap}
    }

    #[test]
    fn test_wrapper_struct_with_field_name() {
        let str_map = StringMap::new();
        let mut wrap = MapWrapWithField::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
        wrap.map.insert("Hello".to_owned(), "World".to_owned());
    }

}
