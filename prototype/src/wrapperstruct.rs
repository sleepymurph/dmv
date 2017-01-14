
macro_rules! impl_from {
    // For tuple structs
    ($from:ty => $into:tt) => {
        impl From<$from> for $into {
            fn from(f: $from) -> Self {
                $into(f)
            }
        }
    };
    // For structs with one named field
    ($from:ty => $into:tt, $field:tt) => {
        impl ::std::convert::From<$from> for $into {
            fn from(f: $from) -> Self {
                $into{ $field: f }
            }
        }
    };
}

macro_rules! impl_deref {
    // For tuple structs
    ($ptr:ty => $deref:tt) => { impl_deref!($ptr => $deref, 0); };
    // For structs with a named field
    ($ptr:ty => $deref:ty, $field:tt) => {
        impl ::std::ops::Deref for $ptr {
            type Target = $deref;
            fn deref(&self) -> &Self::Target {
                &self.$field
            }
        }

        impl ::std::ops::DerefMut for $ptr {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.$field
            }
        }
    };
}

macro_rules! wrapper_struct {
    ($(#[$attr:meta])* pub struct $wrapper:tt($inner:ty);) => {
        $(#[$attr])*
        pub struct $wrapper($inner);
        impl_from!($inner => $wrapper);
        impl_deref!($wrapper => $inner);
    };
    ($(#[$attr:meta])* struct $wrapper:tt($inner:ty);) => {
        $(#[$attr])*
        struct $wrapper($inner);
        impl_from!($inner => $wrapper);
        impl_deref!($wrapper => $inner);
    };

    ($(#[$attr:meta])* pub struct $wrapper:tt{$field:ident: $inner:ty}) => {
        $(#[$attr])*
        pub struct $wrapper{ $field: $inner }
        impl_from!($inner => $wrapper, $field);
        impl_deref!($wrapper => $inner, $field);
    };
    ($(#[$attr:meta])* struct $wrapper:tt{$field:ident: $inner:ty};) => {
        $(#[$attr])*
        struct $wrapper{ $field: $inner }
        impl_from!($inner => $wrapper, $field);
        impl_deref!($wrapper => $inner, $field);
    };
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;
    type StringMap = HashMap<String, String>;

    struct MapWrapTupleStruct(StringMap);
    impl_from!(StringMap => MapWrapTupleStruct);
    impl_deref!(MapWrapTupleStruct => StringMap);

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
    impl_deref!(MapWrapStruct => StringMap, inner);

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
