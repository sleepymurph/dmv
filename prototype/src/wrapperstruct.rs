
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
    (struct $wrapper:tt($inner:ty);) => {
        struct $wrapper($inner);
        impl_from!($inner => $wrapper);
        impl_deref!($wrapper => $inner);
    }
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
    }

}
