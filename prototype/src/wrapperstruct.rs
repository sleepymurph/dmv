
macro_rules! from_deref {
    // $outer must be a path so that it can be called as a constructor in From.
    // For some reason it will not let you invoke a type as a constructor.
    ($outer:path => $inner:ty) => {

        use std;

        impl From<$inner> for $outer {
            fn from(inner: $inner) -> Self {
                $outer(inner)
            }
        }

        impl std::ops::Deref for $outer {
            type Target = $inner;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $outer {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;

    type StringMap = HashMap<String,String>;
    pub struct MapWrap(StringMap);
    from_deref!(MapWrap => StringMap);

    #[test]
    fn test_macro() {
        let str_map = StringMap::new();
        let mut wrap = MapWrap::from(str_map);
        wrap.insert("Hello".to_owned(), "World".to_owned());
        assert_eq!(wrap.get("Hello"), Some(&"World".to_owned()));
    }
}
