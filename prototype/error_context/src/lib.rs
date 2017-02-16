use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct ErrorContext {
    context: String,
    cause: Box<Error + Send + Sync>,
}

impl Error for ErrorContext {
    fn description(&self) -> &str { "(context)" }
    fn cause(&self) -> Option<&Error> { Some(&*self.cause) }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.context, self.cause)
    }
}

pub trait AddErrorContext<T, E> {
    fn err_context<F, S>(self, context: F) -> Result<T, ErrorContext>
        where F: FnOnce() -> S,
              S: Into<String>;
}

impl<T, E> AddErrorContext<T, E> for Result<T, E>
    where E: Into<Box<Error + Send + Sync>>
{
    fn err_context<F, S>(self, context: F) -> Result<T, ErrorContext>
        where F: FnOnce() -> S,
              S: Into<String>
    {
        match self {
            Ok(t) => Ok(t),
            Err(e) => {
                Err(ErrorContext {
                    context: context().into(),
                    cause: e.into(),
                })
            }
        }
    }
}

use std::io;

impl From<ErrorContext> for io::Error {
    fn from(e: ErrorContext) -> Self { io::Error::new(io::ErrorKind::Other, e) }
}


#[cfg(test)]
mod test {
    use std::io;
    use super::*;

    fn failed_io_op() -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Other, "Not enough context"))
    }

    fn assert_error_contains<T, E>(result: Result<T, E>, expected: &str)
        where E: Into<Box<Error + Send + Sync>>
    {
        match result {
            Ok(_) => panic!("Expected error, got Ok()"),
            Err(e) => {
                let e = e.into();
                let display = format!("{}", e);
                if !display.contains(expected) {
                    panic!("Expected error to contain '{}', got: {}",
                           expected,
                           display);
                }
            }
        }
    }

    #[test]
    fn test_return_inside_closure() {
        let x = true;
        let closure = || {
            if x {
                return Err(());
            };
            Ok(())
        };
        let result = closure();
        assert_eq!(result, Err(()));
    }

    #[test]
    fn test_trait_add_context() {
        let result = failed_io_op().err_context(|| "Added context");
        assert_error_contains(result, "Added context");
    }

    #[test]
    fn test_context_err_back_into_io_error() {
        fn inner() -> io::Result<()> {
            failed_io_op().err_context(|| "Added context")?;
            Ok(())
        }
        let result = inner();
        assert_error_contains(result, "Added context");
    }

    #[test]
    fn test_try_inside_inner() {
        fn inner() -> io::Result<()> {
            failed_io_op()?;
            Ok(())
        };
        let result = inner();
        assert_error_contains(result, "Not enough context");
    }

    #[test]
    fn test_try_inside_closure() {
        let closure = || -> Result<(), Box<Error + Send + Sync>> {
            failed_io_op()?;
            Ok(())
        };
        let result = closure();
        assert_error_contains(result, "Not enough context");
    }
}
