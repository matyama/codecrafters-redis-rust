use crate::{Command, DataType, Error};

#[derive(Debug)]
#[repr(transparent)]
pub struct Select(usize);

impl TryFrom<&[DataType]> for Select {
    type Error = Error;

    #[inline]
    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        args.first()
            .ok_or_else(|| Error::WrongNumArgs("select"))
            .and_then(usize::try_from)
            .map(Self)
            .map_err(|e| match e {
                Error::NegInt(_) => Error::err("DB index is out of range"),
                e => e,
            })
    }
}

impl From<Select> for Command {
    #[inline]
    fn from(Select(index): Select) -> Self {
        Self::Select(index)
    }
}
