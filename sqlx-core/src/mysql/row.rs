use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;

use crate::decode::Decode;
use crate::error::UnexpectedNullError;
use crate::mysql::protocol;
use crate::mysql::MySql;
use crate::row::{ColumnIndex, Row};
use crate::types::Type;

pub enum MySqlValue<'c> {
    Binary(&'c [u8]),
    Text(&'c str),
}

impl<'c> TryFrom<Option<MySqlValue<'c>>> for MySqlValue<'c> {
    type Error = crate::Error;

    #[inline]
    fn try_from(value: Option<MySqlValue<'c>>) -> Result<Self, Self::Error> {
        match value {
            Some(value) => Ok(value),
            None => Err(crate::Error::decode(UnexpectedNullError)),
        }
    }
}

pub struct MySqlRow<'c> {
    pub(super) row: protocol::Row<'c>,
    pub(super) columns: Arc<HashMap<Box<str>, usize>>,
    pub(super) binary: bool,
}

impl<'c> Row<'c> for MySqlRow<'c> {
    type Database = MySql;

    fn len(&self) -> usize {
        self.row.len()
    }

    fn get_raw<'r, I>(&'r self, index: I) -> crate::Result<Option<MySqlValue<'c>>>
    where
        I: ColumnIndex<Self::Database>,
    {
        let index = index.resolve(self)?;
        let buffer = self.row.get(index);

        buffer
            .map(|buf| match self.binary {
                true => Ok(MySqlValue::Binary(buf)),
                false => Ok(MySqlValue::Text(from_utf8(buf)?)),
            })
            .transpose()
            .map_err(|err: Utf8Error| crate::Error::Decode(Box::new(err)))
    }
}
