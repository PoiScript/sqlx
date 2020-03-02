use std::collections::HashMap;
use std::sync::Arc;

use futures_core::future::BoxFuture;

use crate::connection::{ConnectionSource, MaybeOwnedConnection};
use crate::cursor::Cursor;
use crate::executor::Execute;
use crate::mysql::{MySql, MySqlArguments, MySqlConnection, MySqlRow};
use crate::pool::Pool;

pub struct MySqlCursor<'c, 'q> {
    source: ConnectionSource<'c, MySqlConnection>,
    query: Option<(&'q str, Option<MySqlArguments>)>,
    columns: Arc<HashMap<Box<str>, usize>>,
    binary: bool,
}

impl<'c, 'q> Cursor<'c, 'q> for MySqlCursor<'c, 'q> {
    type Database = MySql;

    #[doc(hidden)]
    fn from_pool<E>(pool: &Pool<MySqlConnection>, query: E) -> Self
    where
        Self: Sized,
        E: Execute<'q, MySql>,
    {
        Self {
            source: ConnectionSource::Pool(pool.clone()),
            columns: Arc::default(),
            binary: true,
            query: Some(query.into_parts()),
        }
    }

    #[doc(hidden)]
    fn from_connection<E, C>(conn: C, query: E) -> Self
    where
        Self: Sized,
        C: Into<MaybeOwnedConnection<'c, MySqlConnection>>,
        E: Execute<'q, MySql>,
    {
        Self {
            source: ConnectionSource::Connection(conn.into()),
            columns: Arc::default(),
            binary: true,
            query: Some(query.into_parts()),
        }
    }

    fn next(&mut self) -> BoxFuture<crate::Result<Option<MySqlRow<'_>>>> {
        Box::pin(next(self))
    }
}

async fn next<'a, 'c: 'a, 'q: 'a>(
    cursor: &'a mut MySqlCursor<'c, 'q>,
) -> crate::Result<Option<MySqlRow<'a>>> {
    let mut conn = cursor.source.resolve_by_ref().await?;

    todo!("MySqlCursor::next")
}
