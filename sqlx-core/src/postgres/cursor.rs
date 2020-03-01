use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_stream::try_stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;

use crate::connection::{ConnectionSource, MaybeOwnedConnection};
use crate::cursor::Cursor;
use crate::database::HasRow;
use crate::executor::Execute;
use crate::pool::{Pool, PoolConnection};
use crate::postgres::protocol::{
    CommandComplete, DataRow, Message, RowDescription, StatementId, TypeFormat,
};
use crate::postgres::{PgArguments, PgConnection, PgRow};
use crate::{Database, Postgres};
use futures_core::Stream;

pub struct PgCursor<'c, 'q> {
    source: ConnectionSource<'c, PgConnection>,
    query: Option<(&'q str, Option<PgArguments>)>,
    columns: Arc<HashMap<Box<str>, usize>>,
    formats: Arc<[TypeFormat]>,
}

impl<'c, 'q> Cursor<'c, 'q> for PgCursor<'c, 'q> {
    type Database = Postgres;

    #[doc(hidden)]
    fn from_pool<E>(pool: &Pool<PgConnection>, query: E) -> Self
    where
        Self: Sized,
        E: Execute<'q, Postgres>,
    {
        Self {
            source: ConnectionSource::Pool(pool.clone()),
            columns: Arc::default(),
            formats: Arc::new([] as [TypeFormat; 0]),
            query: Some(query.into_parts()),
        }
    }

    #[doc(hidden)]
    fn from_connection<E, C>(conn: C, query: E) -> Self
    where
        Self: Sized,
        C: Into<MaybeOwnedConnection<'c, PgConnection>>,
        E: Execute<'q, Postgres>,
    {
        Self {
            source: ConnectionSource::Connection(conn.into()),
            columns: Arc::default(),
            formats: Arc::new([] as [TypeFormat; 0]),
            query: Some(query.into_parts()),
        }
    }

    fn next(&mut self) -> BoxFuture<crate::Result<Option<PgRow<'_>>>> {
        Box::pin(next(self))
    }
}

// Used to describe the incoming results
// We store the column map in an Arc and share it among all rows
async fn describe(
    conn: &mut PgConnection,
) -> crate::Result<(HashMap<Box<str>, usize>, Vec<TypeFormat>)> {
    let description: Option<_> = loop {
        match conn.stream.read().await? {
            Message::ParseComplete | Message::BindComplete => {}

            Message::RowDescription => {
                break Some(RowDescription::read(conn.stream.buffer())?);
            }

            Message::NoData => {
                break None;
            }

            message => {
                return Err(
                    protocol_err!("next/describe: unexpected message: {:?}", message).into(),
                );
            }
        }
    };

    let mut columns = HashMap::new();
    let mut formats = Vec::new();

    if let Some(description) = description {
        columns.reserve(description.fields.len());
        formats.reserve(description.fields.len());

        for (index, field) in description.fields.iter().enumerate() {
            if let Some(name) = &field.name {
                columns.insert(name.clone(), index);
            }

            formats.push(field.type_format);
        }
    }

    Ok((columns, formats))
}

// A form of describe that uses the statement cache
async fn get_or_describe(
    conn: &mut PgConnection,
    statement: StatementId,
) -> crate::Result<(Arc<HashMap<Box<str>, usize>>, Arc<[TypeFormat]>)> {
    if !conn.cache_statement_columns.contains_key(&statement)
        || !conn.cache_statement_formats.contains_key(&statement)
    {
        let (columns, formats) = describe(conn).await?;

        conn.cache_statement_columns
            .insert(statement, Arc::new(columns));

        conn.cache_statement_formats
            .insert(statement, Arc::from(formats));
    }

    Ok((
        Arc::clone(&conn.cache_statement_columns[&statement]),
        Arc::clone(&conn.cache_statement_formats[&statement]),
    ))
}

async fn next<'a, 'c: 'a, 'q: 'a>(
    cursor: &'a mut PgCursor<'c, 'q>,
) -> crate::Result<Option<PgRow<'a>>> {
    let mut conn = cursor.source.resolve_by_ref().await?;

    // The first time [next] is called we need to actually execute our
    // contained query. We guard against this happening on _all_ next calls
    // by using [Option::take] which replaces the potential value in the Option with `None
    if let Some((query, arguments)) = cursor.query.take() {
        let statement = conn.run(query, arguments).await?;

        // If there is a statement ID, this is a non-simple or prepared query
        let (columns, formats) = if let Some(statement) = statement {
            // A prepared statement will re-use the previous column map if
            // this query has been executed before
            get_or_describe(&mut *conn, statement).await?
        } else {
            // A non-prepared query must be described each time
            let (columns, formats) = describe(&mut *conn).await?;
            (Arc::new(columns), Arc::<[TypeFormat]>::from(formats))
        };

        cursor.columns = columns;
        cursor.formats = formats;
    }

    loop {
        match conn.stream.read().await? {
            // Indicates that a phase of the extended query flow has completed
            // We as SQLx don't generally care as long as it is happening
            Message::ParseComplete | Message::BindComplete => {}

            // Indicates that _a_ query has finished executing
            // Parsing the message would allow inspecting the affected rows
            Message::CommandComplete => {
                break;
            }

            Message::DataRow => {
                let data = DataRow::read(&mut *conn)?;

                return Ok(Some(PgRow {
                    connection: conn,
                    columns: Arc::clone(&cursor.columns),
                    formats: Arc::clone(&cursor.formats),
                    data,
                }));
            }

            message => {
                return Err(protocol_err!("next: unexpected message: {:?}", message).into());
            }
        }
    }

    Ok(None)
}
