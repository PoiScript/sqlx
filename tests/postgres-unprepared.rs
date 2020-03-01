use sqlx::{Connect, Executor, PgConnection};

// ""
// "SELECT 1"
// "INSERT INTO .."
// "CREATE TABLE .."
// "SELECT 1 FROM not_found"

#[cfg_attr(feature = "runtime-async-std", async_std::test)]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
async fn test_empty_query() -> anyhow::Result<()> {
    let mut conn = connect().await?;
    let affected = conn.execute("").await?;

    assert_eq!(affected, 0);

    Ok(())
}

async fn connect() -> anyhow::Result<PgConnection> {
    let _ = dotenv::dotenv();
    let _ = env_logger::try_init();

    Ok(PgConnection::connect(dotenv::var("DATABASE_URL")?).await?)
}
