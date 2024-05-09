use byteorder::{BigEndian, ByteOrder};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use std::{fmt::Display, error::Error};

#[derive(Debug, FromRow)]
pub struct Target {
    pub id: uuid::Uuid,
    pub name: String,
    pub content_hash: Vec<u8>,
}

impl Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let content_hash = BigEndian::read_u128(&self.content_hash);
        write!(
            f,
            "id: {}, name: {}, content_hash: {}",
            self.id, self.name, content_hash
        )
    }
}

#[derive(Debug)]
pub enum PlayPostgresError {
    NoPool,
    ConnectionFailed,
    UuidExtensionCreateFailed,
    CreateTablesFailed,
    InsertFailed,
    UpdateFailed,
    GetFailed,
    ListFailed,
}

impl Display for PlayPostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlayPostgresError::NoPool => write!(f, "No connection pool available"),
            PlayPostgresError::ConnectionFailed => write!(f, "Connection to postgres failed"),
            PlayPostgresError::UuidExtensionCreateFailed => write!(f, "Connection to postgres failed"),
            PlayPostgresError::CreateTablesFailed => write!(f, "Failed to create tables"),
            PlayPostgresError::InsertFailed => write!(f, "Failed to insert"),
            PlayPostgresError::UpdateFailed => write!(f, "Failed to update"),
            PlayPostgresError::GetFailed => write!(f, "Failed to get"),
            PlayPostgresError::ListFailed => write!(f, "Failed to list"),
        }
    }
}

impl Error for PlayPostgresError {}

pub struct PlayPostgres {
    pool: Option<PgPool>,
}

impl Default for PlayPostgres {
    fn default() -> Self {
        PlayPostgres { pool: None }
    }
}

impl PlayPostgres {
    pub async fn connect(&mut self, url: &str) -> Result<(), PlayPostgresError> {
        self.pool = Some(
            PgPoolOptions::new()
                .max_connections(5)
                .connect(url)
                .await
                .map_err(|_| PlayPostgresError::ConnectionFailed)?,
        );
        Ok(())
    }

    fn pool(&self) -> Result<&PgPool, PlayPostgresError> {
        Ok(self
            .pool
            .as_ref()
            .ok_or_else(|| PlayPostgresError::NoPool)?)
    }

    pub async fn create_tables(&self) -> Result<(), PlayPostgresError> {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .execute(self.pool()?)
            .await
            .map_err(|_| PlayPostgresError::UuidExtensionCreateFailed)?;

        sqlx::query(
            r#"
CREATE TABLE IF NOT EXISTS targets (
id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
name VARCHAR(255) NOT NULL,
content_hash BYTEA
);
            "#,
        )
        .execute(self.pool()?)
        .await
        .map_err(|_| PlayPostgresError::CreateTablesFailed)?;

        Ok(())
    }

    pub async fn insert(
        &self,
        name: &str,
        content_hash: u128,
    ) -> Result<uuid::Uuid, PlayPostgresError> {
        let content_hash_bytes = content_hash.to_be_bytes();
        let id = sqlx::query_scalar(
            "INSERT INTO targets (name, content_hash) VALUES ($1, $2) RETURNING id;",
        )
        .bind(name)
        .bind(content_hash_bytes)
        .fetch_one(self.pool()?)
        .await
        .map_err(|_| PlayPostgresError::InsertFailed)?;

        Ok(id)
    }

    pub async fn update(
        &self,
        id: &uuid::Uuid,
        name: &str,
        content_hash: u128,
    ) -> Result<(), PlayPostgresError> {
        sqlx::query("UPDATE targets SET name = $1, content_hash = $2 WHERE id = $3;")
            .bind(name)
            .bind(content_hash.to_be_bytes())
            .bind(id)
            .execute(self.pool()?)
            .await
            .map_err(|_| PlayPostgresError::UpdateFailed)?;

        Ok(())
    }

    pub async fn get(&self, id: uuid::Uuid) -> Result<Target, PlayPostgresError> {
        let rec = sqlx::query_as("SELECT * FROM targets WHERE id = $1")
            .bind(id)
            .fetch_one(self.pool()?)
            .await
            .map_err(|_| PlayPostgresError::GetFailed)?;

        Ok(rec)
    }

    pub async fn list(&self) -> Result<Vec<Target>, PlayPostgresError> {
        let recs = sqlx::query_as("SELECT * FROM targets;")
            .fetch_all(self.pool()?)
            .await
            .map_err(|_| PlayPostgresError::ListFailed)?;

        Ok(recs)
    }
}
