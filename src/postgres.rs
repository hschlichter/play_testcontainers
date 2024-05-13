use byteorder::{BigEndian, ByteOrder};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    FromRow, PgPool, Row,
};
use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub struct Target {
    pub id: uuid::Uuid,
    pub name: String,
    pub hash: u128,
}

impl FromRow<'_, PgRow> for Target {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            hash: {
                let data: Vec<u8> = row.try_get("hash")?;
                if data.len() == 16 {
                    Ok(BigEndian::read_u128(&data))
                } else {
                    Err(sqlx::Error::ColumnDecode {
                        index: "hash".to_string(),
                        source: Box::new(PlayPostgresError::InvalidHash),
                    })
                }
            }?,
        })
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
    InvalidHash,
}

impl Display for PlayPostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlayPostgresError::NoPool => write!(f, "No connection pool available"),
            PlayPostgresError::ConnectionFailed => write!(f, "Connection to postgres failed"),
            PlayPostgresError::UuidExtensionCreateFailed => {
                write!(f, "Connection to postgres failed")
            }
            PlayPostgresError::CreateTablesFailed => write!(f, "Failed to create tables"),
            PlayPostgresError::InsertFailed => write!(f, "Failed to insert"),
            PlayPostgresError::UpdateFailed => write!(f, "Failed to update"),
            PlayPostgresError::GetFailed => write!(f, "Failed to get"),
            PlayPostgresError::ListFailed => write!(f, "Failed to list"),
            PlayPostgresError::InvalidHash => todo!(),
        }
    }
}

impl Error for PlayPostgresError {}

#[derive(Default)]
pub struct PlayPostgres {
    pool: Option<PgPool>,
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
        self.pool.as_ref().ok_or(PlayPostgresError::NoPool)
    }

    pub async fn create_tables(&self) -> Result<(), PlayPostgresError> {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .execute(self.pool()?)
            .await
            .map_err(|_| PlayPostgresError::UuidExtensionCreateFailed)?;

        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS targets (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name VARCHAR(255) NOT NULL,
            hash BYTEA);
        ",
        )
        .execute(self.pool()?)
        .await
        .map_err(|_| PlayPostgresError::CreateTablesFailed)?;

        Ok(())
    }

    pub async fn insert(&self, name: &str, hash: u128) -> Result<uuid::Uuid, PlayPostgresError> {
        let id =
            sqlx::query_scalar("INSERT INTO targets (name, hash) VALUES ($1, $2) RETURNING id;")
                .bind(name)
                .bind(hash.to_be_bytes())
                .fetch_one(self.pool()?)
                .await
                .map_err(|_| PlayPostgresError::InsertFailed)?;

        Ok(id)
    }

    pub async fn update(
        &self,
        id: &uuid::Uuid,
        name: &str,
        hash: u128,
    ) -> Result<(), PlayPostgresError> {
        sqlx::query("UPDATE targets SET name = $1, hash = $2 WHERE id = $3;")
            .bind(name)
            .bind(hash.to_be_bytes())
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
