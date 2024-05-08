use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ByteOrder};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use std::fmt::Display;

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

pub struct PlayPostgres {
    pool: Option<PgPool>,
}

impl Default for PlayPostgres {
    fn default() -> Self {
        PlayPostgres { pool: None }
    }
}

impl PlayPostgres {
    pub async fn connect(&mut self, url: &str) -> Result<()> {
        self.pool = Some(PgPoolOptions::new().max_connections(5).connect(url).await?);

        Ok(())
    }

    pub async fn create_tables(&self) -> Result<()> {
        if let Some(pool) = &self.pool {
            sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
                .execute(pool)
                .await?;

            sqlx::query(
                r#"
CREATE TABLE IF NOT EXISTS targets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    content_hash BYTEA
);
                "#,
            )
            .execute(pool)
            .await?;

            Ok(())
        } else {
            Err(anyhow!("Error during table creation"))
        }
    }

    pub async fn insert(&self, name: &str, content_hash: u128) -> Result<uuid::Uuid> {
        if let Some(pool) = &self.pool {
            let content_hash_bytes = content_hash.to_be_bytes();
            let id = sqlx::query_scalar(
                "INSERT INTO targets (name, content_hash) VALUES ($1, $2) RETURNING id;",
            )
            .bind(name)
            .bind(content_hash_bytes)
            .fetch_one(pool)
            .await?;

            Ok(id)
        } else {
            Err(anyhow!("Error during insertion"))
        }
    }

    pub async fn update(&self, id: &uuid::Uuid, name: &str, content_hash: u128) -> Result<()> {
        if let Some(pool) = &self.pool {
            sqlx::query("UPDATE targets SET name = $1, content_hash = $2 WHERE id = $3;")
                .bind(name)
                .bind(content_hash.to_be_bytes())
                .bind(id)
                .execute(pool)
                .await?;

            Ok(())
        } else {
            Err(anyhow!("Error during insertion"))
        }
    }

    pub async fn get(&self, id: uuid::Uuid) -> Result<Target> {
        if let Some(pool) = &self.pool {
            let rec = sqlx::query_as::<_, Target>("SELECT * FROM targets WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await?;

            Ok(rec)
        } else {
            Err(anyhow!("Error during insertion"))
        }
    }

    pub async fn list(&self) -> Result<Vec<Target>> {
        if let Some(pool) = &self.pool {
            let recs = sqlx::query_as::<_, Target>("SELECT * FROM targets;")
                .fetch_all(pool)
                .await?;

            Ok(recs)
        } else {
            Err(anyhow!("Error during insertion"))
        }
    }
}
