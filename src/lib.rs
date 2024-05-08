pub fn hello() -> String {
    "Hello World".to_string()
}

pub mod redis;
pub use redis::PlayRedis;

pub mod s3;
pub use s3::PlayS3;

pub mod postgres;
pub use postgres::PlayPostgres;
