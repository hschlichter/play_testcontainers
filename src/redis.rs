use anyhow::Result;
use redis::Commands;

pub struct PlayRedis {
    conn: redis::Connection,
}

impl PlayRedis {
    pub fn create(url: String) -> PlayRedis {
        let client = redis::Client::open(url).unwrap();
        let conn = client.get_connection().unwrap();

        PlayRedis {
            conn
        }
    }

    pub fn get(mut self, key: String) -> Result<String> {
        let val = self.conn.get(key)?;
        Ok(val)
    }

    pub fn put(mut self, key: String, val: String) -> Result<()> {
        self.conn.set(key, val)?;
        Ok(())
    }
}
