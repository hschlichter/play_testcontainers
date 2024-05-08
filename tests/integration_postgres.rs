use byteorder::{BigEndian, ByteOrder};
use play_testcontainers::PlayPostgres;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage};

#[tokio::test]
async fn test_postgres() {
    let container = GenericImage::new("postgres", "16")
        .with_exposed_port(5432)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "mysecretpassword")
        .start()
        .await;

    let host = container.get_host().await;
    let port = container.get_host_port_ipv4(5432).await;
    let connection_string = format!("postgres://postgres:mysecretpassword@{host}:{port}");

    let mut play = PlayPostgres::default();
    play.connect(&connection_string)
        .await
        .expect("Failed to connect");
    play.create_tables().await.expect("Failed to create tables");
    let id0 = play
        .insert("hello", 1_000_111)
        .await
        .expect("Failed to insert");
    let id1 = play
        .insert("world", 2_000_222)
        .await
        .expect("Failed to insert");

    if let Ok(recs) = play.list().await {
        for r in &recs {
            println!("{}", r);
        }
    }

    let rec0 = play.get(id0).await.expect("Failed to get record 0");
    let rec1 = play.get(id1).await.expect("Failed to get record 1");

    assert_eq!(rec0.name, "hello");
    assert_eq!(rec1.name, "world");

    play.update(&id0, "fubar", BigEndian::read_u128(&rec0.content_hash))
        .await
        .expect("Failed to update");

    if let Ok(recs) = play.list().await {
        for r in recs {
            println!("{}", r);
        }
    }
}
