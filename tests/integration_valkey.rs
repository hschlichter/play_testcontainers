use play_testcontainers::PlayRedis;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage};

#[tokio::test]
async fn test_redis_async() {
    let container = GenericImage::new("valkey/valkey", "7.2.5")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await;

    let host = container.get_host().await;
    let port = container.get_host_port_ipv4(6379).await;
    let url = format!("redis://{}:{}", host, port);

    let play = PlayRedis::create(url);
    let key = "hello".to_string();
    let val = "world".to_string();

    assert!(play.put(key, val).is_ok());
}
