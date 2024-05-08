use play_testcontainers::PlayRedis;
use testcontainers_modules::redis;

#[test]
fn test_redis() {
    use testcontainers::runners::SyncRunner;

    let container = redis::Redis::default().start();
    let host = container.get_host();
    let port = container.get_host_port_ipv4(6379);
    let url = format!("redis://{}:{}", host, port);

    let play = PlayRedis::create(url);
    let key = "hello".to_string();
    let val = "world".to_string();

    assert!(play.put(key, val).is_ok());
}

#[tokio::test]
async fn test_redis_async() {
    use testcontainers::runners::AsyncRunner;

    let container = redis::Redis::default().start().await;
    let host = container.get_host().await;
    let port = container.get_host_port_ipv4(6379).await;
    let url = format!("redis://{}:{}", host, port);

    let play = PlayRedis::create(url);
    let key = "hello".to_string();
    let val = "world".to_string();

    assert!(play.put(key, val).is_ok());
}
