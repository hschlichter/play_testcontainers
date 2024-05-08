use std::io::Write;

use play_testcontainers::PlayS3;
use rand::{distributions::Alphanumeric, Rng};
use tempfile::NamedTempFile;
use base64::{engine::general_purpose, Engine as _};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio;

fn create_temp_file_with_random_data(
    num_of_chunks: u32,
    size_of_chunk: u32,
) -> (NamedTempFile, usize) {
    let mut temp_file = NamedTempFile::new().expect("failed to create temp file");
    let mut rng = rand::thread_rng();
    for _ in 0..num_of_chunks {
        let data: Vec<u8> = (0..size_of_chunk)
            .map(|_| rng.sample(Alphanumeric) as u8)
            .collect();
        temp_file
            .write_all(&data)
            .expect("failed to write to temp file");
    }

    (temp_file, (num_of_chunks * size_of_chunk) as usize)
}

#[tokio::test]
async fn test_s3() {
    let container = minio::MinIO::default().start().await;
    let port = container.get_host_port_ipv4(9000).await;

    let client = PlayS3::create_client(
        "minioadmin".to_string(),
        "minioadmin".to_string(),
        format!("http://127.0.0.1:{port}"), // Using get_host from the container does not work
    );

    let (temp_file, temp_file_size) = create_temp_file_with_random_data(10, 1024 * 1024);

    let bucket = PlayS3::create_bucket(&client, "test-bucket")
        .await
        .expect("Failed to create bucket");
    let (hash, put_size) = PlayS3::put_file(&client, &bucket, temp_file.path())
        .await
        .expect("Failed to upload");
    assert_eq!(put_size, temp_file_size);

    let temp_file_get = NamedTempFile::new().expect("Failed to create temp file for get");
    let _ = PlayS3::get_file(&client, &bucket, &hash, temp_file_get.path())
        .await
        .expect("Failed to get file");

    let hash_of_get_file = PlayS3::hash_file(temp_file_get.path())
        .await
        .expect("Failed to hash file");
    assert_eq!(hash, general_purpose::URL_SAFE_NO_PAD.encode(hash_of_get_file.to_be_bytes()))
}
