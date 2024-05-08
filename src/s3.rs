use anyhow::Result;
use aws_config::Region;
use aws_sdk_s3::{
    config::Credentials,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client, Config,
};
use base64::{engine::general_purpose, Engine as _};
use std::path::Path;
use tokio::{
    fs::File,
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};
use xxhash_rust::xxh3::Xxh3;

const PART_SIZE: usize = 5 * 1024 * 1024;

pub struct PlayS3 {}

impl PlayS3 {
    pub fn create_client(access_key: String, secret_key: String, endpoint_url: String) -> Client {
        // let access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID must be set");
        // let secret_key = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
        // let endpoint_url = env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL must be set");

        let config = Config::builder()
            .credentials_provider(Credentials::new(
                access_key, secret_key, None, None, "custom",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint_url)
            .build();

        Client::from_conf(config)
    }

    pub async fn create_bucket(client: &Client, name: &str) -> Result<String> {
        match client.create_bucket().bucket(name).send().await {
            Ok(_) => Ok(name.to_string()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_file(
        client: &Client,
        bucket: &str,
        key: &str,
        filepath: &Path,
    ) -> Result<usize> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(filepath)
            .await?;

        let mut bytes_received = 0;
        let mut obj = client.get_object().bucket(bucket).key(key).send().await?;
        while let Some(bytes) = obj.body.try_next().await? {
            bytes_received += bytes.len();

            file.write_all(&bytes).await?;
        }

        Ok(bytes_received)
    }

    pub async fn hash_file(filepath: &Path) -> Result<u64> {
        let file = File::open(filepath).await?;
        let mut hasher = Xxh3::new();
        let mut reader = BufReader::new(file);

        let mut buf = vec![0; 64 * 1024];
        while let Ok(n) = reader.read(&mut buf).await {
            hasher.update(&buf[..n]);
            if n == 0 {
                break;
            }
        }

        Ok(hasher.digest())
    }

    async fn fill_buffer(file: &mut File, buf_size: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0; buf_size];
        let mut cur = 0;
        while let Ok(n) = file.read(&mut buf[cur..]).await {
            cur += n;

            // Check if the file has been read completely
            if n == 0 || cur == buf.len() {
                break;
            }

            // Check if the buffer is full
            if cur != buf.len() {
                continue;
            }
        }

        Ok(buf[..cur].to_vec())
    }

    pub async fn put_file(
        client: &Client,
        bucket: &str,
        filepath: &Path,
    ) -> Result<(String, usize)> {
        let hash = PlayS3::hash_file(filepath).await?;
        let key = general_purpose::URL_SAFE_NO_PAD.encode(hash.to_be_bytes());

        let create_resp = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .send()
            .await?;

        let upload_id = create_resp.upload_id.expect("upload id not found");

        let mut file = File::open(filepath).await?;
        let mut part_number = 1;
        let mut parts = Vec::new();

        let mut total_bytes = 0;
        while let Ok(buf) = PlayS3::fill_buffer(&mut file, PART_SIZE).await {
            total_bytes += buf.len();

            if buf.is_empty() {
                break;
            }

            let part_resp = client
                .upload_part()
                .bucket(bucket)
                .key(&key)
                .part_number(part_number)
                .upload_id(&upload_id)
                .body(ByteStream::from(buf.clone()))
                .send()
                .await?;

            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(part_resp.e_tag.expect("etag not found"))
                    .build(),
            );

            part_number += 1;
        }

        let completed_parts = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let _ = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(completed_parts)
            .send()
            .await?;

        Ok((key.clone(), total_bytes))
    }
}
