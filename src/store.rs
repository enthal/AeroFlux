use std::mem::size_of;

use prost::Message;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::*;

pub mod aeroflux {
    tonic::include_proto!("aeroflux");
}
use aeroflux::{
    store_server::{Store, StoreServer},
    Empty, ReadRequest, ReadResponse, Record, Timestamp, WriteRequest, WriteResponse,
};
use tracing_subscriber::fmt::format;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let addr = "[::1]:11000".parse().unwrap();
    info!("Listen: {addr}");
    Server::builder()
        .add_service(StoreServer::new(StoreService {}))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Debug)]
pub struct StoreService {}

#[tonic::async_trait]
impl Store for StoreService {
    #[instrument(err, skip(self, req), fields(req = ?req.get_ref()))]
    async fn write(&self, req: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        info!("start");
        let req = req.get_ref();

        let pathing = Pathing {
            topic: req.topic.clone(),
            segment_index: req.segment_index,
        };

        fs::create_dir_all(pathing.topic_dir_path()).await?;

        let mut records_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(pathing.records_file_path())
            .await?;
        let mut index_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(pathing.index_file_path())
            .await?;

        // TODO: u32::MAX is probably the wrong max records file length!  u64.
        // records_file must not be longer than u32::MAX; TODO: else error (the file is corrupt)
        let records_start_pos = //.
            records_file.metadata().await?.len() as u32;
        // records_file must have fewer than u32::MAX records; TODO: else error (the file is corrupt)
        let records_start_count =
            index_file.metadata().await?.len() as u32 / size_of::<u32>() as u32;
        // TODO: reject (segment must be closed):
        //   if records_start_count as u64 + req.records.len() > u32::MAX

        let mut records_buf: Vec<u8> = Vec::new();
        let mut index_buf: Vec<u8> = Vec::new();
        for record in &req.records {
            let pos = records_buf.len() as u32;
            record.encode(&mut records_buf).unwrap();
            index_buf.extend_from_slice(&(records_start_pos + pos).to_ne_bytes());
        }
        // TODO: reject (segment must be closed) if records_start_pos+records_buf.len() > u32::MAX
        records_file.write_all(&records_buf).await?;
        index_file.write_all(&index_buf).await?;
        records_file.sync_all().await?;
        index_file.sync_all().await?;

        Ok(Response::new(WriteResponse {
            at_offset: records_start_count,
            next_offset: records_start_count + req.records.len() as u32,
        }))
    }

    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    #[instrument(err, skip(self, req), fields(req = ?req.get_ref()))]
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<Self::ReadStream>, Status> {
        info!("start");
        let (stream_tx, stream_rx) = mpsc::channel(16); // TODO: tunable?

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }
}

#[derive(Debug)]
pub struct Pathing {
    topic: String,
    segment_index: u64,
}
impl Pathing {
    const ROOT_PATH: &str = ".data/";

    fn topic_dir_path(&self) -> String {
        format!("{}/{}", Pathing::ROOT_PATH, self.topic)
    }
    fn records_file_path(&self) -> String {
        format!("{}/{}.records", self.topic_dir_path(), self.segment_index)
    }
    fn index_file_path(&self) -> String {
        format!("{}/{}.index", self.topic_dir_path(), self.segment_index)
    }
}
