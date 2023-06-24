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
    Empty, Record, Timestamp, WriteRequest, WriteResponse,
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
        info!("");
        let req = req.get_ref();
        let root_path = format!(".data/");

        let topic_path = format!("{}/{}", &root_path, req.topic);
        fs::create_dir_all(&topic_path).await?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{}/{}.data", &topic_path, req.segment_index))
            .await?;

        let len = file.metadata().await?.len();

        let mut buf: Vec<u8> = Vec::new();
        for record in &req.records {
            record.encode(&mut buf).unwrap();
        }
        // TODO: reject if len+buf.len() ? u32::MAX
        file.write_all(&buf).await?;
        file.sync_all().await?;

        Ok(Response::new(WriteResponse {
            at_offset: len as u32,
            next_offset: len as u32 + buf.len() as u32,
        }))
    }
}
