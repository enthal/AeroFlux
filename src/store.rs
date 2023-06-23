use tokio::sync::{broadcast, mpsc};
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
    #[instrument(err, skip(self, write_request), fields(req = ?write_request.get_ref()))]
    async fn write(
        &self,
        write_request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        info!("");
        Err(Status::unimplemented("TODO"))
    }
}
