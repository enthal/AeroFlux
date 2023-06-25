use std::{io::SeekFrom, mem::size_of};

use prost::Message;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::*;

pub mod aeroflux {
    tonic::include_proto!("aeroflux");
}
use aeroflux::{
    read_response::Event,
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
            record.encode_length_delimited(&mut records_buf).unwrap(); // TODO: no unwrap
            index_buf.extend_from_slice(&(records_start_pos + pos).to_le_bytes());
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
        let req = req.get_ref();
        let pathing = Pathing {
            topic: req.topic.clone(),
            segment_index: req.segment_index,
        };

        let mut records_file = OpenOptions::new()
            .read(true)
            .open(pathing.records_file_path())
            .await?; // TODO: convert error
        let mut index_file = OpenOptions::new()
            .read(true)
            .open(pathing.index_file_path())
            .await?;

        // Seek records_file to pos from index_file at offset
        let record_count =
            index_file.seek(SeekFrom::End(0)).await? as u32 / size_of::<u32>() as u32;
        if req.from_offset > record_count {
            return Err(Status::out_of_range(format!(
                "No such offset ({}) in segment ({} records)",
                req.from_offset, record_count
            )));
        }
        index_file
            .seek(SeekFrom::Start(
                req.from_offset as u64 * size_of::<u32>() as u64,
            ))
            .await?; // TODO: convert error
        let from_pos = index_file.read_u32_le().await?; // little-endian
        info!(
            "Read: from_offset:{} from_pos:{}",
            req.from_offset, from_pos
        );
        records_file.seek(SeekFrom::Start(from_pos as u64)).await?;

        let (stream_tx, stream_rx) = mpsc::channel(16); // TODO: tunable?

        // Frame: Decode variable-length record length_delimiter.
        // TODO: consider using fixed-length (4 byte?) record length_delimiter.
        let mut length_delimiter_buf = [0; 10]; // decode_length_delimiter needs exactly 10
        records_file.read_exact(&mut length_delimiter_buf).await?;
        let record_length = prost::decode_length_delimiter(&length_delimiter_buf[..]).unwrap(); // TODO: no unwrap
        let record_start_pos = from_pos as u64 + prost::length_delimiter_len(record_length) as u64;
        records_file.seek(SeekFrom::Start(record_start_pos)).await?;

        // Read and send record
        let mut record_buf = vec![0u8; record_length];
        records_file.read_exact(&mut record_buf).await?;
        let record_buf = prost::bytes::Bytes::from(record_buf);
        let record = Record::decode(record_buf).unwrap(); // TODO: no unwrap
        stream_tx
            .send(Ok(ReadResponse {
                event: Some(Event::Record(record)),
            }))
            .await
            .unwrap(); // TODO: no unwrap

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
