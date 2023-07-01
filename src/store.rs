use std::{io::SeekFrom, mem::size_of, ops::Range};

use prost::{DecodeError, Message};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc::{self, Sender},
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
    Empty, ErrorCode, ReadRequest, ReadResponse, Record, WriteRequest, WriteResponse,
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

        // Note: pos means byte index in records_file; offset means record offset in segment.

        // records_file must not be longer than u32::MAX, since its length must fit in a 4-byte index file entry.
        let records_start_pos = async {
            let len = records_file.metadata().await?.len();
            if len > u32::MAX as u64 {
                Err(Status::data_loss("records_file is too large"))
            } else {
                Ok(len as u32)
            }
        }
        .await?;

        // records_file must have fewer than u32::MAX records; TODO: else error (the file is corrupt)
        let first_record_offset =
            index_file.metadata().await?.len() as u32 / size_of::<u32>() as u32;
        if (first_record_offset as u64) + (req.records.len() as u64) > (u32::MAX as u64) {
            // TODO: store fact that segment is closed.
            return Err(Status::out_of_range("Segment full"));
        }

        let mut records_buf: Vec<u8> = Vec::new();
        let mut index_buf: Vec<u8> = Vec::new();
        for record in &req.records {
            let pos = records_buf.len() as u32;
            record
                .encode_length_delimited(&mut records_buf)
                .expect("protobuf encoding should succeed");
            if (records_start_pos as u64) + (records_buf.len() as u64) > (u32::MAX as u64) {
                // TODO: store fact that segment is closed.
                return Err(Status::out_of_range("Segment full"));
            }
            index_buf.extend_from_slice(&(records_start_pos + pos).to_le_bytes());
        }

        records_file.write_all(&records_buf).await?;
        index_file.write_all(&index_buf).await?;
        records_file.sync_all().await?;
        index_file.sync_all().await?;

        Ok(Response::new(WriteResponse {
            at_offset: first_record_offset,
            next_offset: first_record_offset + req.records.len() as u32,
        }))
    }

    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    #[instrument(err, skip(self, req), fields(req = ?req.get_ref()))]
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<Self::ReadStream>, Status> {
        let req = req.get_ref();
        let pathing = Pathing {
            topic: req.topic.clone(),
            segment_index: req.segment_index,
        };

        // Find from_pos (in records_file) per index_file at offset
        let mut index_file = OpenOptions::new()
            .read(true)
            .open(pathing.index_file_path())
            .await?;
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
        let from_pos = index_file.read_u32_le().await? as u64; // little-endian
        info!("from_pos:{}", from_pos);

        let (tx, rx) = mpsc::channel(16); // TODO: tunable?

        // Read and send every record in record_range
        let record_range = req.from_offset..record_count;
        let records_file = OpenOptions::new()
            .read(true)
            .open(pathing.records_file_path())
            .await?; // TODO: convert error
        tokio::spawn(
            async move {
                let terminate = |event: Event| async {
                    tx.send(Ok(ReadResponse { event: Some(event) })).await.ok();
                    // best effort
                };
                match Self::read_and_send(record_range, records_file, from_pos, &tx).await {
                    Ok(_) => {
                        terminate(Event::End(Empty {})).await;
                    }
                    Err(err) => {
                        if false {
                        } else if let Some(_) = err.downcast_ref::<Status>() {
                            // Error on send.  Don't try to send again.
                        } else if let Some(_) = err.downcast_ref::<DecodeError>() {
                            terminate(Event::Error(ErrorCode::ProtobufError as i32)).await;
                        } else if let Some(_) = err.downcast_ref::<std::io::Error>() {
                            terminate(Event::Error(ErrorCode::IoError as i32)).await;
                        } else {
                            error!("Unknown Error: {}", err);
                        }
                    }
                }
            }
            .instrument(info_span!("")),
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl StoreService {
    #[instrument(err, skip(records_file, stream_tx), fields())]
    async fn read_and_send(
        record_range: Range<u32>,
        mut records_file: fs::File,
        mut file_pos: u64,
        stream_tx: &Sender<Result<ReadResponse, Status>>,
    ) -> Result<(), Error> {
        info!("start");
        //file_pos += 300;

        records_file.seek(SeekFrom::Start(file_pos)).await?;
        for _ in record_range {
            // Frame: Decode variable-length record length_delimiter.
            // TODO: consider using fixed-length (4 byte?) record length_delimiter.
            let mut length_delimiter_buf = [0; 10]; // decode_length_delimiter needs exactly 10
            records_file.read_exact(&mut length_delimiter_buf).await?;
            let record_length = prost::decode_length_delimiter(&length_delimiter_buf[..])?;
            let record_start_pos =
                file_pos as u64 + prost::length_delimiter_len(record_length) as u64;
            records_file.seek(SeekFrom::Start(record_start_pos)).await?;

            // Read and send record
            let mut record_buf = vec![0u8; record_length];
            records_file.read_exact(&mut record_buf).await?;
            let record_buf = prost::bytes::Bytes::from(record_buf);
            let record = Record::decode(record_buf)?;
            stream_tx
                .send(Ok(ReadResponse {
                    event: Some(Event::Record(record)),
                }))
                .await?;

            file_pos = record_start_pos + record_length as u64;
        }

        info!("end");
        Ok(())
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
