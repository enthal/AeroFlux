use std::{
    collections::HashMap,
    io::SeekFrom,
    mem::size_of,
    ops::Range,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use prost::{DecodeError, Message};
use std::sync::atomic::AtomicU32;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::*;

pub mod aeroflux {
    tonic::include_proto!("aeroflux");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("aeroflux_descriptor");
}
use aeroflux::{
    read_response::Event,
    store_server::{Store, StoreServer},
    CreateSegmentRequest, Empty, ErrorCode, ReadRequest, ReadResponse, StoreRecord, Timestamp,
    WriteRecord, WriteRequest, WriteResponse, FILE_DESCRIPTOR_SET,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?;

    let store_service = StoreService::new(
        //
        ".data".to_string(),
        Arc::new(SystemClock {}),
    );
    store_service.on_startup().await?;

    let addr = "[::1]:11000".parse().unwrap();
    info!("Listen: {addr}");
    Server::builder()
        .add_service(StoreServer::new(store_service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct SegmentID {
    topic: String,
    segment_index: u64,
}

#[derive(Debug)]
pub struct SegmentSink {
    // topic: String,
    // segment_index: u64,
    next_offset: AtomicU32,
    tx: Option<Sender<WriteEvent>>, // None when closed
}
type SegementsById = Arc<Mutex<HashMap<SegmentID, SegmentSink>>>;

enum WriteEvent {
    Record(WriteRecord),
    Sync(SyncEvent),
}

struct SyncEvent {
    tx: Option<oneshot::Sender<WriteResponse>>,
}

#[derive(Debug)]
pub struct StoreService {
    root_path: String,
    segements_by_id: SegementsById,
    clock: Arc<dyn Clock>,
}
impl StoreService {
    fn new(root_path: String, clock: Arc<dyn Clock>) -> StoreService {
        StoreService {
            root_path,
            segements_by_id: Arc::new(Mutex::new(HashMap::new())),
            clock,
        }
    }
}

impl StoreService {
    #[instrument(err, skip(self))]
    async fn on_startup(&self) -> Result<(), Error> {
        info!("");
        let pathing = Pathing {
            root: self.root_path.clone(),
            topic: "unused".to_string(),
            segment_index: 0,
        };
        fs::create_dir_all(pathing.topics_dir_path()).await?;

        let mut topics_dir = fs::read_dir(pathing.topics_dir_path()).await?;
        while let Some(topic_dir_entry) = topics_dir.next_entry().await? {
            let topic_name = topic_dir_entry
                .file_name()
                .into_string()
                .expect("sane topic name"); // TODO: don't panic
            info!("Topic: {}", topic_name);

            let mut segments_dir = fs::read_dir(topic_dir_entry.path()).await?;
            while let Some(segment_dir_entry) = segments_dir.next_entry().await? {
                let segment_index: u64 = segment_dir_entry
                    .file_name()
                    .into_string()
                    .expect("sane segment name") // TODO: don't panic
                    .parse()?; // TODO: skip bad segment dir names?
                info!("  Segment: {}", segment_index);

                let segment_id = SegmentID {
                    topic: topic_name.clone(),
                    segment_index: segment_index,
                };
                self.create_segment(segment_id).await?
            }
        }

        info!("done");
        Ok(())
    }

    async fn create_segment(&self, segment_id: SegmentID) -> Result<(), Status> {
        let mut db = self.segements_by_id.lock().expect("Mutex should be valid");
        if db.contains_key(&segment_id) {
            return Err(Status::already_exists(format!(
                "Segment already exists: {:?}",
                segment_id
            )));
        }

        let (tx, mut rx) = mpsc::channel(16); // TODO: tunable? Backpressure is appropriate.

        db.insert(
            segment_id.clone(),
            SegmentSink {
                next_offset: AtomicU32::new(0),
                tx: Some(tx),
            },
        );

        let pathing = Pathing {
            root: self.root_path.clone(),
            topic: segment_id.topic,
            segment_index: segment_id.segment_index,
        };
        let clock = self.clock.clone();
        tokio::spawn(
            async move {
                // TODO: Handle returned errors somehow.

                fs::create_dir_all(pathing.segment_dir_path()).await?;

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
                let mut records_next_pos = async {
                    let len = records_file.metadata().await?.len();
                    if len > u32::MAX as u64 {
                        Err(Status::data_loss("records_file is too large"))
                    } else {
                        Ok(len as u32)
                    }
                }
                .await?;

                // records_file must have fewer than u32::MAX records; TODO: else error (the file is corrupt)
                let next_offset = index_file.metadata().await?.len() / size_of::<u32>() as u64;
                if (next_offset) > (u32::MAX as u64) {
                    // TODO: store fact that segment is closed.
                    return Err(Status::out_of_range("Segment full"));
                }
                let mut next_offset = next_offset as u32;

                // TODO: Bytes/Buf
                let mut records_buf: Vec<u8> = Vec::new();
                while let Some(write_event) = rx.recv().await {
                    match write_event {
                        WriteEvent::Record(record) => {
                            info!("Write record to pos [{}]", records_next_pos);
                            records_buf.clear();

                            StoreRecord {
                                timestamp: Some(clock.now()),
                                value: record.value,
                            }
                            .encode_length_delimited(&mut records_buf)
                            .expect("protobuf encoding should succeed");

                            if (records_next_pos as u64) + (records_buf.len() as u64)
                                > (u32::MAX as u64)
                            {
                                // TODO: store fact that segment is closed.
                                return Err(Status::out_of_range("Segment full"));
                            }

                            // TODO: await write and sync call pairs together
                            records_file.write_all(&records_buf).await?;
                            index_file
                                .write_all(&(records_next_pos).to_le_bytes())
                                .await?;

                            records_next_pos += records_buf.len() as u32;
                            next_offset += 1;
                        }

                        WriteEvent::Sync(sync_event) => {
                            records_file.sync_all().await?;
                            index_file.sync_all().await?;

                            let response = WriteResponse {
                                timestamp: Some(clock.now()),
                                next_offset,
                            };
                            info!("{:?}", response);

                            if let Some(response_tx) = sync_event.tx {
                                let _ = response_tx.send(response).map_err(|_| {
                                    warn!("Sync response listener hung up");
                                });
                            }
                        }
                    }
                }

                Ok(())
            }
            .instrument(info_span!("write loop")),
        );

        info!("Created segment; Open segment count: {}", db.len());

        Ok(())
    }
}

#[tonic::async_trait]
impl Store for StoreService {
    //

    #[instrument(res, err, skip(self, req), fields(req = ?req.get_ref()))]
    async fn create_segment(
        &self,
        req: Request<CreateSegmentRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = req.get_ref();

        let segment_id = SegmentID {
            topic: req.topic.clone(),
            segment_index: req.segment_index,
        };

        self.create_segment(segment_id).await?;

        Ok(Response::new(Empty {}))
    }

    #[instrument(err, skip(self, req), fields(req = ?req.get_ref()))] // TODO: don't log the req data!
    async fn write(&self, req: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        info!("start");
        let req = req.get_ref();

        let segment_id = SegmentID {
            topic: req.topic.clone(),
            segment_index: req.segment_index,
        };
        let tx = match self
            .segements_by_id
            .lock()
            .expect("Mutex should be valid")
            .get(&segment_id)
        {
            None => Err(Status::not_found("No such topic segment")),
            Some(sink) => sink
                .tx
                .clone()
                .ok_or_else(|| Status::failed_precondition("Segment is closed")),
        }?;

        for record in &req.records {
            tx.send(WriteEvent::Record(record.clone()))
                .await
                .map_err(|_e| Status::data_loss("Segment closed"))?
        }

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteEvent::Sync(SyncEvent {
            tx: Some(response_tx),
        }))
        .await
        .map_err(|_e| Status::data_loss("Segment closed"))?;

        Ok(Response::new(
            response_rx
                .await
                .map_err(|_e| Status::data_loss("Writer quit"))?,
        ))
    }

    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    #[instrument(err, skip(self, req), fields(req = ?req.get_ref()))]
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<Self::ReadStream>, Status> {
        let req = req.get_ref();
        let pathing = Pathing {
            root: self.root_path.clone(),
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
        for offset in record_range {
            // Frame: Decode variable-length record length_delimiter.
            // TODO: consider using fixed-length (4 byte?) record length_delimiter.
            let mut length_delimiter_buf = [0; 10]; // decode_length_delimiter needs exactly 10
            records_file.read(&mut length_delimiter_buf).await?;
            let record_length = prost::decode_length_delimiter(&length_delimiter_buf[..])?;
            let record_start_pos =
                file_pos as u64 + prost::length_delimiter_len(record_length) as u64;
            info!("offset:{}, length:{}", offset, record_length);
            records_file.seek(SeekFrom::Start(record_start_pos)).await?;

            // Read and send record
            let mut record_buf = vec![0u8; record_length];
            records_file.read_exact(&mut record_buf).await?;
            let record_buf = prost::bytes::Bytes::from(record_buf);
            let record = StoreRecord::decode(record_buf)?;
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

trait Clock: Send + Sync + core::fmt::Debug {
    fn now(&self) -> Timestamp;
}

#[derive(Debug)]
struct SystemClock {}
impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current epoch");
        Timestamp {
            seconds: now.as_secs(),
            nanos: now.subsec_nanos(),
        }
    }
}

#[derive(Debug)]
pub struct Pathing {
    root: String,
    topic: String,
    segment_index: u64,
}
impl Pathing {
    fn topics_dir_path(&self) -> String {
        format!("{}/topics", self.root)
    }
    fn segment_dir_path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.topics_dir_path(),
            self.topic,
            self.segment_index
        )
    }
    fn records_file_path(&self) -> String {
        format!("{}/records", self.segment_dir_path())
    }
    fn index_file_path(&self) -> String {
        format!("{}/index", self.segment_dir_path())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use tonic::Request;

    use crate::aeroflux::read_response::Event;
    use crate::aeroflux::store_server::Store;
    use crate::aeroflux::Empty;
    use crate::aeroflux::ReadRequest;
    use crate::aeroflux::ReadResponse;
    use crate::aeroflux::StoreRecord;
    use crate::aeroflux::Timestamp;
    use crate::aeroflux::WriteRecord;
    use crate::aeroflux::WriteRequest;
    use crate::Clock;
    use crate::Error;
    use crate::StoreService;

    #[test]
    fn hello() {
        let result = 2 + 2;
        assert_eq!(result, 4);
        assert_ne!("😇", "👿");
    }

    struct TestDir {
        path: String,
    }
    impl TestDir {
        fn new() -> TestDir {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("current epoch");
            let path = format!(
                "/tmp/aeroflux/test/{}.{:06}",
                now.as_secs(),
                now.subsec_micros()
            );
            //println!("TestDir: {}", path);
            TestDir { path }
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.path.as_str()).is_ok();
            //println!("TestDir drop {}", self.path);
        }
    }

    #[derive(Debug)]
    struct FixedClock {}
    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            FIXED_TIMESTAMP
        }
    }

    const FIXED_TIMESTAMP: Timestamp = Timestamp {
        seconds: 1234,
        nanos: 5678,
    };

    #[tokio::test]
    async fn create_write_read() -> Result<(), Error> {
        let test_dir = TestDir::new();
        let store_service = StoreService::new(
            //
            test_dir.path.clone(),
            Arc::new(FixedClock {}),
        );

        // Read, before the segment exists
        match store_service.read(new_read_request(0)).await {
            Err(e) => assert_eq!(tonic::Code::NotFound, e.code()),
            Ok(x) => panic!("result not an error: {:?}", x),
        }

        // Create the segment
        let _ = store_service
            .create_segment(crate::SegmentID {
                topic: "test".to_string(),
                segment_index: 42,
            })
            .await?;

        // TODO: test no records in response to read from empty segment

        // Write 2 records to segment
        let write_response = store_service
            .write(tonic::Request::new(WriteRequest {
                topic: "test".to_string(),
                segment_index: 42,
                records: vec![
                    WriteRecord {
                        value: "hello".into(),
                    },
                    WriteRecord {
                        value: "goodbye".into(),
                    },
                ],
            }))
            .await?;
        assert_eq!(2, write_response.get_ref().next_offset);

        // Read from offset 999: error: OutOfRange
        match store_service.read(new_read_request(999)).await {
            Err(e) => assert_eq!(tonic::Code::OutOfRange, e.code()),
            Ok(x) => panic!("result not an error: {:?}", x),
        }

        // Read from offset 0
        read_assert_eq(
            &store_service,
            0,
            vec![
                new_read_response_record("hello"),
                new_read_response_record("goodbye"),
                new_read_response_end(),
            ],
        )
        .await?;

        // Read from offset 1
        read_assert_eq(
            &store_service,
            1,
            vec![
                //
                new_read_response_record("goodbye"),
                new_read_response_end(),
            ],
        )
        .await?;

        // TODO: test error on read from invalid offset
        // TODO: test write/read more records to same segment
        // TODO: test new StoreService.on_startup

        Ok(())
    }

    fn new_read_request(from_offset: u32) -> Request<ReadRequest> {
        Request::new(ReadRequest {
            topic: "test".to_string(),
            segment_index: 42,
            from_offset,
        })
    }

    async fn read_assert_eq(
        store_service: &StoreService,
        from_offset: u32,
        expect: Vec<ReadResponse>,
    ) -> Result<(), Error> {
        let mut read_rx = store_service
            .read(new_read_request(from_offset))
            .await?
            .into_inner();
        let mut responses: Vec<ReadResponse> = vec![];
        while let Some(read_result) = read_rx.as_mut().recv().await {
            let read_response = read_result?;
            responses.push(read_response);
        }
        assert_eq!(expect, responses);
        Ok(())
    }

    fn new_read_response_record(s: &str) -> ReadResponse {
        ReadResponse {
            event: Some(Event::Record(StoreRecord {
                timestamp: Some(FIXED_TIMESTAMP),
                value: s.into(),
            })),
        }
    }
    fn new_read_response_end() -> ReadResponse {
        ReadResponse {
            event: Some(Event::End(Empty {})),
        }
    }
}
