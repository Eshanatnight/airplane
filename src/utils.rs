use std::error::Error;

use arrow_array::RecordBatch;
use arrow_flight::FlightClient;
use arrow_json::ArrayWriter;
use serde_json::{Map, Value};
use tonic::transport::Channel;

pub fn record_batches_to_json(records: &[&RecordBatch]) -> Vec<Map<String, Value>> {
    let buf = vec![];

    let mut writer = ArrayWriter::new(buf);
    writer.write_batches(records).unwrap();
    writer.finish().unwrap();

    let buf = writer.into_inner();

    let json_rows: Vec<Map<String, Value>> = match serde_json::from_reader(buf.as_slice()) {
        Ok(json) => json,
        Err(_) => vec![],
    };

    json_rows
}

pub async fn get_client() -> Result<FlightClient, Box<dyn Error>> {
    let channel = Channel::from_static("http://localhost:8002")
        .connect()
        .await?;

    let client = FlightClient::new(channel);
    let inn = client
        .into_inner()
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(4 * 1024 * 1024);

    let mut client = FlightClient::new_from_inner(inn);

    client.add_header("authorization", "Basic YWRtaW46YWRtaW4=")?;

    Ok(client)
}
