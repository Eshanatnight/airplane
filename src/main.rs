#![allow(unused_imports)]
use std::{collections::HashMap, error::Error, sync::Arc};

use arrow_array::RecordBatch;
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightClient,
    Ticket,
};
use arrow_schema::Schema;
use futures::TryStreamExt;
use serde_json::Map;
use serde_json::{json, Value};
use std::env::args;
use tonic::transport::Channel;
pub fn record_batches_to_json(records: &[&RecordBatch]) -> Vec<Map<String, Value>> {
    let buf = vec![];
    let mut writer = arrow_json::ArrayWriter::new(buf);
    writer.write_batches(records).unwrap();
    writer.finish().unwrap();

    let buf = writer.into_inner();

    let json_rows: Vec<Map<String, Value>> = match serde_json::from_reader(buf.as_slice()) {
        Ok(json) => json,
        Err(_) => vec![],
    };

    json_rows
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = args().collect::<Vec<_>>();
    args.pop().unwrap();

    let query = args
        .pop()
        .unwrap_or_else(|| "select * from teststream limit 50".to_owned());
    let start_time = args.pop().unwrap_or_else(|| "10days".to_owned());
    let end_time = args.pop().unwrap_or_else(|| "now".to_owned());
    let channel = Channel::from_static("http://localhost:8002")
        .connect()
        .await?;
    let mut client = FlightClient::new(channel);
    client.add_header("authorization", "Basic YWRtaW46YWRtaW4=")?;
    let td = format!(
        "{}\"query\":\"{}\", \"startTime\": \"{}\", \"endTime\": \"{}\"{}",
        '{', query, start_time, end_time, '}'
    );
    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;
    // let resp = client
    //     .get_schema(arrow_flight::FlightDescriptor {
    //         r#type: 1,
    //         cmd: vec![].into(),
    //         path: ["teststream".to_owned()].to_vec(),
    //     })
    //     .await?;
    //
    // dbg!(resp);

    let response = client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await?;

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;
    println!("{}", q);
    Ok(())
}
