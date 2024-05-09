mod tasks;
mod utils;

use std::env::args;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let mut args = args().collect::<Vec<_>>();
    // args.remove(0);
    //
    // let end_time = args.pop().unwrap_or_else(|| "now".to_owned());
    // let start_time = args.pop().unwrap_or_else(|| "10days".to_owned());
    // let query = args
    //     .pop()
    //     .unwrap_or_else(|| "select * from teststream".to_owned());
    // println!("{}:{}:{}", query, start_time, end_time);
    //
    // let td = format!(
    //     "{}\"query\":\"{}\", \"startTime\": \"{}\", \"endTime\": \"{}\"{}",
    //     '{', query, start_time, end_time, '}'
    // );
    // let ticket_data = serde_json::from_str::<Value>(&td)?;
    // let mut ticket: Vec<u8> = vec![];
    // serde_json::to_writer(&mut ticket, &ticket_data)?;
    //
    // let response = client
    //     .do_get(Ticket {
    //         ticket: ticket.into(),
    //     })
    //     .await?;
    //
    // let batches: Vec<RecordBatch> = response.try_collect().await?;
    // let q = batches.iter().collect::<Vec<&RecordBatch>>();
    // let s = record_batches_to_json(&q);
    // let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    // let q = serde_json::to_string_pretty(&s)?;
    // println!("{}", q);


    tasks::select_all().await?;
    // tasks::select_count().await?;
    // tasks::select_count_alias().await?;
    // tasks::select_all_last_min().await?;
    // tasks::select_count_alias_last_min().await?;
    // tasks::select_distinct().await?;
    // tasks::select_based_on_os("Windows").await?;
    // tasks::select_based_on_os("Linux").await?;
    // tasks::select_count_last_min().await?;
    Ok(())
}
