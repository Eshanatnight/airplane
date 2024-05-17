use std::error::Error;

use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use chrono::Timelike;
use futures::TryStreamExt;
use serde_json::Value;

use crate::utils::{get_client, record_batches_to_json};

#[allow(unused)]
/// queries are locked to 9000 entries for ever call
pub async fn select_all() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select * from teststream limit 9000\", \"startTime\": \"1min\", \"endTime\": \"now\"{}",
        '{', '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

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

#[allow(unused)]
pub async fn select_all_last_min() -> Result<(), Box<dyn Error>> {
    let end = chrono::Utc::now();
    let start =
        end - chrono::Duration::from_std(humantime::parse_duration("1min").unwrap()).unwrap();

    let curr_min_start = end
        .with_second(0u32)
        .expect("Seconds is Valid")
        .with_nanosecond(0u32)
        .expect("Seconds is Valid");

    let curr_min_end = end
        .with_second(59u32)
        .expect("Seconds is Valid")
        .with_second(0u32)
        .expect("Seconds is Valid");

    let end = end.to_rfc3339();
    let start = start.to_rfc3339();

    let curr_min_start = curr_min_start.to_rfc3339();
    let curr_min_end = curr_min_end.to_rfc3339();

    let td = format!(
        "{}\"query\":\"select * from teststream limit 9000\", \"startTime\": \"{}\", \"endTime\": \"{}\"{}",
        '{',
        // curr_min_start,
        // "2024-05-15T09:22:00.00+00:00",
        start,
        // curr_min_end,
        // "2024-05-15T09:22:59.00+00:00",
        end,
        '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;

    println!("{}", q);
    Ok(())
}

#[allow(unused)]
pub async fn select_count() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select count(*) from teststream\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
        '{',  '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;
    println!("{}", q);
    Ok(())
}

#[allow(unused)]
pub async fn select_count_last_min() -> Result<(), Box<dyn Error>> {
    let end = chrono::Utc::now();
    let start =
        end - chrono::Duration::from_std(humantime::parse_duration("1min").unwrap()).unwrap();

    let curr_min_start = end
        .with_second(0u32)
        .expect("Seconds is Valid")
        .with_nanosecond(0u32)
        .expect("Seconds is Valid");

    let curr_min_end = end
        .with_second(59u32)
        .expect("Seconds is Valid")
        .with_second(0u32)
        .expect("Seconds is Valid");

    let end = end.to_rfc3339();
    let start = start.to_rfc3339();

    let curr_min_start = curr_min_start.to_rfc3339();
    let curr_min_end = curr_min_end.to_rfc3339();

    let td = format!(
        "{}\"query\":\"select count(*) from teststream\", \"startTime\": \"{}\", \"endTime\": \"{}\"{}",
        '{',
        // curr_min_start,
        // "2024-05-15T09:22:00.00+00:00",
        start,
        // curr_min_end,
        // "2024-05-15T09:22:59.00+00:00",
        end,
        '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;

    println!("{}", q);
    Ok(())
}

#[allow(unused)]
pub async fn select_count_alias() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select count(*) as count from teststream\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
        '{',  '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;

    Ok(())
}

#[allow(unused)]
pub async fn select_count_alias_last_min() -> Result<(), Box<dyn Error>> {
    let end = chrono::Utc::now();
    let start =
        end - chrono::Duration::from_std(humantime::parse_duration("1min").unwrap()).unwrap();

    let curr_min_start = end
        .with_second(0u32)
        .expect("Seconds is Valid")
        .with_nanosecond(0u32)
        .expect("Seconds is Valid");

    let curr_min_end = end
        .with_second(59u32)
        .expect("Seconds is Valid")
        .with_second(0u32)
        .expect("Seconds is Valid");

    let end = end.to_rfc3339();
    let start = start.to_rfc3339();

    let curr_min_start = curr_min_start.to_rfc3339();
    let curr_min_end = curr_min_end.to_rfc3339();

    let td = format!(
        "{}\"query\":\"select count(*) as count from teststream\", \"startTime\": \"{}\", \"endTime\": \"{}\"{}",
        '{',
        // curr_min_start,
        // "2024-05-15T09:22:00.00+00:00",
        start,
        // curr_min_end,
        // "2024-05-15T09:22:59.00+00:00",
        end,
        '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;

    println!("{}", q);
    Ok(())
}

#[allow(unused)]
pub async fn select_based_on_os(os: &str) -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select * from teststream where os = '{}' limit 9000\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
        '{', os,  '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;

    println!("{}", q);
    Ok(())
}

#[allow(unused)]
pub async fn select_distinct() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select distinct(device_id) from teststream limit 9000\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
        '{',  '}'
    );

    let mut client = get_client().await?;

    let ticket_data = serde_json::from_str::<Value>(&td)?;
    let mut ticket: Vec<u8> = vec![];
    serde_json::to_writer(&mut ticket, &ticket_data)?;

    let response = match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Error: {}", err);
            return Ok(());
        }
    };

    let batches: Vec<RecordBatch> = response.try_collect().await?;
    let q = batches.iter().collect::<Vec<&RecordBatch>>();
    let s = record_batches_to_json(&q);
    let s = s.into_iter().map(Value::Object).collect::<Vec<_>>();
    let q = serde_json::to_string_pretty(&s)?;

    println!("{}", q);
    Ok(())
}
