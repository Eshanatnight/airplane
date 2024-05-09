use std::error::Error;

use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use serde_json::Value;

use tokio::{fs as AsyncFs, io::AsyncWriteExt};

use crate::utils::{get_client, record_batches_to_json};

pub async fn select_all() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select * from teststream\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
        '{',  '}'
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

pub async fn select_all_last_min() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select * from teststream\", \"startTime\": \"1min\", \"endTime\": \"now\"{}",
        '{', '}'
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

pub async fn select_count_last_min() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select count(*) from teststream\", \"startTime\": \"1min\", \"endTime\": \"now\"{}",
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

pub async fn select_count_alias_last_min() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select count(*) as count from teststream\", \"startTime\": \"1min\", \"endTime\": \"now\"{}",
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

pub async fn select_based_on_os(os: &str) -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select * from teststream where os = '{}' limit 900\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
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

pub async fn select_distinct() -> Result<(), Box<dyn Error>> {
    let td = format!(
        "{}\"query\":\"select distinct(device_id) from teststream limit 900\", \"startTime\": \"10days\", \"endTime\": \"now\"{}",
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
