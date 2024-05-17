mod tasks;
mod utils;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tasks::select_all().await?;
    tasks::select_count().await?;
    tasks::select_count_alias().await?;
    tasks::select_all_last_min().await?;
    tasks::select_count_alias_last_min().await?;
    tasks::select_distinct().await?;
    tasks::select_based_on_os("Windows").await?;
    tasks::select_based_on_os("Linux").await?;
    tasks::select_count_last_min().await?;
    Ok(())
}
