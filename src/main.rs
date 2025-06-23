use anyhow::Result;
use chdig::chdig_main_async;
use std::env::args_os;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    return chdig_main_async(args_os()).await;
}
