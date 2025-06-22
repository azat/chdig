use anyhow::Result;
use chdig::chdig_main;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    return chdig_main().await;
}
