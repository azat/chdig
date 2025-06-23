use anyhow::Result;
use chdig::chdig_main_async;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    return chdig_main_async().await;
}
