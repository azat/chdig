use clap::{Args, Parser};
use std::collections::HashMap;
use url;

#[derive(Parser)]
#[command(name = "chdig")]
#[command(bin_name = "chdig")]
#[command(author, version, about, long_about = None)]
pub struct ChDigOptions {
    #[command(flatten)]
    pub clickhouse: ClickHouseOptions,
}

#[derive(Args, Clone)]
pub struct ClickHouseOptions {
    #[arg(
        short('u'),
        long,
        value_name = "URL",
        default_value = "tcp://127.1",
        env = "CHDIG_URL"
    )]
    pub url: String,
    // Safe version for "url" (to show in UI)
    #[clap(skip)]
    pub url_safe: String,
    #[arg(short('c'), long)]
    pub cluster: Option<String>,
}

fn adjust_defaults(options: &mut ChDigOptions) {
    let mut url = url::Url::parse(options.clickhouse.url.as_ref()).unwrap();
    let mut url_safe = url.clone();

    // url_safe
    if url_safe.password().is_some() {
        url_safe.set_password(None).unwrap();
    }
    options.clickhouse.url_safe = url_safe.to_string();

    // some default settings in URL
    {
        let pairs: HashMap<_, _> = url_safe.query_pairs().into_owned().collect();
        let mut mut_pairs = url.query_pairs_mut();
        // default is: 500ms (too small)
        if !pairs.contains_key("connection_timeout") {
            mut_pairs.append_pair("connection_timeout", "5s");
        }
    }
    options.clickhouse.url = url.to_string();
}

// TODO:
// - config, I tried twelf but it is too buggy for now [1], let track [2] instead, I've also tried
//   viperus for the first version of this program, but it was even more buggy and does not support
//   new clap, and also it is not maintained anymore.
//
//     [1]: https://github.com/clap-rs/clap/discussions/2763
//     [2]: https://github.com/bnjjj/twelf/issues/15
//
// - clap_complete
pub fn parse() -> ChDigOptions {
    let mut options = ChDigOptions::parse();

    adjust_defaults(&mut options);

    return options;
}
