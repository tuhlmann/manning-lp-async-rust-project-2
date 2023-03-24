mod download_symbols;
mod output_symbols;
mod process_symbols;
mod stock_signal;
mod symbol_data;

use async_std::prelude::*;
use async_std::stream;
use chrono::prelude::*;
use clap::Parser;
use std::fs;
use std::time::Duration;
use xactor::*;

use crate::download_symbols::DownloadSymbols;
use crate::download_symbols::DownloadSymbolsActor;
use crate::output_symbols::OutputSymbolsDataActor;
use crate::process_symbols::ProcessSymbolsDataActor;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long)]
    symbols: Option<String>,
    #[clap(short, long)]
    from: String,
    #[clap(long)]
    symbol_file: Option<String>,
    #[clap(long)]
    out_file: String,
}

fn read_symbols_from_file(filename: &str) -> Option<Vec<String>> {
    let content = fs::read_to_string(filename).ok().map(|s| {
        s.split(',')
            .map(|s| String::from(s.trim()))
            .collect::<Vec<String>>()
    });

    content
}

#[xactor::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to = Utc::now();
    let file: Option<String> = opts.symbol_file;

    let symbols: Vec<String> = file
        .map(|f| read_symbols_from_file(f.as_str()))
        .flatten()
        .or({
            opts.symbols
                .map(|s| s.split(',').map(String::from).collect::<Vec<String>>())
        })
        .expect("Either pass --file or --symbols to pass the symbols to fetch.");

    // start the actors
    let _download_actor = DownloadSymbolsActor::start_default().await?;
    let _process_actor = ProcessSymbolsDataActor::start_default().await?;
    let _output_actor = OutputSymbolsDataActor::new(&opts.out_file).start().await?;

    // one initial run
    Broker::from_registry()
        .await?
        .publish(DownloadSymbols {
            symbols: symbols.clone(),
            beginning: from.clone(),
            end: to.clone(),
        })
        .expect("Could not talk to broker");
    // Run every 30 seconds
    let mut interval = stream::interval(Duration::from_secs(30));
    while let Some(_) = interval.next().await {
        Broker::from_registry()
            .await?
            .publish(DownloadSymbols {
                symbols: symbols.clone(),
                beginning: from.clone(),
                end: to.clone(),
            })
            .expect("Could not talk to broker");
    }

    Ok(())
}
