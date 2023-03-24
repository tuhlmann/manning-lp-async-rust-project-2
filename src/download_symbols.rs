use chrono::{DateTime, Utc};
use xactor::*;

use crate::{
    process_symbols::ProcessSymbolsData,
    symbol_data::{fetch_all_symbol_data, FetchedSymbolData},
};

#[xactor::message]
#[derive(Clone)]
pub struct DownloadSymbols {
    pub symbols: Vec<String>,
    pub beginning: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Default)]
pub struct DownloadSymbolsActor;

#[async_trait::async_trait]
impl Actor for DownloadSymbolsActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<DownloadSymbols>()
            .await
            .expect("Could not subscribe to <DownloadSymbols> msg");
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<DownloadSymbols> for DownloadSymbolsActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: DownloadSymbols) -> () {
        let fetched_symbols_data: Vec<FetchedSymbolData> =
            fetch_all_symbol_data(&msg.symbols, &msg.beginning, &msg.end).await;

        if let Ok(mut broker) = Broker::from_registry().await {
            broker
                .publish(ProcessSymbolsData {
                    fetched_symbols_data,
                    beginning: msg.beginning,
                    end: msg.end,
                })
                .expect("Error publishing ProcessSymbolsData");
        }
    }
}
