use chrono::{DateTime, Utc};
use xactor::*;

use crate::symbol_data::{process_symbols_data, FetchedSymbolData};

#[xactor::message]
#[derive(Clone)]
pub struct ProcessSymbolsData {
    pub fetched_symbols_data: Vec<FetchedSymbolData>,
    pub beginning: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Default)]
pub struct ProcessSymbolsDataActor;

#[async_trait::async_trait]
impl Actor for ProcessSymbolsDataActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<ProcessSymbolsData>()
            .await
            .expect("Could not subscribe to <ProcessSymbolsData> msg");
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<ProcessSymbolsData> for ProcessSymbolsDataActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ProcessSymbolsData) -> () {
        let output = process_symbols_data(&msg.fetched_symbols_data, &msg.beginning).await;

        if let Ok(mut broker) = Broker::from_registry().await {
            for o in output {
                broker
                    .publish(o)
                    .expect("Error publishing OutputSymbolsData")
            }
        }
    }
}
