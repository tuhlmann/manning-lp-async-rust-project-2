use std::fs::File;
use std::io::Write;

use chrono::{DateTime, Utc};
use xactor::*;

#[xactor::message]
#[derive(Clone, Debug)]
pub struct OutputSymbolsData {
    pub beginning: DateTime<Utc>,
    pub symbol: String,
    pub last_price: f64,
    pub pct_change: f64,
    pub period_min: f64,
    pub period_max: f64,
    pub sma: f64,
}

pub struct OutputSymbolsDataActor {
    file_handle: std::fs::File,
}

impl OutputSymbolsDataActor {
    pub fn new(outfile: &str) -> Self {
        let mut outf = File::create(outfile).expect("Could not create output file");
        write!(outf, "period start,symbol,price,change %,min,max,30d avg\n")
            .expect("Could not write to outfile");
        OutputSymbolsDataActor { file_handle: outf }
    }

    fn output_symbols(&mut self, data: &OutputSymbolsData) -> () {
        // a simple way to output CSV data
        write!(
            self.file_handle,
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}\n",
            data.beginning.to_rfc3339(),
            data.symbol,
            data.last_price,
            data.pct_change,
            data.period_min,
            data.period_max,
            data.sma
        )
        .expect("Could not write to outfile");
    }
}

#[async_trait::async_trait]
impl Actor for OutputSymbolsDataActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<OutputSymbolsData>()
            .await
            .expect("Could not subscribe to <OutputSymbolsData> msg");
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<OutputSymbolsData> for OutputSymbolsDataActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: OutputSymbolsData) -> () {
        self.output_symbols(&msg);
    }
}
