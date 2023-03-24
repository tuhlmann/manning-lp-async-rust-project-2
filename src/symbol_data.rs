use std::io::{Error, ErrorKind};

use chrono::{DateTime, Utc};
use futures::future::join_all;
use time::OffsetDateTime;
use yahoo_finance_api as yahoo;

use crate::{
    output_symbols::OutputSymbolsData,
    stock_signal::{AsyncStockSignal, MaxPrice, MinPrice, PriceDifference, WindowedSMA},
};

#[derive(Debug, Clone)]
pub struct FetchedSymbolData {
    pub symbol: String,
    pub symbol_data: Vec<f64>,
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
pub async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();

    let b = OffsetDateTime::from_unix_timestamp(beginning.timestamp()).unwrap();
    let e = OffsetDateTime::from_unix_timestamp(end.timestamp()).unwrap();

    let response = provider
        .get_quote_history(symbol, b, e)
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

///
/// Convenience function that chains together the entire processing chain.
///
pub async fn process_symbols_data(
    fetched_symbols_data: &Vec<FetchedSymbolData>,
    beginning: &DateTime<Utc>,
) -> Vec<OutputSymbolsData> {
    let mut result: Vec<OutputSymbolsData> = vec![];
    for symbol_data in fetched_symbols_data {
        if let Some(re) = process_symbol_data(symbol_data, beginning).await {
            result.push(re);
        }
    }
    result
}

///
/// Convenience function that chains together the entire processing chain.
///
pub async fn process_symbol_data(
    fetched_symbol_data: &FetchedSymbolData,
    beginning: &DateTime<Utc>,
) -> Option<OutputSymbolsData> {
    if !fetched_symbol_data.symbol_data.is_empty() {
        let diff = PriceDifference {};
        let min = MinPrice {};
        let max = MaxPrice {};
        let sma = WindowedSMA { window_size: 30 };

        let period_max: f64 = max.calculate(&fetched_symbol_data.symbol_data).await?;
        let period_min: f64 = min.calculate(&fetched_symbol_data.symbol_data).await?;

        let last_price = *fetched_symbol_data.symbol_data.last()?;
        let (_, pct_change) = diff.calculate(&fetched_symbol_data.symbol_data).await?;
        let sma = sma.calculate(&fetched_symbol_data.symbol_data).await?;

        Some(OutputSymbolsData {
            beginning: beginning.clone(),
            symbol: fetched_symbol_data.symbol.clone(),
            last_price,
            pct_change: pct_change * 100.0,
            period_min,
            period_max,
            sma: *sma.last().unwrap_or(&0.0),
        })
    } else {
        None
    }
}

pub async fn fetch_symbol_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Option<FetchedSymbolData> {
    let closing_data = fetch_closing_data(symbol, beginning, end).await.ok();
    closing_data.map(|d| FetchedSymbolData {
        symbol: symbol.into(),
        symbol_data: d,
    })
}

pub async fn fetch_all_symbol_data(
    symbols: &Vec<String>,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Vec<FetchedSymbolData> {
    let handles = symbols
        .iter()
        .map(|symbol| fetch_symbol_data(symbol, beginning, end));
    let result = join_all(handles).await.into_iter().flatten().collect();

    result
}
