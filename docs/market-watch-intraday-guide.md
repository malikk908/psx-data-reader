# Market-Watch Intraday Snapshot Guide

This guide explains what the `market_watch_poller` stores, how to read it, and how it differs from regular 1-minute OHLCV candles.

It is written for consumers moving from exchange-style `1m OHLCV` data to the newer PSX `market-watch` snapshot dataset.

## What This Dataset Is

The poller in `src/psx/market_watch_poller.py` fetches the PSX `/market-watch` page once per cycle and stores one snapshot per symbol.

Each stored document is a point-in-time market snapshot, not an exchange-native 1-minute candle.

The source page provides:

- Last traded price at the time of scrape
- Session-level open/high/low values
- Session-cumulative traded volume
- Reference values such as `ldcp`, `change`, and `change_pct`

## Where It Is Stored

Collection:

- `finhisaab_intraday.intraday_klines_temp`

Storage behavior:

- Documents are keyed by `(symbol, scraped_at_minute)`
- `scraped_at_minute` is the UTC scrape timestamp floored to the minute
- `scraped_at` keeps the exact UTC scrape timestamp
- Documents expire automatically after 48 hours through a TTL index on `scraped_at`

Practical meaning:

- At most one document exists per symbol per UTC minute
- If the poller runs multiple times in the same minute, the same document is updated
- This makes the dataset minute-bucketed snapshots, not raw tick history

## Important Field Semantics

Raw fields from PSX:

- `price`: latest visible traded price at scrape time
- `open`: session open, not minute open
- `high`: session high, not minute high
- `low`: session low, not minute low
- `volume`: session-cumulative traded volume, not minute volume
- `ldcp`: previous close reference
- `change`, `change_pct`: derived from current price vs `ldcp`

Derived field added by us:

- `volume_delta`: change in cumulative volume vs the previous stored snapshot for the same symbol

`volume_delta` rules:

- Computed as `current.volume - previous.volume`
- Computed only when the previous snapshot is trustworthy
- If polling is interrupted, it represents change since the last retained snapshot, which may span more than one minute
- Set to `null` when there is no good comparison point

`volume_delta` becomes `null` when:

- This is the first retained snapshot for that symbol
- The previous stored snapshot belongs to a different PKT trading day
- The source cumulative volume moved backwards, which indicates a reset or bad comparison point

## How This Differs From Regular 1m OHLCV

Regular 1-minute OHLCV data usually means:

- One finalized candle per minute
- `open`, `high`, `low`, `close`, and `volume` all describe that exact minute
- `volume` is minute-traded volume for that candle

This dataset is different:

- It stores one snapshot per symbol per minute bucket
- `price` is the current price at scrape time
- `open`, `high`, and `low` are session-wide values from the exchange page
- `volume` is session-cumulative, not interval volume
- `volume_delta` is the best available interval-like volume, but only between stored snapshots

So this collection should be treated as a minute-bucketed snapshot feed, not as canonical 1m candles.

## What Consumers Should Use

For price-alert features:

- Use `price` as the current market price
- Use `scraped_at` or `scraped_at_minute` to check freshness and ordering
- Ignore `open`, `high`, and `low` if the old logic expected minute-candle values
- Use `volume_delta` only if the feature needs approximate volume movement between snapshots

For volume-aware logic:

- Use `volume` when you need total traded volume so far in the session
- Use `volume_delta` when you need incremental volume since the previous stored snapshot
- Do not treat `volume_delta` as guaranteed exchange-native 1-minute volume

## Migration Notes For Teams Coming From 1m OHLCV

Safe assumptions to keep:

- Newer documents represent newer market state
- `price` is the field to use for threshold-based alerting
- The dataset is good for current-price monitoring

Assumptions that must change:

- `open`, `high`, and `low` are not minute-bar values
- `volume` is not minute volume
- Missing or `null` `volume_delta` does not mean zero trading activity; it means the interval volume could not be derived safely

Recommended transition approach:

- Keep alert logic centered on `price`
- Treat this dataset as a near-real-time snapshot stream
- If true candle behavior is needed later, derive candles in a downstream aggregation step rather than assuming the stored snapshots already are candles

## Summary

This dataset is well-suited for live monitoring and alerting based on current price.

It is not a drop-in semantic replacement for regular 1-minute OHLCV candles. The biggest differences are:

- `open/high/low` are session values
- `volume` is cumulative session volume
- `volume_delta` is derived from successive stored snapshots and is the consumer-friendly replacement for raw cumulative volume when interval movement is needed
