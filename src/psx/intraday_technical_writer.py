"""Read durable intraday candles and upsert backend-compatible snapshots."""

import os
from datetime import datetime, timezone

import pymongo

from psx.technical_snapshot import build_technical_snapshot


INTRADAY_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "4h")
INTRADAY_LOOKBACK_CANDLES = {
    "1m": 500,
    "5m": 500,
    "15m": 400,
    "30m": 400,
    "1h": 300,
    "4h": 300,
}


class IntradayTechnicalWriter:
    """Materialize one symbol/timeframe into ``stock_technical_snapshots``.

    ``candle_collection`` is normally in the company-data database and
    ``snapshot_collection`` is explicitly supplied from the primary database.
    This keeps the two Mongo ownership boundaries visible at the call site.
    """

    def __init__(self, candle_collection, snapshot_collection):
        self.candle_collection = candle_collection
        self.snapshot_collection = snapshot_collection

    def ensure_indexes(self):
        return self.snapshot_collection.create_index(
            [("symbol", pymongo.ASCENDING), ("timeframe", pymongo.ASCENDING)],
            unique=True,
            name="symbol_timeframe_unique",
        )

    def compute_and_save(self, symbol, timeframe, lookback_candles=None, computed_at=None):
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_timeframe = str(timeframe or "").strip().lower()
        if not normalized_symbol:
            raise ValueError("symbol is required")
        if normalized_timeframe not in INTRADAY_TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        limit = lookback_candles if isinstance(lookback_candles, int) and not isinstance(lookback_candles, bool) and lookback_candles > 0 else INTRADAY_LOOKBACK_CANDLES[normalized_timeframe]
        rows = list(self.candle_collection.find(
            {"symbol": normalized_symbol, "timeframe": normalized_timeframe},
            {
                "_id": 0,
                "timestamp": 1,
                "date": 1,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "isForming": 1,
                "barCloseAt": 1,
                "updatedAt": 1,
            },
        ).sort([("timestamp", -1)]).limit(limit))
        rows.reverse()
        if not rows:
            return None

        source_rows = [
            {
                "date": row["date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row.get("volume") or 0,
            }
            for row in rows
        ]
        last_row = rows[-1]
        snapshot = build_technical_snapshot(
            normalized_symbol,
            source_rows,
            computed_at=computed_at or datetime.now(timezone.utc),
        )
        snapshot.update({
            "timeframe": normalized_timeframe,
            "barIsForming": last_row.get("isForming") is True,
            "barCloseAt": last_row.get("barCloseAt"),
            "source": {
                "baseCollection": "IntradayKline",
                "timeframe": normalized_timeframe,
                "priceBasis": "raw",
                "lookbackCandles": len(rows),
                "firstCandleDate": rows[0].get("date"),
                "lastCandleDate": last_row.get("date"),
                "sourceUpdatedAt": last_row.get("updatedAt"),
            },
        })
        self.snapshot_collection.update_one(
            {"symbol": normalized_symbol, "timeframe": normalized_timeframe},
            {"$set": snapshot},
            upsert=True,
        )
        return snapshot


def build_intraday_technical_writer_from_databases(company_data_db, primary_db):
    """Build a writer using separate company-data and primary DB handles."""
    return IntradayTechnicalWriter(
        company_data_db["intraday_klines"],
        primary_db["stock_technical_snapshots"],
    )


def primary_database_settings(environ=None):
    """Return explicit primary DB settings without reusing ingest config."""
    environ = os.environ if environ is None else environ
    return {
        "uri": environ.get("FINHISAAB_PRIMARY_DB_MONGO_URI", "mongodb://127.0.0.1:27017/"),
        "db_name": environ.get("FINHISAAB_PRIMARY_DB_NAME", "finhisaab"),
    }


StockTechnicalSnapshotWriter = IntradayTechnicalWriter
