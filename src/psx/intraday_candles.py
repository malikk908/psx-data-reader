"""Shared stock intraday candle construction and lifecycle helpers."""

from datetime import datetime, timedelta, timezone
import math


PKT = timezone(timedelta(hours=5))
PSX_FEED_DELAY_MS = 5 * 60 * 1000
TIMEFRAME_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
}
HIGHER_TIMEFRAMES = tuple((label, TIMEFRAME_MINUTES[label]) for label in ("5m", "15m", "30m", "1h", "4h"))
OHLCV_FIELDS = ("open", "high", "low", "close", "volume")


def _as_utc(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    raise TypeError("expected datetime")


def epoch_ms(value):
    return int(_as_utc(value).timestamp() * 1000)


def pkt_trading_date(value):
    return _as_utc(value).astimezone(PKT).date().isoformat()


def finite_number(value, fallback=0):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return fallback
    number = float(value)
    return number if math.isfinite(number) else fallback


def finite_or_none(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def bar_close_ms(timestamp, timeframe):
    minutes = TIMEFRAME_MINUTES.get(timeframe)
    return timestamp + minutes * 60 * 1000 if minutes else None


def bar_lifecycle(timestamp, timeframe, data_as_of):
    close_ms = bar_close_ms(timestamp, timeframe)
    if close_ms is None:
        return {"isForming": False, "barCloseAt": None}
    return {
        "isForming": epoch_ms(data_as_of) < close_ms,
        "barCloseAt": datetime.fromtimestamp(close_ms / 1000, timezone.utc),
    }


def candle_differs(incoming, existing):
    if not existing:
        return True
    return any(finite_number(incoming.get(field)) != finite_number(existing.get(field)) for field in OHLCV_FIELDS)


def lifecycle_differs(existing, lifecycle):
    if not existing:
        return True
    stored_forming = existing.get("isForming") is True
    if stored_forming != lifecycle["isForming"]:
        return True
    stored_close = existing.get("barCloseAt")
    stored_ms = epoch_ms(stored_close) if stored_close else None
    wanted_close = lifecycle.get("barCloseAt")
    wanted_ms = epoch_ms(wanted_close) if wanted_close else None
    return stored_ms != wanted_ms


def build_one_minute_candles(snapshots):
    """Build synthetic delayed 1m candles from one symbol's sorted raw snapshots."""
    candles = []
    previous_close = None
    previous_session_high = None
    previous_session_low = None

    for snapshot in snapshots:
        close = finite_or_none(snapshot.get("price"))
        if close is None:
            continue

        scrape_minute = _as_utc(snapshot["scraped_at_minute"])
        timestamp = epoch_ms(scrape_minute) - PSX_FEED_DELAY_MS
        trading_date = pkt_trading_date(snapshot["scraped_at"])

        if previous_close is None:
            open_price = finite_number(snapshot.get("open"), close)
            high = finite_number(snapshot.get("high"), max(open_price, close))
            low = finite_number(snapshot.get("low"), min(open_price, close))
            volume = finite_number(snapshot.get("volume"), 0)
        else:
            open_price = previous_close
            high = max(open_price, close)
            low = min(open_price, close)
            session_high = finite_or_none(snapshot.get("high"))
            session_low = finite_or_none(snapshot.get("low"))
            if session_high is not None and previous_session_high is not None and session_high > previous_session_high:
                high = max(high, session_high)
            if session_low is not None and previous_session_low is not None and session_low < previous_session_low:
                low = min(low, session_low)
            volume = finite_or_none(snapshot.get("volume_delta"))
            if volume is None or volume < 0:
                volume = 0

        candles.append({
            "symbol": snapshot["symbol"],
            "timeframe": "1m",
            "timestamp": timestamp,
            "tradingDate": trading_date,
            "date": datetime.fromtimestamp(timestamp / 1000, timezone.utc),
            "open": open_price,
            "high": max(high, open_price, close),
            "low": min(low, open_price, close),
            "close": close,
            "volume": volume,
        })

        previous_close = close
        session_high = finite_or_none(snapshot.get("high"))
        session_low = finite_or_none(snapshot.get("low"))
        if session_high is not None:
            previous_session_high = session_high if previous_session_high is None else max(previous_session_high, session_high)
        if session_low is not None:
            previous_session_low = session_low if previous_session_low is None else min(previous_session_low, session_low)

    return candles


def build_higher_timeframe_candles(one_minute_candles, timeframe, timeframe_minutes):
    """Aggregate one symbol's 1m candles into UTC-epoch-aligned buckets."""
    if not one_minute_candles:
        return []

    bucket_ms = timeframe_minutes * 60 * 1000
    buckets = {}
    for candle in sorted(one_minute_candles, key=lambda item: item["timestamp"]):
        bucket_start = candle["timestamp"] // bucket_ms * bucket_ms
        bucket = buckets.get(bucket_start)
        if bucket is None:
            buckets[bucket_start] = {
                "symbol": candle["symbol"],
                "timeframe": timeframe,
                "timestamp": bucket_start,
                "tradingDate": candle["tradingDate"],
                "date": datetime.fromtimestamp(bucket_start / 1000, timezone.utc),
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": finite_number(candle.get("volume"), 0),
            }
            continue

        bucket["high"] = max(bucket["high"], candle["high"], candle["open"], candle["close"])
        bucket["low"] = min(bucket["low"], candle["low"], candle["open"], candle["close"])
        bucket["close"] = candle["close"]
        bucket["volume"] += finite_number(candle.get("volume"), 0)

    candles = [buckets[key] for key in sorted(buckets)]
    for candle in candles:
        candle["high"] = max(candle["high"], candle["open"], candle["close"])
        candle["low"] = min(candle["low"], candle["open"], candle["close"])
    return candles
