"""Synthetic technical-compute benchmark for the intraday worker.

This measures Python indicator throughput independently of MongoDB. Production
benchmark runs should use the worker's generation, lag, and MongoDB metrics.
"""

import argparse
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from psx.intraday_technical_writer import INTRADAY_LOOKBACK_CANDLES, INTRADAY_TIMEFRAMES
from psx.technical_snapshot import build_technical_snapshot


def _synthetic_rows(count):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(count):
        close = 100 + index * 0.03 + math.sin(index / 7) * 2
        rows.append({
            "date": start + timedelta(minutes=index),
            "open": close - 0.2,
            "high": close + 0.8,
            "low": close - 0.8,
            "close": close,
            "volume": 1000 + (index % 20) * 10,
        })
    return rows


def run_benchmark(symbol_count=500, candles=None, technical_workers=4, timeframes=None):
    timeframes = tuple(timeframes or INTRADAY_TIMEFRAMES)
    if candles is None:
        candles = max(INTRADAY_LOOKBACK_CANDLES[timeframe] for timeframe in timeframes)
    rows = _synthetic_rows(candles)
    pairs = [
        (f"S{symbol_index:04d}", timeframe)
        for symbol_index in range(symbol_count)
        for timeframe in timeframes
    ]

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=technical_workers) as executor:
        list(executor.map(lambda pair: build_technical_snapshot(pair[0], rows), pairs))
    elapsed = time.perf_counter() - started
    return {
        "symbolCount": symbol_count,
        "timeframes": list(timeframes),
        "pairCount": len(pairs),
        "candleCount": candles,
        "technicalWorkers": technical_workers,
        "elapsed_s": round(elapsed, 4),
        "pairsPerSecond": round(len(pairs) / elapsed, 2) if elapsed else None,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark Python intraday technical computation.")
    parser.add_argument("--symbols", type=int, default=500)
    parser.add_argument("--candles", type=int)
    parser.add_argument("--technical-workers", type=int, default=4)
    parser.add_argument("--timeframes", default=",".join(INTRADAY_TIMEFRAMES))
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    timeframes = tuple(value.strip() for value in args.timeframes.split(",") if value.strip())
    if not timeframes or any(value not in INTRADAY_TIMEFRAMES for value in timeframes):
        raise SystemExit("--timeframes must contain only supported intraday timeframes")
    if args.symbols <= 0 or args.technical_workers <= 0 or (args.candles is not None and args.candles <= 0):
        raise SystemExit("symbols, candles, and technical-workers must be greater than zero")
    print(json.dumps(run_benchmark(args.symbols, args.candles, args.technical_workers, timeframes)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
