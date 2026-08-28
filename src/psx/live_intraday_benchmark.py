"""Run the intraday worker against an isolated local MongoDB dataset."""

import argparse
import copy
import json
import time
import tracemalloc
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

from psx.intraday_compute_worker import IntradayComputeWorker, WorkerConfig
from psx.intraday_materializer import IntradayMaterializer
from psx.intraday_pipeline_state import IntradayPipelineState, STATE_COLLECTION
from psx.intraday_technical_writer import IntradayTechnicalWriter


def _mongo_opcounters(client):
    return dict(client.admin.command("serverStatus").get("opcounters", {}))


def _mongo_opcounter_delta(before, after):
    return {
        key: after.get(key, 0) - before.get(key, 0)
        for key in sorted(set(before) | set(after))
    }


def _parse_datetime(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def run_benchmark(
    uri,
    company_db_name,
    primary_db_name,
    trading_date,
    now,
    technical_workers=4,
):
    company_client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    primary_client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    tracemalloc.start()
    started = time.perf_counter()
    worker = None
    try:
        company_client.admin.command("ping")
        primary_client.admin.command("ping")
        mongo_ops_before = _mongo_opcounters(company_client)
        company_db = company_client[company_db_name]
        primary_db = primary_client[primary_db_name]
        raw_collection = company_db["intraday_klines_temp"]
        candle_collection = company_db["intraday_klines"]
        state_collection = company_db[STATE_COLLECTION]
        snapshot_collection = primary_db["stock_technical_snapshots"]

        candle_collection.delete_many({})
        state_collection.delete_many({})
        snapshot_collection.delete_many({})

        materializer = IntradayMaterializer(raw_collection, candle_collection)
        writer = IntradayTechnicalWriter(candle_collection, snapshot_collection)
        state = IntradayPipelineState(state_collection)
        materializer.ensure_indexes()
        writer.ensure_indexes()
        state.ensure_indexes()

        config = WorkerConfig(
            uri,
            company_db_name,
            uri,
            primary_db_name,
            technical_workers=technical_workers,
            technical_execution_mode="process",
        )
        worker = IntradayComputeWorker(state, materializer, writer, config, clock=lambda: now)
        scraped_at = now.replace(microsecond=0)
        state.publish_generation(scraped_at, written_at=scraped_at)
        result = worker.reconcile_eod(trading_date, now=now)
        elapsed = time.perf_counter() - started
        current, peak = tracemalloc.get_traced_memory()
        mongo_ops_after = _mongo_opcounters(company_client)
        candle_counts = {
            row["_id"]: row["count"]
            for row in candle_collection.aggregate([
                {"$group": {"_id": "$timeframe", "count": {"$sum": 1}}},
            ])
        }
        return {
            "tradingDate": trading_date,
            # The raw source stores the PKT date implicitly in scraped_at; this
            # benchmark database is intentionally loaded with one trading date.
            "rawSnapshots": raw_collection.count_documents({}),
            "rawSymbols": len(raw_collection.distinct("symbol")),
            "candleCounts": candle_counts,
            "snapshotCount": snapshot_collection.count_documents({}),
            "elapsed_s": round(elapsed, 4),
            "peakPythonAllocation_mb": round(peak / 1024 / 1024, 2),
            "currentPythonAllocation_mb": round(current / 1024 / 1024, 2),
            "mongoOps": _mongo_opcounter_delta(mongo_ops_before, mongo_ops_after),
            "technicalWorkers": technical_workers,
            "result": result,
        }
    finally:
        if worker is not None:
            worker.shutdown()
        tracemalloc.stop()
        company_client.close()
        primary_client.close()


def _latest_raw_by_symbol(collection):
    return list(collection.aggregate([
        {"$sort": {"symbol": 1, "scraped_at_minute": -1}},
        {"$group": {"_id": "$symbol", "document": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$document"}},
    ]))


def run_sustained_benchmark(
    uri,
    company_db_name,
    primary_db_name,
    cycles=5,
    technical_workers=4,
):
    """Append local-only poll updates and measure repeated real generations."""
    company_client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    primary_client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    worker = None
    tracemalloc.start()
    started = time.perf_counter()
    cycle_results = []
    try:
        company_client.admin.command("ping")
        primary_client.admin.command("ping")
        mongo_ops_before = _mongo_opcounters(company_client)
        company_db = company_client[company_db_name]
        primary_db = primary_client[primary_db_name]
        raw_collection = company_db["intraday_klines_temp"]
        candle_collection = company_db["intraday_klines"]
        state = IntradayPipelineState(company_db[STATE_COLLECTION])
        materializer = IntradayMaterializer(raw_collection, candle_collection)
        writer = IntradayTechnicalWriter(candle_collection, primary_db["stock_technical_snapshots"])
        config = WorkerConfig(
            uri,
            company_db_name,
            uri,
            primary_db_name,
            technical_workers=technical_workers,
            technical_execution_mode="process",
        )
        clock = [datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)]
        worker = IntradayComputeWorker(
            state,
            materializer,
            writer,
            config,
            clock=lambda: clock[0],
        )

        for cycle in range(1, cycles + 1):
            latest = _latest_raw_by_symbol(raw_collection)
            if not latest:
                raise ValueError("benchmark database has no raw snapshots")
            new_documents = []
            for source in latest:
                document = copy.deepcopy(source)
                document.pop("_id", None)
                document["scraped_at"] = source["scraped_at"] + timedelta(minutes=1)
                document["scraped_at_minute"] = source["scraped_at_minute"] + timedelta(minutes=1)
                document["price"] = float(source["price"]) + 0.01
                document["high"] = max(float(source.get("high", document["price"])), document["price"])
                document["volume"] = float(source.get("volume", 0)) + 100
                document["volume_delta"] = 100
                new_documents.append(document)
            raw_collection.insert_many(new_documents, ordered=False)

            latest_scrape = max(document["scraped_at"] for document in new_documents)
            if latest_scrape.tzinfo is None:
                latest_scrape = latest_scrape.replace(tzinfo=timezone.utc)
            clock[0] = latest_scrape + timedelta(minutes=10)
            state.publish_generation(latest_scrape, written_at=clock[0])
            cycle_started = time.perf_counter()
            result = worker.run_cycle(max_generations=1)
            elapsed = time.perf_counter() - cycle_started
            generation = result.get("generations", [{}])
            generation = generation[0] if generation else {}
            cycle_results.append({
                "cycle": cycle,
                "status": result["status"],
                "elapsed_s": round(elapsed, 4),
                "generation": generation.get("generation"),
                "generationLag": generation.get("generation_lag"),
                "rawLag_s": generation.get("raw_lag_s"),
                "technicalElapsed_s": generation.get("technical_elapsed_s"),
                "technicalPairs": generation.get("technical", {}).get("pairCount", 0),
                "candleWrites": generation.get("materializer", {}).get("written", 0),
            })

        current, peak = tracemalloc.get_traced_memory()
        mongo_ops_after = _mongo_opcounters(company_client)
        return {
            "cycles": cycle_results,
            "cycleCount": cycles,
            "totalElapsed_s": round(time.perf_counter() - started, 4),
            "peakPythonAllocation_mb": round(peak / 1024 / 1024, 2),
            "currentPythonAllocation_mb": round(current / 1024 / 1024, 2),
            "mongoOps": _mongo_opcounter_delta(mongo_ops_before, mongo_ops_after),
            "rawSnapshots": raw_collection.count_documents({}),
            "snapshotCount": primary_db["stock_technical_snapshots"].count_documents({}),
            "technicalWorkers": technical_workers,
        }
    finally:
        if worker is not None:
            worker.shutdown()
        tracemalloc.stop()
        company_client.close()
        primary_client.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run a live local MongoDB intraday benchmark.")
    parser.add_argument("--mode", choices=("eod", "sustained"), default="eod")
    parser.add_argument("--uri", default="mongodb://127.0.0.1:27040")
    parser.add_argument("--company-db", default="finhisaab_intraday_benchmark")
    parser.add_argument("--primary-db", default="finhisaab_primary_benchmark")
    parser.add_argument("--trading-date", default="2026-08-28")
    parser.add_argument("--now", default="2026-08-28T12:30:00Z")
    parser.add_argument("--technical-workers", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=5)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.technical_workers <= 0 or args.cycles <= 0:
        raise SystemExit("technical-workers and cycles must be greater than zero")
    if args.mode == "sustained":
        result = run_sustained_benchmark(
            args.uri,
            args.company_db,
            args.primary_db,
            args.cycles,
            args.technical_workers,
        )
    else:
        result = run_benchmark(
            args.uri,
            args.company_db,
            args.primary_db,
            args.trading_date,
            _parse_datetime(args.now),
            args.technical_workers,
        )
    print(json.dumps(result, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
