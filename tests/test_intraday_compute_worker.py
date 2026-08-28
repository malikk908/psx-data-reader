import unittest
from datetime import datetime, timezone

from psx.intraday_compute_worker import (
    INTRADAY_TIMEFRAMES,
    IntradayComputeWorker,
    WorkerConfig,
    company_data_database_settings,
    is_safe_eod_time,
    parse_args,
)
from psx.intraday_compute_benchmark import parse_args as parse_benchmark_args, run_benchmark


NOW = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)


class NoopLease:
    def __init__(self, *args):
        self.ensure_count = 0

    def start(self):
        pass

    def ensure(self):
        self.ensure_count += 1

    def stop(self):
        pass


class FakeState:
    def __init__(self, documents):
        self.documents = list(documents)
        self.index = 0
        self.completed = []
        self.failures = []
        self.released = False
        self.eod_completed = []

    def claim_lease(self, owner, ttl_seconds, now):
        return self.documents[0]

    def get(self):
        document = self.documents[min(self.index, len(self.documents) - 1)].copy()
        self.index += 1
        if self.completed:
            document["completedGeneration"] = self.completed[-1]
        return document

    def mark_completed(self, owner, generation, completed_at, require_active=False):
        self.completed.append(generation)
        return {"generation": generation}

    def record_failure(self, owner, error, failed_at):
        self.failures.append(str(error))

    def mark_eod_completed(self, owner, trading_date, completed_at):
        self.eod_completed.append(trading_date)
        return {"lastEodTradingDate": trading_date}

    def release_lease(self, owner, now):
        self.released = True


class FakeMaterializer:
    def __init__(self, changed=None, error=None):
        self.changed = changed or {}
        self.error = error
        self.calls = []
        self.eod_calls = []

    def rollup_incremental(self, trading_date, now, window_minutes):
        self.calls.append((trading_date, now, window_minutes))
        if self.error:
            raise self.error
        return {"changedByTimeframe": self.changed, "written": 1}

    def rollup_trading_date(self, trading_date, now):
        self.eod_calls.append("rollup")
        return {"changedByTimeframe": self.changed}

    def prune_raw_snapshots(self):
        self.eod_calls.append("raw_prune")
        return {"deletedCount": 2}

    def prune_candles(self):
        self.eod_calls.append("candle_prune")
        return {"deletedCount": 3}


class FakeWriter:
    def __init__(self):
        self.calls = []

    def compute_and_save(self, symbol, timeframe, computed_at):
        self.calls.append((symbol, timeframe, computed_at))
        return {"symbol": symbol, "timeframe": timeframe}


def config(technical_workers=2):
    return WorkerConfig(
        "mongodb://company",
        "company-data",
        "mongodb://primary",
        "primary",
        technical_workers=technical_workers,
    )


class ComputeWorkerTests(unittest.TestCase):
    def test_coalesces_to_newest_state_each_loop(self):
        state = FakeState([
            {"generation": 4, "completedGeneration": 1, "latestScrapedAt": NOW},
            {"generation": 9, "completedGeneration": 4, "latestScrapedAt": NOW},
        ])
        materializer = FakeMaterializer()
        worker = IntradayComputeWorker(
            state, materializer, FakeWriter(), config(), owner="worker", clock=lambda: NOW,
            lease_renewer_factory=NoopLease,
        )

        result = worker.run_cycle()

        self.assertEqual([item["generation"] for item in result["generations"]], [4, 9])
        self.assertEqual(state.completed, [4, 9])
        self.assertTrue(state.released)

    def test_failure_is_recorded_without_marking_generation_complete(self):
        state = FakeState([{"generation": 3, "completedGeneration": 0, "latestScrapedAt": NOW}])
        materializer = FakeMaterializer(error=RuntimeError("materializer failed"))
        worker = IntradayComputeWorker(
            state, materializer, FakeWriter(), config(), owner="worker", clock=lambda: NOW,
            lease_renewer_factory=NoopLease,
        )

        result = worker.run_cycle()

        self.assertEqual(result["status"], "failed")
        self.assertEqual(state.completed, [])
        self.assertEqual(len(state.failures), 1)

    def test_lease_loss_does_not_mark_generation_complete(self):
        class LostLease(NoopLease):
            def ensure(self):
                raise RuntimeError("lease lost")

        state = FakeState([{"generation": 3, "completedGeneration": 0, "latestScrapedAt": NOW}])
        worker = IntradayComputeWorker(
            state, FakeMaterializer(), FakeWriter(), config(), owner="worker", clock=lambda: NOW,
            lease_renewer_factory=LostLease,
        )

        result = worker.run_cycle()

        self.assertEqual(result["status"], "failed")
        self.assertEqual(state.completed, [])

    def test_dispatches_only_changed_timeframe_symbol_pairs(self):
        changed = {"1m": ["ABC", "DEF"], "15m": ["ABC"], "4h": []}
        state = FakeState([{"generation": 1, "completedGeneration": 0, "latestScrapedAt": NOW}])
        writer = FakeWriter()
        worker = IntradayComputeWorker(
            state, FakeMaterializer(changed), writer, config(), owner="worker", clock=lambda: NOW,
            lease_renewer_factory=NoopLease,
        )

        worker.run_cycle(max_generations=1)

        self.assertEqual(
            {(symbol, timeframe) for symbol, timeframe, _ in writer.calls},
            {("ABC", "1m"), ("DEF", "1m"), ("ABC", "15m")},
        )
        self.assertEqual(len(writer.calls), 3)

    def test_config_and_cli_keep_database_boundaries_explicit(self):
        environ = {
            "FINHISAAB_COMPANY_DATA_MONGO_URI": "mongodb://company",
            "FINHISAAB_COMPANY_DATA_DB_NAME": "company-data",
            "FINHISAAB_PRIMARY_DB_MONGO_URI": "mongodb://primary",
            "FINHISAAB_PRIMARY_DB_NAME": "main",
            "INTRADAY_COMPUTE_TECHNICAL_WORKERS": "3",
        }
        self.assertEqual(company_data_database_settings(environ), {
            "uri": "mongodb://company", "db_name": "company-data",
        })
        settings = WorkerConfig.from_env(environ)
        self.assertEqual(settings.primary_uri, "mongodb://primary")
        self.assertEqual(settings.technical_workers, 3)
        args = parse_args(["--once", "--eod", "--trading-date", "2026-08-28", "--technical-workers", "2"])
        self.assertTrue(args.once)
        self.assertTrue(args.eod)
        self.assertEqual(args.trading_date, "2026-08-28")
        self.assertEqual(args.technical_workers, 2)

    def test_eod_requires_buffered_weekday_close(self):
        before_close = datetime(2026, 8, 28, 11, 54, tzinfo=timezone.utc)  # 16:54 PKT
        after_close = datetime(2026, 8, 28, 11, 55, tzinfo=timezone.utc)  # 16:55 PKT
        saturday = datetime(2026, 8, 29, 12, tzinfo=timezone.utc)
        self.assertFalse(is_safe_eod_time("2026-08-28", before_close))
        self.assertTrue(is_safe_eod_time("2026-08-28", after_close))
        self.assertTrue(is_safe_eod_time("2026-08-28", saturday))
        self.assertFalse(is_safe_eod_time("2026-08-29", saturday))

    def test_eod_reconciles_before_pruning_and_completes_start_generation(self):
        state = FakeState([{"generation": 2, "completedGeneration": 0, "latestScrapedAt": NOW}])
        materializer = FakeMaterializer()
        worker = IntradayComputeWorker(
            state, materializer, FakeWriter(), config(), owner="worker", clock=lambda: NOW,
            lease_renewer_factory=NoopLease,
        )

        result = worker.reconcile_eod("2026-08-28", now=NOW)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(materializer.eod_calls, ["rollup", "raw_prune", "candle_prune"])
        self.assertEqual(state.completed, [2])
        self.assertEqual(state.eod_completed, ["2026-08-28"])

    def test_synthetic_benchmark_reports_all_requested_pairs(self):
        result = run_benchmark(symbol_count=2, candles=30, technical_workers=2, timeframes=("1m", "5m"))

        self.assertEqual(result["pairCount"], 4)
        self.assertEqual(result["candleCount"], 30)
        self.assertGreater(result["pairsPerSecond"], 0)
        self.assertEqual(parse_benchmark_args(["--symbols", "2"]).symbols, 2)


if __name__ == "__main__":
    unittest.main()
