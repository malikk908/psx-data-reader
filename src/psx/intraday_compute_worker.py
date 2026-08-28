"""Long-lived, coalescing worker for stock intraday computation.

The worker has one durable work signal: ``intraday_pipeline_state``.  It does
not import backend code or create a per-symbol work queue.  MongoDB remains the
source of truth for both recovery and idempotent retries.
"""

import argparse
import atexit
import logging
import multiprocessing
from multiprocessing import util
import os
import socket
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, replace
from datetime import datetime, time as time_type, timedelta, timezone

from pymongo import MongoClient

from psx.intraday_materializer import (
    DEFAULT_SYMBOL_BATCH_SIZE,
    IntradayMaterializer,
    build_materializer_from_database,
)
from psx.intraday_pipeline_state import (
    STATE_COLLECTION,
    IntradayPipelineState,
)
from psx.intraday_technical_writer import (
    INTRADAY_TIMEFRAMES,
    IntradayTechnicalWriter,
    build_intraday_technical_writer_from_databases,
    primary_database_settings,
)

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


logger = logging.getLogger(__name__)
UTC = timezone.utc
PKT = timezone(timedelta(hours=5))
EOD_CLOSE = time_type(16, 55)
FRIDAY_EOD_CLOSE = time_type(16, 55)
TECHNICAL_EXECUTION_MODES = ("sync", "thread", "process")

_PROCESS_TECHNICAL_WRITER = None
_PROCESS_MONGO_CLIENTS = ()
_PROCESS_CLIENT_FINALIZER = None


class LeaseLostError(RuntimeError):
    """Raised when a worker can no longer safely publish pipeline state."""


def utc_now():
    return datetime.now(UTC)


def _positive_float(value, name):
    result = float(value)
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


def _positive_int(value, name):
    result = int(value)
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


def _technical_execution_mode(value):
    mode = str(value).strip().lower()
    if mode not in TECHNICAL_EXECUTION_MODES:
        allowed = ", ".join(TECHNICAL_EXECUTION_MODES)
        raise ValueError(f"technical execution mode must be one of: {allowed}")
    return mode


def _close_process_clients():
    global _PROCESS_MONGO_CLIENTS
    for client in _PROCESS_MONGO_CLIENTS:
        client.close()
    _PROCESS_MONGO_CLIENTS = ()


def _initialize_process_technical_writer(company_data_uri, company_data_db_name, primary_uri, primary_db_name):
    """Create process-local Mongo handles; parent handles are never shared."""
    global _PROCESS_TECHNICAL_WRITER, _PROCESS_MONGO_CLIENTS, _PROCESS_CLIENT_FINALIZER

    company_client = MongoClient(company_data_uri, serverSelectionTimeoutMS=5000)
    primary_client = MongoClient(primary_uri, serverSelectionTimeoutMS=5000)
    try:
        company_client.admin.command("ping")
        primary_client.admin.command("ping")
        _PROCESS_MONGO_CLIENTS = (company_client, primary_client)
        _PROCESS_TECHNICAL_WRITER = build_intraday_technical_writer_from_databases(
            company_client[company_data_db_name],
            primary_client[primary_db_name],
        )
        # multiprocessing workers exit through multiprocessing.util, so keep a
        # finalizer in addition to atexit for explicit client cleanup.
        _PROCESS_CLIENT_FINALIZER = util.Finalize(
            None, _close_process_clients, exitpriority=10
        )
        atexit.register(_close_process_clients)
    except Exception:
        company_client.close()
        primary_client.close()
        raise


def _process_compute_and_save(task):
    timeframe, symbol, computed_at = task
    if _PROCESS_TECHNICAL_WRITER is None:
        raise RuntimeError("technical process worker was not initialized")
    return _PROCESS_TECHNICAL_WRITER.compute_and_save(
        symbol,
        timeframe,
        computed_at=computed_at,
    )


def company_data_database_settings(environ=None):
    """Return the explicitly configured company-data database settings.

    The ``MONGODB_INTRADAY_*`` names are retained because the market-watch
    poller already uses them.  The longer names make the ownership boundary
    unambiguous for deployments that run both databases separately.
    """
    environ = os.environ if environ is None else environ
    return {
        "uri": environ.get(
            "FINHISAAB_COMPANY_DATA_MONGO_URI",
            environ.get("MONGODB_INTRADAY_URI", "mongodb://127.0.0.1:27017/"),
        ),
        "db_name": environ.get(
            "FINHISAAB_COMPANY_DATA_DB_NAME",
            environ.get("MONGODB_INTRADAY_DB_NAME", "finhisaab_intraday"),
        ),
    }


@dataclass(frozen=True)
class WorkerConfig:
    company_data_uri: str
    company_data_db_name: str
    primary_uri: str
    primary_db_name: str
    poll_interval_seconds: float = 15.0
    retry_interval_seconds: float = 30.0
    lease_ttl_seconds: float = 120.0
    lease_renew_interval_seconds: float = 30.0
    technical_workers: int = 4
    # Direct construction defaults to thread mode so unit-test collaborators
    # remain injectable. Environment-configured workers default to processes.
    technical_execution_mode: str = "thread"
    symbol_batch_size: int = DEFAULT_SYMBOL_BATCH_SIZE
    incremental_window_minutes: int = 20
    eod_check_interval_seconds: float = 60.0
    closed_poll_interval_seconds: float = 60.0

    def __post_init__(self):
        _technical_execution_mode(self.technical_execution_mode)

    @classmethod
    def from_env(cls, environ=None):
        environ = os.environ if environ is None else environ
        company = company_data_database_settings(environ)
        primary = primary_database_settings(environ)
        lease_ttl = _positive_float(
            environ.get("INTRADAY_COMPUTE_LEASE_TTL_SECONDS", "120"),
            "INTRADAY_COMPUTE_LEASE_TTL_SECONDS",
        )
        renew_interval = _positive_float(
            environ.get("INTRADAY_COMPUTE_LEASE_RENEW_INTERVAL_SECONDS", str(lease_ttl / 4)),
            "INTRADAY_COMPUTE_LEASE_RENEW_INTERVAL_SECONDS",
        )
        if renew_interval >= lease_ttl:
            raise ValueError("lease renewal interval must be shorter than lease TTL")
        return cls(
            company_data_uri=company["uri"],
            company_data_db_name=company["db_name"],
            primary_uri=primary["uri"],
            primary_db_name=primary["db_name"],
            poll_interval_seconds=_positive_float(
                environ.get("INTRADAY_COMPUTE_POLL_INTERVAL_SECONDS", "15"),
                "INTRADAY_COMPUTE_POLL_INTERVAL_SECONDS",
            ),
            retry_interval_seconds=_positive_float(
                environ.get("INTRADAY_COMPUTE_RETRY_INTERVAL_SECONDS", "30"),
                "INTRADAY_COMPUTE_RETRY_INTERVAL_SECONDS",
            ),
            lease_ttl_seconds=lease_ttl,
            lease_renew_interval_seconds=renew_interval,
            technical_workers=_positive_int(
                environ.get("INTRADAY_COMPUTE_TECHNICAL_WORKERS", "4"),
                "INTRADAY_COMPUTE_TECHNICAL_WORKERS",
            ),
            technical_execution_mode=_technical_execution_mode(
                environ.get("INTRADAY_COMPUTE_TECHNICAL_EXECUTION_MODE", "process")
            ),
            symbol_batch_size=_positive_int(
                environ.get("INTRADAY_COMPUTE_SYMBOL_BATCH_SIZE", str(DEFAULT_SYMBOL_BATCH_SIZE)),
                "INTRADAY_COMPUTE_SYMBOL_BATCH_SIZE",
            ),
            incremental_window_minutes=_positive_int(
                environ.get("INTRADAY_COMPUTE_INCREMENTAL_WINDOW_MINUTES", "20"),
                "INTRADAY_COMPUTE_INCREMENTAL_WINDOW_MINUTES",
            ),
            eod_check_interval_seconds=_positive_float(
                environ.get("INTRADAY_COMPUTE_EOD_CHECK_INTERVAL_SECONDS", "60"),
                "INTRADAY_COMPUTE_EOD_CHECK_INTERVAL_SECONDS",
            ),
            closed_poll_interval_seconds=_positive_float(
                environ.get("INTRADAY_COMPUTE_CLOSED_POLL_INTERVAL_SECONDS", "60"),
                "INTRADAY_COMPUTE_CLOSED_POLL_INTERVAL_SECONDS",
            ),
        )


def _owner_id():
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"


class LeaseRenewer:
    """Renew a lease independently of synchronous materialization work."""

    def __init__(self, state, owner, ttl_seconds, interval_seconds, clock=utc_now):
        self.state = state
        self.owner = owner
        self.ttl_seconds = ttl_seconds
        self.interval_seconds = interval_seconds
        self.clock = clock
        self.lost = threading.Event()
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run, name="intraday-lease-renewer", daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop_event.wait(self.interval_seconds):
            try:
                renewed = self.state.renew_lease(self.owner, self.ttl_seconds, now=self.clock())
            except Exception:
                logger.exception("Intraday compute lease renewal failed")
                self.lost.set()
                return
            if renewed is None:
                logger.error("Intraday compute lease was lost by owner=%s", self.owner)
                self.lost.set()
                return

    def ensure(self):
        if self.lost.is_set():
            raise LeaseLostError("intraday compute lease is no longer owned")
        try:
            renewed = self.state.renew_lease(self.owner, self.ttl_seconds, now=self.clock())
        except Exception as exc:
            self.lost.set()
            raise LeaseLostError("intraday compute lease renewal failed") from exc
        if renewed is None:
            self.lost.set()
            raise LeaseLostError("intraday compute lease is no longer owned")

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(self.interval_seconds, 1.0))


class IntradayComputeWorker:
    """Coalescing compute runner with injectable collaborators for tests."""

    def __init__(
        self,
        state,
        materializer,
        technical_writer,
        config,
        owner=None,
        clock=utc_now,
        monotonic=time.monotonic,
        sleeper=time.sleep,
        lease_renewer_factory=LeaseRenewer,
        process_pool_factory=ProcessPoolExecutor,
        process_task=_process_compute_and_save,
        shutdown_callbacks=(),
    ):
        self.state = state
        self.materializer = materializer
        self.technical_writer = technical_writer
        self.config = config
        self.owner = owner or _owner_id()
        self.clock = clock
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.lease_renewer_factory = lease_renewer_factory
        self.process_pool_factory = process_pool_factory
        self.process_task = process_task
        self.shutdown_callbacks = list(shutdown_callbacks)
        self._technical_process_pool = None
        self._shutdown = False
        self._last_eod_check = 0.0

    def _technical_process_executor(self):
        if self._technical_process_pool is None:
            self._technical_process_pool = self.process_pool_factory(
                max_workers=self.config.technical_workers,
                initializer=_initialize_process_technical_writer,
                initargs=(
                    self.config.company_data_uri,
                    self.config.company_data_db_name,
                    self.config.primary_uri,
                    self.config.primary_db_name,
                ),
                mp_context=multiprocessing.get_context("spawn"),
            )
        return self._technical_process_pool

    def shutdown(self):
        """Stop reusable technical workers before closing parent Mongo clients."""
        if self._shutdown:
            return
        self._shutdown = True
        try:
            if self._technical_process_pool is not None:
                self._technical_process_pool.shutdown(wait=True, cancel_futures=True)
        finally:
            for callback in self.shutdown_callbacks:
                callback()

    @staticmethod
    def _as_utc(value):
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _trading_date(self, state_document):
        scraped_at = self._as_utc(state_document.get("latestScrapedAt"))
        return (scraped_at or self.clock()).astimezone(PKT).date().isoformat()

    def _write_changed_pairs(self, changed_by_timeframe, lease, computed_at):
        pairs = [
            (timeframe, symbol)
            for timeframe in INTRADAY_TIMEFRAMES
            for symbol in sorted(changed_by_timeframe.get(timeframe, []))
        ]
        if not pairs:
            return {"pairCount": 0, "written": 0}

        mode = _technical_execution_mode(self.config.technical_execution_mode)
        if mode == "sync":
            written = 0
            for timeframe, symbol in pairs:
                lease.ensure()
                self.technical_writer.compute_and_save(
                    symbol,
                    timeframe,
                    computed_at=computed_at,
                )
                written += 1
            return {"pairCount": len(pairs), "written": written}

        written = 0
        pair_iterator = iter(pairs)
        if mode == "process":
            executor = self._technical_process_executor()
        else:
            executor = ThreadPoolExecutor(max_workers=self.config.technical_workers)
        pending = {}
        failure = None

        def submit_next():
            try:
                timeframe, symbol = next(pair_iterator)
            except StopIteration:
                return False
            if mode == "process":
                future = executor.submit(
                    self.process_task,
                    (timeframe, symbol, computed_at),
                )
            else:
                future = executor.submit(
                    self.technical_writer.compute_and_save,
                    symbol,
                    timeframe,
                    computed_at=computed_at,
                )
            pending[future] = (timeframe, symbol)
            return True

        try:
            for _ in range(self.config.technical_workers):
                try:
                    if not submit_next():
                        break
                except Exception as exc:
                    failure = exc
                    break
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    pending.pop(future)
                    if failure is None:
                        try:
                            lease.ensure()
                        except Exception as exc:
                            failure = exc
                    try:
                        future.result()
                    except Exception as exc:
                        if failure is None:
                            failure = exc
                    else:
                        if failure is None:
                            written += 1
                            try:
                                submit_next()
                            except Exception as exc:
                                failure = exc
            if failure is not None:
                if mode == "process" and isinstance(failure, BrokenProcessPool):
                    executor.shutdown(wait=True, cancel_futures=True)
                    self._technical_process_pool = None
                raise failure
        finally:
            if mode == "thread":
                executor.shutdown(wait=True)
        return {"pairCount": len(pairs), "written": written}

    def _process_generation(self, state_document, lease):
        generation = state_document["generation"]
        generation_lag = max(0, generation - state_document.get("completedGeneration", 0))
        trading_date = self._trading_date(state_document)
        started = self.monotonic()
        materialized = self.materializer.rollup_incremental(
            trading_date,
            now=self.clock(),
            window_minutes=self.config.incremental_window_minutes,
        )
        lease.ensure()
        technical_started = self.monotonic()
        technical = self._write_changed_pairs(
            materialized.get("changedByTimeframe", {}), lease, self.clock()
        )
        lease.ensure()
        completed_at = self.clock()
        completed = self.state.mark_completed(
            self.owner,
            generation,
            completed_at,
            require_active=True,
        )
        if completed is None:
            raise LeaseLostError("lease lost before generation completion")

        elapsed = self.monotonic() - started
        summary = {
            "generation": generation,
            "tradingDate": trading_date,
            "materializer": materialized,
            "technical": technical,
            "elapsed_s": elapsed,
            "technical_elapsed_s": self.monotonic() - technical_started,
            "generation_lag": generation_lag,
        }
        latest_write = self._as_utc(state_document.get("latestRawWriteAt"))
        if latest_write:
            summary["raw_lag_s"] = max(0.0, (completed_at - latest_write).total_seconds())
        logger.info(
            "intraday compute generation=%s elapsed=%.2fs technical=%.2fs "
            "generation_lag=%d changed_pairs=%d raw_lag=%s",
            generation,
            elapsed,
            summary["technical_elapsed_s"],
            generation_lag,
            technical["pairCount"],
            f"{summary['raw_lag_s']:.2f}s" if "raw_lag_s" in summary else "n/a",
        )
        return summary

    def run_cycle(self, max_generations=None):
        """Claim one lease and coalesce all currently pending generations.

        A bounded ``max_generations`` is used by ``--once`` so an invocation
        has deterministic work even if the poller is active continuously.
        """
        cycle_started = self.monotonic()
        claimed = self.state.claim_lease(
            self.owner,
            ttl_seconds=self.config.lease_ttl_seconds,
            now=self.clock(),
        )
        if claimed is None:
            return {
                "status": "lease_unavailable",
                "generations": [],
                "elapsed_s": self.monotonic() - cycle_started,
            }

        lease = self.lease_renewer_factory(
            self.state,
            self.owner,
            self.config.lease_ttl_seconds,
            self.config.lease_renew_interval_seconds,
            clock=self.clock,
        )
        lease.start()
        generations = []
        try:
            while max_generations is None or len(generations) < max_generations:
                state_document = self.state.get() or claimed
                latest = state_document.get("generation", 0)
                completed = state_document.get("completedGeneration", 0)
                if latest <= completed:
                    break
                # Read the newest state immediately before processing. Any
                # intermediate generations are intentionally skipped.
                generations.append(self._process_generation(state_document, lease))
        except Exception as exc:
            self.state.record_failure(self.owner, exc, self.clock())
            logger.exception("Intraday compute cycle failed owner=%s", self.owner)
            return {
                "status": "failed",
                "generations": generations,
                "error": str(exc),
                "elapsed_s": self.monotonic() - cycle_started,
            }
        finally:
            lease.stop()
            self.state.release_lease(self.owner, self.clock())
        elapsed = self.monotonic() - cycle_started
        logger.info(
            "intraday compute cycle status=completed generations=%d elapsed=%.2fs",
            len(generations),
            elapsed,
        )
        return {"status": "completed", "generations": generations, "elapsed_s": elapsed}

    def reconcile_eod(self, trading_date, now=None):
        """Rebuild one PKT trading date, write changed technicals, then prune."""
        now = self._as_utc(now or self.clock())
        if not is_safe_eod_time(trading_date, now):
            raise ValueError("EOD reconciliation is only allowed after the buffered PKT close")
        claimed = self.state.claim_lease(
            self.owner,
            ttl_seconds=self.config.lease_ttl_seconds,
            now=now,
        )
        if claimed is None:
            return {"status": "lease_unavailable"}
        lease = self.lease_renewer_factory(
            self.state,
            self.owner,
            self.config.lease_ttl_seconds,
            self.config.lease_renew_interval_seconds,
            clock=self.clock,
        )
        lease.start()
        try:
            started = self.monotonic()
            state_at_start = self.state.get() or claimed
            generation = state_at_start.get("generation", 0)
            completed_generation = state_at_start.get("completedGeneration", 0)
            generation_date = self._trading_date(state_at_start)
            result = self.materializer.rollup_trading_date(trading_date, now=now)
            lease.ensure()
            technical = self._write_changed_pairs(
                result.get("changedByTimeframe", {}), lease, now
            )
            lease.ensure()
            raw_pruning = self.materializer.prune_raw_snapshots()
            candle_pruning = self.materializer.prune_candles()
            lease.ensure()
            if generation > completed_generation and generation_date == trading_date:
                if self.state.mark_completed(
                    self.owner,
                    generation,
                    self.clock(),
                    require_active=True,
                ) is None:
                    raise LeaseLostError("lease lost before EOD generation completion")
            if self.state.mark_eod_completed(self.owner, trading_date, self.clock()) is None:
                raise LeaseLostError("lease lost before EOD completion")
            summary = {
                "status": "completed",
                "tradingDate": trading_date,
                "materializer": result,
                "technical": technical,
                "rawPruning": raw_pruning,
                "candlePruning": candle_pruning,
                "elapsed_s": self.monotonic() - started,
            }
            logger.info(
                "intraday EOD trading_date=%s elapsed=%.2fs changed_pairs=%d "
                "raw_deleted=%d candle_deleted=%d",
                trading_date,
                summary["elapsed_s"],
                technical["pairCount"],
                raw_pruning.get("deletedCount", 0),
                candle_pruning.get("deletedCount", 0),
            )
            return summary
        except Exception as exc:
            self.state.record_failure(self.owner, exc, self.clock())
            logger.exception("Intraday EOD reconciliation failed owner=%s", self.owner)
            return {"status": "failed", "tradingDate": trading_date, "error": str(exc)}
        finally:
            lease.stop()
            self.state.release_lease(self.owner, self.clock())

    def run_forever(self):
        """Poll durable state conservatively; Mongo state is the work signal."""
        while True:
            summary = self.run_cycle()
            self._run_scheduled_eod()
            if summary["status"] == "failed":
                self.sleeper(self.config.retry_interval_seconds)
            elif summary["status"] == "lease_unavailable":
                self.sleeper(self.config.poll_interval_seconds)
            elif not is_compute_session_open(self.clock()):
                self.sleeper(self.config.closed_poll_interval_seconds)
            else:
                self.sleeper(self.config.poll_interval_seconds)

    def _run_scheduled_eod(self):
        """Run each available trading date once after its buffered PKT close."""
        now_monotonic = self.monotonic()
        if now_monotonic - self._last_eod_check < self.config.eod_check_interval_seconds:
            return None
        self._last_eod_check = now_monotonic
        state_document = self.state.get()
        if not state_document:
            return None
        trading_date = self._trading_date(state_document)
        if state_document.get("lastEodTradingDate") == trading_date:
            return None
        if not is_safe_eod_time(trading_date, self.clock()):
            return None
        return self.reconcile_eod(trading_date, now=self.clock())


def is_safe_eod_time(trading_date, now):
    """Require a weekday's buffered close before allowing destructive pruning."""
    day = datetime.strptime(trading_date, "%Y-%m-%d").date()
    if day.weekday() > 4:
        return False
    close = FRIDAY_EOD_CLOSE if day.weekday() == 4 else EOD_CLOSE
    pkt_now = now.astimezone(PKT)
    close_at = datetime.combine(day, close, tzinfo=PKT)
    return pkt_now >= close_at


def is_compute_session_open(now):
    """Return whether the poller can still publish normal intraday data."""
    pkt_now = now.astimezone(PKT)
    if pkt_now.weekday() > 4:
        return False
    current = pkt_now.time().replace(second=0, microsecond=0)
    if current < time_type(9, 25):
        return False
    if pkt_now.weekday() == 4 and time_type(12, 1) <= current < time_type(14, 30):
        return False
    close = FRIDAY_EOD_CLOSE if pkt_now.weekday() == 4 else time_type(15, 55)
    return current < close


def build_worker(config):
    """Connect explicitly to both databases and construct all worker stages."""
    company_client = MongoClient(config.company_data_uri, serverSelectionTimeoutMS=5000)
    primary_client = None
    try:
        company_client.admin.command("ping")
        primary_client = MongoClient(config.primary_uri, serverSelectionTimeoutMS=5000)
        primary_client.admin.command("ping")
        company_db = company_client[config.company_data_db_name]
        primary_db = primary_client[config.primary_db_name]
        state = IntradayPipelineState(company_db[STATE_COLLECTION])
        materializer = build_materializer_from_database(
            company_db,
            company_db,
            symbol_batch_size=config.symbol_batch_size,
        )
        technical_writer = build_intraday_technical_writer_from_databases(company_db, primary_db)
        state.ensure_indexes()
        materializer.ensure_indexes()
        technical_writer.ensure_indexes()
        worker = IntradayComputeWorker(
            state,
            materializer,
            technical_writer,
            config,
            shutdown_callbacks=(company_client.close, primary_client.close),
        )
        return worker, company_client, primary_client
    except Exception:
        company_client.close()
        if primary_client is not None:
            primary_client.close()
        raise


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="PSX stock intraday compute worker.")
    parser.add_argument("--once", action="store_true", help="process at most one newest generation and exit")
    parser.add_argument(
        "--eod", "--reconcile-eod", dest="eod", action="store_true",
        help="run guarded EOD reconciliation and pruning, then exit",
    )
    parser.add_argument("--trading-date", help="PKT trading date for --eod (YYYY-MM-DD)")
    parser.add_argument("--poll-interval", type=float, help="override state polling interval in seconds")
    parser.add_argument("--retry-interval", type=float, help="override failed-cycle retry interval in seconds")
    parser.add_argument("--technical-workers", type=int, help="override bounded technical write concurrency")
    parser.add_argument(
        "--technical-execution-mode",
        choices=TECHNICAL_EXECUTION_MODES,
        help="override technical pair execution (default: configured process mode)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = WorkerConfig.from_env()
    overrides = {}
    if args.poll_interval is not None:
        overrides["poll_interval_seconds"] = _positive_float(args.poll_interval, "--poll-interval")
    if args.retry_interval is not None:
        overrides["retry_interval_seconds"] = _positive_float(args.retry_interval, "--retry-interval")
    if args.technical_workers is not None:
        overrides["technical_workers"] = _positive_int(args.technical_workers, "--technical-workers")
    if args.technical_execution_mode is not None:
        overrides["technical_execution_mode"] = args.technical_execution_mode
    if overrides:
        config = replace(config, **overrides)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    worker, company_client, primary_client = build_worker(config)
    try:
        if args.eod:
            trading_date = args.trading_date
            if trading_date is None:
                state_document = worker.state.get() or {}
                trading_date = worker._trading_date(state_document)
            result = worker.reconcile_eod(trading_date)
            return 0 if result["status"] == "completed" else 1
        if args.once:
            result = worker.run_cycle(max_generations=1)
            return 0 if result["status"] in {"completed", "lease_unavailable"} else 1
        worker.run_forever()
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down.")
        return 0
    finally:
        worker.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
