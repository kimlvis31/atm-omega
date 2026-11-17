"""Composition root for independent core, optional GUI and isolated research workers."""

# Imports
import argparse
import csv
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService, candle_row, parse_instrument
from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.candles import PostgresCandleStore
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.research.service import ResearchService
from omega.runtime.config import load_master
from omega.runtime.market_data_service import MarketDataService
from omega.runtime.service import CoreService
from omega.runtime.settings import atomic_json
from omega.strategies.registry import StrategyRegistry


def demo_rows(instrument="binance:usdm:BTCUSDT", count=360):
    rows = []
    start = (time.time_ns() // 1_000_000 // 60000 - count) * 60000
    for n in range(count):
        opened = 100 + 4 * math.sin(n / 13)
        closed = 100 + 4 * math.sin((n + 1) / 13)
        rows.append(
            {
                "instrument": instrument,
                "open_time_ms": start + n * 60000,
                "open": f"{opened:.4f}",
                "high": f"{max(opened, closed) + 0.5:.4f}",
                "low": f"{min(opened, closed) - 0.5:.4f}",
                "close": f"{closed:.4f}",
                "volume": str(10 + n % 7),
            }
        )
    return rows


def create_demo_file(root):
    target = root / "data" / "demo-candles.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        rows = demo_rows()
        with target.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    return target


class MarketView:
    def __init__(self, root, paper, *, demo=False):
        self.root, self.paper, self.demo = root, paper, demo
        self.lock = threading.Lock()
        self.cache, self.retry_after = {}, 0
        self._health = {
            "status": "available" if demo else "not_connected",
            "source": "demo" if demo else "database",
            "demo": demo,
        }

    def health(self):
        with self.lock:
            return dict(self._health)

    def __call__(self, instrument_key, limit):
        instrument = parse_instrument(instrument_key)
        with self.paper.lock:
            manual = self.paper.data["candles"].get(instrument.key, [])[-limit:]
            provenance = self.paper.data.get("feed", {}).get("provenance", {}).get(instrument.key)
            database_feed = provenance == "database_committed"
        if manual:
            return {
                "instrument": instrument.key,
                "candles": manual,
                "source": "database" if database_feed else "manual_paper",
                "quality": {
                    "status": "observed"
                    if database_feed
                    else "mixed_manual_and_database"
                    if provenance == "mixed_manual_and_database"
                    else "synthetic_or_user_supplied",
                    "interpolation_rate": 0,
                },
            }
        if self.demo:
            rows = [
                {k: v for k, v in row.items() if k != "instrument"}
                for row in demo_rows(instrument.key, min(limit, 2000))
            ]
            with self.lock:
                self._health = {"status": "demo", "source": "demo", "demo": True}
            return {
                "instrument": instrument.key,
                "candles": rows,
                "source": "demo",
                "quality": {"status": "synthetic_demo", "interpolation_rate": 0},
            }
        with self.lock:
            now = time.monotonic()
            self.cache = {key: value for key, value in self.cache.items() if now - value[0] < 2}
            cache_key = instrument.key, limit
            if cache_key in self.cache and now - self.cache[cache_key][0] < 2:
                return self.cache[cache_key][1]
            if now < self.retry_after:
                return self._unavailable(instrument.key)
            store = None
            try:
                settings = load_database_settings(self.root / "database.config.toml")
                store = PostgresCandleStore(settings)
                candles = store.latest(instrument, limit)
                rows = [candle_row(c) for c in candles]
                gaps = sum(
                    max(0, (b.open_time_ms - a.close_time_ms) // 60000)
                    for a, b in zip(candles, candles[1:], strict=False)
                )
                result = {
                    "instrument": instrument.key,
                    "candles": rows,
                    "source": "database",
                    "quality": {
                        "status": "gapped" if gaps else "observed",
                        "missing_minutes": gaps,
                        "interpolation_rate": 0,
                    },
                }
                if len(self.cache) >= 12:
                    del self.cache[min(self.cache, key=lambda key: self.cache[key][0])]
                self.cache[cache_key] = now, result
                self._health = {
                    "status": "available",
                    "source": "database",
                    "demo": False,
                    "note": "database snapshots; collector runs independently",
                }
                return result
            except (StorageError, ValidationError, OSError):
                self.retry_after = now + 10
                self._health = {"status": "not_connected", "source": "database", "demo": False}
                return self._unavailable(instrument.key)
            finally:
                if store is not None:
                    store.close()

    @staticmethod
    def _unavailable(instrument):
        return {
            "instrument": instrument,
            "candles": [],
            "source": "database",
            "quality": {"status": "unavailable", "reason": "database_or_schema_not_ready"},
        }


def build_core(root: Path, *, demo=False, trust_external=False, master_config=None):
    root = Path(root).resolve()
    config = load_master(root) if master_config is None else master_config
    registry = StrategyRegistry(root / "ts_modules", trust_external=trust_external)
    # Acquire the sole-owner lock BEFORE any provider reads/writes runtime state.
    core = CoreService(root)
    try:
        path = root / "state" / "paper.json"
        state = None
        if path.exists():
            if path.stat().st_size > 16_000_000:
                raise ValidationError("paper state exceeds limit")
            state = json.loads(path.read_text(encoding="utf-8"))

        def save_paper(data):
            if (
                len(json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8"))
                > 16_000_000
            ):
                raise StorageError(
                    "paper snapshot capacity reached; export and archive history before continuing"
                )
            atomic_json(path, data)

        paper = PaperTradingService(
            registry.create,
            create_features,
            persist=save_paper,
            state=state,
        )
        core.paper = paper
        core.modules_provider = registry.list_modules
        core.candles_provider = MarketView(root, paper, demo=demo)
        from omega.runtime.calendar_view import CalendarView

        core.calendar_candles_provider = CalendarView(root, enabled=not demo)
        from omega.runtime.auxiliary_research import run_auxiliary_export_job
        from omega.runtime.margin_research import run_margin_simulation_job

        core.research = ResearchService(
            root,
            handlers={
                "simulate_margin": run_margin_simulation_job,
                "export_auxiliary": run_auxiliary_export_job,
            },
        )
        from omega.runtime.shadow_inference import ShadowInferenceService

        core.shadow_inference = ShadowInferenceService(
            root, core.research, core_epoch=core.control.snapshot()["epoch"]
        )
        from omega.runtime.causal_recovery import CausalInferenceHost

        core.causal_inference = CausalInferenceHost(
            root,
            core_epoch=core.control.snapshot()["epoch"],
            training_job_provider=core.research.status,
            on_incident=lambda code: core.record_incident(code, "critical"),
            evidence_limits=config.neural_evidence,
            on_storage_warning=lambda code: core.record_incident(code, "warning"),
            core_instance=core.instance,
        )
        core.market_data = (
            MarketDataService(
                root,
                capture_rest_raw=True,
                raw_directory=config.paths["data_dir"] / "raw" / "candles",
            )
            if getattr(config, "capture_rest_raw", False)
            else MarketDataService(root)
        )
        paper.rules_provider = core.market_data.execution_rules
        paper.execution_policy_provider = lambda: core.settings.snapshot()["execution"]
        from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
        from omega.runtime.analysis_execution import AnalysisExecution
        from omega.runtime.analysis_library import AnalysisLibrary
        from omega.runtime.analysis_manifest import AnalysisInputManifest
        from omega.runtime.analysis_policy import policy_for_saved_analysis
        from omega.runtime.auxiliary_resources import PublicAuxiliaryResources, PublicDepthResources
        from omega.runtime.auxiliary_schedule import AuxiliarySchedule
        from omega.runtime.auxiliary_service import AuxiliaryService
        from omega.runtime.chart_series import ChartSeriesService
        from omega.runtime.depth_collection import DepthCollection
        from omega.runtime.leverage_reference import LeverageReferenceService
        from omega.runtime.paper_feed import PaperFeedSupervisor
        from omega.runtime.paper_inputs import PaperAnalysisSource

        def analysis_references():
            with paper.lock:
                references = [
                    {"analysis_id": row["analysis_snapshot"]["analysis_id"], "strategy_id": key}
                    for key, row in paper.data["strategies"].items()
                    if row.get("analysis_snapshot", {}).get("analysis_id")
                ]
            if core.analysis_execution is not None:
                references.extend(
                    {"analysis_id": row["analysis_id"], "strategy_id": "CA:" + row["execution_id"]}
                    for row in core.analysis_execution.snapshot()["instances"]
                    if row["state"] not in {"stopped", "error"}
                )
            return references

        core.analysis_library = AnalysisLibrary(
            root / "state" / "analysis-library.json", references_provider=analysis_references
        )
        core.analysis_execution = AnalysisExecution(
            root,
            lambda: CommittedAnalysisFrameSource(root),
            input_policy_factory=lambda frozen: policy_for_saved_analysis(root, frozen),
            manifest_factory=lambda: AnalysisInputManifest(
                root,
                quota_provider=lambda: (
                    core.settings.snapshot()["analysis"]["input_storage_mb"] * 1_000_000
                ),
            ),
        )
        core.chart_series = ChartSeriesService(lambda: CommittedAnalysisFrameSource(root))
        from omega.runtime.chart_observations import ChartObservationsView

        core.chart_observations = ChartObservationsView(root)
        if (root / "database.config.toml").exists():
            try:
                settings = load_database_settings(root / "database.config.toml")
                destination = database_destination_id(settings)
                resources = PublicAuxiliaryResources(root, destination)
                core.auxiliary_service = AuxiliaryService(
                    root, destination_id=destination, resource_factory=resources
                )
                core.auxiliary_schedule = AuxiliarySchedule(
                    root, destination_id=destination, resource_factory=resources
                )
                core.depth_collection = DepthCollection(
                    root, PublicDepthResources(root, destination)
                )
            except (OSError, StorageError, ValidationError):
                core.record_incident("AUXILIARY_CONFIG_UNAVAILABLE", "warning")

        def execution_incident(incident):
            core.record_incident(incident["code"], incident["severity"])
            core.log(
                "trading",
                "CRITICAL" if incident["severity"] == "critical" else "WARNING",
                incident["code"],
                account_id=incident.get("account_id"),
                instance=incident.get("instance"),
                instrument=incident.get("instrument"),
            )

        core.paper_feed = PaperFeedSupervisor(
            paper,
            PaperAnalysisSource(
                root,
                manifest_factory=lambda: AnalysisInputManifest(
                    root,
                    namespace="paper-inputs",
                    consumer_kind="paper_input",
                    quota_provider=lambda: (
                        core.settings.snapshot()["analysis"]["input_storage_mb"] * 1_000_000
                    ),
                ),
            ),
            on_incident=core.record_incident,
            on_execution_incident=execution_incident,
        )
        core.leverage_reference = LeverageReferenceService(root)
        from omega.runtime.margin_trading import MarginTradingService

        core.margin_trading = MarginTradingService(
            root,
            registry,
            lambda: CommittedAnalysisFrameSource(root),
            core_epoch=core.control.snapshot()["epoch"],
            entry_gate=lambda: not core._closing and not core.control.snapshot()["paused"],
            mutation_lock=core.owner_mutation_lock,
            on_incident=lambda code, detail: core.record_incident(code.upper(), "critical"),
            external_feature_provider=core.causal_inference.external_features,
            protection_source=core.market_data.collectors.protection_observations,
        )
        from omega.runtime.shutdown import ShutdownService
        from omega.runtime.shutdown_participants import (
            MarginShutdownParticipant,
            PaperShutdownParticipant,
        )

        def pause_shutdown(operation_id):
            while not core._closing:
                if core.owner_mutation_lock.acquire(timeout=0.1):
                    try:
                        if core._closing:
                            raise ValidationError("core is closing")
                        return core.pause_for_shutdown(operation_id)
                    finally:
                        core.owner_mutation_lock.release()
            raise ValidationError("core is closing")

        core.shutdown_management = ShutdownService(
            root,
            [PaperShutdownParticipant(core), MarginShutdownParticipant(core)],
            core_epoch=core.control.snapshot()["epoch"],
            pause_entries=pause_shutdown,
            on_incident=lambda code: core.record_incident(code, "critical"),
        )
        return core
    except Exception:
        core.close()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description="Omega paper core; real orders are disabled")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--demo", action="store_true", help="explicit synthetic chart and CSV data")
    parser.add_argument("--no-gui", action="store_true", help="override GUI autostart for this run")
    parser.add_argument(
        "--web-push",
        action="store_true",
        help="Opt in to mobile Web Push; requires private HTTPS origin and optional push extra",
    )
    parser.add_argument(
        "--push-subject",
        default="",
        help="Web Push contact HTTPS URL or mailto URI (used only with --web-push)",
    )
    console_flags = parser.add_mutually_exclusive_group()
    console_flags.add_argument(
        "--console", action="store_true", help="Use the core terminal for owner commands; type help"
    )
    console_flags.add_argument(
        "--no-console",
        action="store_true",
        help="Headless service; use console.bat / console.sh separately",
    )
    parser.add_argument(
        "--logs", action="store_true", help="Open a separate visible log terminal at startup"
    )
    parser.add_argument(
        "--trust-ts", action="store_true", help="allow selected local TS plugins to execute Python"
    )
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("port must be 0..65535")
    if args.web_push and not args.push_subject:
        parser.error(
            "--web-push requires --push-subject with your contact HTTPS host or mailto URI"
        )
    root = args.root.resolve()
    config = load_master(root)
    if args.demo:
        print(f"Synthetic demo CSV: {create_demo_file(root)}")
    service = build_core(root, demo=args.demo, trust_external=args.trust_ts, master_config=config)
    if args.web_push:
        push = service.enable_web_push(subject=args.push_subject)
        if not push["available"]:
            print("Mobile Web Push unavailable; check HTTPS, contact, push extra and private key.")
    service.refresh_leverage_on_start = config.refresh_leverage_on_start
    stopped = threading.Event()
    if service.shutdown_management is not None:
        service.shutdown_management.on_exit = stopped.set
    interactive = not args.no_console and (
        args.console
        or (config.console_interactive and sys.stdin is not None and sys.stdin.isatty())
    )
    for name in ("SIGINT", "SIGTERM"):
        signal.signal(getattr(signal, name), lambda *_: stopped.set())
    if interactive:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.default_int_handler)
    console_input = None
    try:
        if interactive:
            from omega.runtime.console_pipe import prepare_console_input

            # Own piped input before any child can inherit a pending Windows read.
            console_input = prepare_console_input(stopped=stopped)
        port = service.start(args.port)
        print(f"Omega paper core: http://127.0.0.1:{port}; Ctrl+C to stop. No real orders.")
        if config.log_autostart or args.logs:
            from omega.runtime.console import open_log_terminal

            try:
                open_log_terminal(root)
            except OSError:
                print(
                    "Log terminal could not open. Use run.bat logs --follow "
                    "/ bash run.sh logs --follow."
                )
        if config.gui_autostart and not args.no_gui:
            command = [sys.executable, "-I", "-m", "omega.gui.app", "--root", str(root)]
            subprocess.Popen(
                command,
                cwd=root,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        if interactive:
            from omega.runtime.console import ControlConsole

            options = {} if console_input is None else {"input_fn": console_input}
            ControlConsole(root).loop(stopped=stopped, shutdown_enabled=True, **options)
        else:
            stopped.wait()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            service.close()
        finally:
            if console_input is not None:
                console_input.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
