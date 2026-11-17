"""Lazy public-only resource composition shared by bounded and scheduled jobs."""

import re
import time
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.binance.auxiliary_budget import AuxiliaryPublicBudget
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.infrastructure.raw_archive import FileRawArchive

CHANNELS = {
    "trade_flow": "agg_trades",
    "trade_flow_repair": "agg_trades",
    "current_oi": "open_interest",
    "history_oi": "open_interest_history",
    "long_short_ratio": "long_short_ratio",
}
LOCAL_DESTINATIONS = {
    ("127.0.0.1", 55432, "omega", "omega_app"),
    ("127.0.0.1", 55433, "omega_test", "omega_app"),
}


def _instrument(plan):
    if (
        type(plan) is not dict
        or type(plan.get("kind")) is not str
        or plan.get("kind") not in CHANNELS
        or type(plan.get("parameters")) is not dict
    ):
        raise ValidationError("public auxiliary resources require a known job kind and parameters")
    try:
        key = plan["parameters"]["instrument"]
        provider, market, symbol = key.split(":")
        instrument = Instrument(provider, Market(market), symbol)
    except (KeyError, ValueError, TypeError, AttributeError):
        raise ValidationError("public auxiliary resources require a canonical instrument") from None
    if (
        instrument.provider != "binance"
        or instrument.market not in {Market.SPOT, Market.USDM}
        or (instrument.market == Market.SPOT and CHANNELS[plan["kind"]] != "agg_trades")
        or "channel" in plan
        and plan["channel"] != CHANNELS[plan["kind"]]
    ):
        raise ValidationError("unsupported public auxiliary instrument or channel")
    return instrument


class PublicAuxiliaryResources:
    """Create resources on the caller's worker, never during construction.

    The runtime owns the returned tuple and closes its store on that same worker.
    Window/repair business validation belongs to the existing collectors. The two
    optional factories are narrow transport/store seams for offline validation.
    No account credentials or order-capable client can be supplied here.
    """

    def __init__(
        self, root, destination_id, clock_ms=None, *, transport_factory=None, store_factory=None
    ):
        if (
            type(destination_id) is not str
            or re.fullmatch("[a-f0-9]{64}", destination_id) is None
            or any(
                value is not None and not callable(value)
                for value in (clock_ms, transport_factory, store_factory)
            )
        ):
            raise ValidationError(
                "public auxiliary resources require a destination and callable factories"
            )
        self.root, self.destination_id = Path(root), destination_id
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.transport_factory = transport_factory or UrllibTransport
        self.store_factory = store_factory or PostgresAuxiliaryStore

    def __call__(self, plan):
        instrument = _instrument(plan)
        return self._build(instrument, auxiliary=True)

    def _build(self, instrument, *, auxiliary):
        settings = load_database_settings(self.root / "database.config.toml")
        if (
            settings.host,
            settings.port,
            settings.dbname,
            settings.user,
        ) not in LOCAL_DESTINATIONS or (database_destination_id(settings) != self.destination_id):
            raise ValidationError(
                "public auxiliary resources require the bound local Omega database"
            )
        client = BinanceClient(
            "https://api.binance.com"
            if instrument.market == Market.SPOT
            else "https://fapi.binance.com",
            self.transport_factory(),
            credentials=None,
            enable_trading=False,
            budget=AuxiliaryPublicBudget(self.root / "state", clock=lambda: self.clock_ms() / 1000),
            clock_ms=self.clock_ms,
            ip_scope="binance-ip",
        )
        if auxiliary:
            client = BinanceAuxiliaryClient(instrument.market, client)
        archive = FileRawArchive(self.root / "state/auxiliary-raw")
        store = self.store_factory(settings)
        if getattr(store, "destination_id", None) != self.destination_id:
            try:
                store.close()
            except Exception:
                raise StorageError("mismatched auxiliary store cleanup failed") from None
            raise ValidationError("public auxiliary store destination changed")
        return client, archive, store


class PublicDepthResources(PublicAuxiliaryResources):
    """The same credential-free IP budget and DB binding for public depth only.

    The depth collector owns endpoint/sequence/weight validation; it receives the
    raw public client, never an account-capable or externally supplied client.
    Construction is idle and returned resources belong to the worker thread.
    """

    def __call__(self, instrument):
        if (
            not isinstance(instrument, Instrument)
            or instrument.provider != "binance"
            or instrument.market not in {Market.SPOT, Market.USDM}
        ):
            raise ValidationError("public depth requires a supported Binance instrument")
        return self._build(instrument, auxiliary=False)
