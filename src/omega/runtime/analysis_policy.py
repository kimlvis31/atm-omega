"""Freeze public input selection policy independently of saved indicator parameters."""

from pathlib import Path

from omega.analysis.catalog import configured_output_fields, create_features, validate_configuration
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.runtime.analysis_manifest import InputPolicy


def policy_for_saved_analysis(root, frozen):
    try:
        provider, market, symbol = frozen["instrument"].split(":")
        instrument = Instrument(provider, Market(market), symbol)
        if provider != "binance" or instrument.market not in {Market.SPOT, Market.USDM}:
            raise ValidationError("unsupported saved analysis instrument")
        config = validate_configuration(frozen["configuration"])
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValidationError("invalid saved analysis input binding") from None
    fields = frozenset(
        field for field in configured_output_fields(config) if field.startswith("CLOSED_")
    )
    # Build a pure, empty graph rather than duplicating provider/input mappings.
    # Constructors allocate no network/DB handles and do not import SciPy.
    graph = create_features(instrument, fields, config)
    channels = graph.descriptor.required_fields - {"OHLCV"}
    if not channels:
        return None
    if channels - {"trade_flow", "metric", "depth"}:
        raise ValidationError("this analysis requires an auxiliary source not yet collected")
    settings = load_database_settings(Path(root) / "database.config.toml")
    if settings.host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValidationError("analysis input policy requires an explicit local Omega database")
    age = min(
        (row["maximum_metric_age_ms"] for row in config.get("market_sentiment", [])), default=330000
    )
    return InputPolicy(database_destination_id(settings), tuple(sorted(channels)), age)
