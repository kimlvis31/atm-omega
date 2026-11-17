"""Compose reusable feature providers from declared fields, independently of TS families."""

import json
import re
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256

from omega.analysis import market_catalog
from omega.analysis.aggregation import CandleAggregation
from omega.analysis.graph import FeatureGraph, FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.momentum import LegacyMomentumFeatures, MomentumConfiguration
from omega.analysis.moving_averages import LegacyMaFeatures, MaConfiguration
from omega.analysis.parabolic import LegacyParabolicFeatures, ParabolicConfiguration
from omega.analysis.swing import LegacySwingFeatures, SwingConfiguration
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument
from omega.ports.modules import ModuleDescriptor

FEATURE_CODES = (
    "EMA",
    "MMACD",
    "MFI",
    "DMIxADX",
    "SWING",
    "PSAR",
    "NES",
    "WOI",
    "MST",
    "STS",
    "IVP",
    "TPD",
    "SMA",
    "WMA",
    "BOL",
)

_MA_KEYS = {"interval_id", "ema_period", "mmacd_periods", "signal_period", "price_precision"}
_MOMENTUM_KEYS = {"interval_id", "analysis_code", "nSamples", "line_index"}
_SWING_KEYS = {"interval_id", "line_index", "swing_range"}
_PARABOLIC_KEYS = {
    "interval_id",
    "line_index",
    "start",
    "acceleration",
    "maximum",
    "price_precision",
}


def validate_configuration(value: dict) -> dict:
    """Use the installed producers' own validators; never retain unsupported fields."""
    if not isinstance(value, dict) or set(value) - (
        {
            "moving_averages",
            "momentum",
            "swing",
            "parabolic",
        }
        | market_catalog.GROUPS.keys()
    ):
        raise ValidationError("only installed analysis groups are supported")
    result, seen = {"moving_averages": [], "momentum": []}, set()
    if "swing" in value:
        result["swing"] = []
    if "parabolic" in value:
        result["parabolic"] = []
    for group, keys, constructor in (
        ("moving_averages", _MA_KEYS, MaConfiguration),
        ("momentum", _MOMENTUM_KEYS, MomentumConfiguration),
        ("swing", _SWING_KEYS, SwingConfiguration),
        ("parabolic", _PARABOLIC_KEYS, ParabolicConfiguration),
    ):
        if group in {"swing", "parabolic"} and group not in value:
            continue
        rows = value.get(group, [])
        if not isinstance(rows, list) or len(rows) > 64:
            raise ValidationError("analysis definitions must be a bounded list")
        for row in rows:
            if not isinstance(row, dict) or set(row) - keys:
                raise ValidationError("unknown analysis parameter")
            parameters = deepcopy(row)
            if group == "moving_averages":
                periods = parameters.get("mmacd_periods", [])
                if not isinstance(periods, list):
                    raise ValidationError("MMACD periods must be a JSON list")
                parameters["mmacd_periods"] = tuple(periods)
            try:
                configuration = constructor(**parameters)
            except (TypeError, ValueError) as exc:
                raise ValidationError(str(exc)) from exc
            canonical = asdict(configuration)
            if group == "moving_averages":
                canonical["mmacd_periods"] = sorted(canonical["mmacd_periods"])
                identity = (group, canonical["interval_id"])
            elif group == "momentum":
                identity = (
                    group,
                    canonical["interval_id"],
                    canonical["analysis_code"],
                    canonical["line_index"],
                )
            else:
                identity = (group, canonical["interval_id"], canonical["line_index"])
            if identity in seen:
                raise ValidationError(
                    "duplicate analysis output; use one definition per interval/line"
                )
            seen.add(identity)
            result[group].append(canonical)
        result[group].sort(
            key=lambda row: (
                row["interval_id"],
                row.get("analysis_code", ""),
                row.get("line_index", 0),
            )
        )
    extra = market_catalog.canonical_groups(value)
    count = len(seen) + sum(len(rows) for rows in extra.values())
    if not count or count > 64:
        raise ValidationError("one to sixty-four installed analysis definitions are required")
    result.update(extra)
    return result


def _configured_definitions(canonical):
    moving = tuple(
        MaConfiguration(**(row | {"mmacd_periods": tuple(row["mmacd_periods"])}))
        for row in canonical["moving_averages"]
    )
    momentum = tuple(MomentumConfiguration(**row) for row in canonical["momentum"])
    swing = tuple(SwingConfiguration(**row) for row in canonical.get("swing", []))
    parabolic = tuple(ParabolicConfiguration(**row) for row in canonical.get("parabolic", []))
    intervals = tuple(sorted({row.interval_id for row in (*moving, *momentum, *swing, *parabolic)}))
    return intervals, moving, momentum, swing, parabolic


def _provided_fields(intervals, moving, momentum, swing, parabolic):
    provided = {f"CLOSED_{iid}" for iid in intervals}
    for row in moving:
        if row.ema_period is not None:
            provided.add(f"{row.interval_id}_EMA_0_EMA")
        if row.mmacd_periods:
            provided.update(
                f"{row.interval_id}_MMACD_{suffix}"
                for suffix in ("MSDELTAABSMAREL", "MSDELTA", "MSDELTAABSMA")
            )
    for row in momentum:
        suffixes = (
            ("MFI", "MFIDEVABSMA", "MFIDEVABSMAREL")
            if row.analysis_code == "MFI"
            else ("DMIxADX", "DMIxADXABSMA", "DMIxADXABSMAREL")
        )
        provided.update(
            f"{row.interval_id}_{row.analysis_code}_{row.line_index}_{suffix}"
            for suffix in suffixes
        )
    for row in swing:
        provided.update(
            f"{row.interval_id}_SWING_{row.line_index}_{suffix}"
            for suffix in ("LSTIMESTAMP", "LSPRICE", "LSTYPE", "CONFIRMED_AT_MS")
        )
    for row in parabolic:
        provided.update(
            f"{row.interval_id}_PSAR_{row.line_index}_{suffix}" for suffix in ("PSAR", "DCC")
        )
    return frozenset(provided)


def configured_output_fields(configuration: dict) -> frozenset[str]:
    """Declared public fields for installed saved analysis, without constructing IO."""
    canonical = validate_configuration(configuration)
    return _provided_fields(*_configured_definitions(canonical)) | market_catalog.output_fields(
        {name: canonical[name] for name in market_catalog.GROUPS if name in canonical}
    )


class EmptyFeatures:
    descriptor = ModuleDescriptor("empty_features", "1", frozenset(), False)

    def update(self, candle):
        return {}

    def checkpoint(self):
        return {"version": 1, "kind": "empty"}

    def restored(self, state, instrument):
        if state != self.checkpoint():
            raise ValidationError("empty analysis checkpoint mismatch")
        return EmptyFeatures()


def create_features(instrument, required_fields, configuration=None):
    config = {} if configuration is None else configuration
    if not isinstance(config, dict):
        raise ValidationError("analysis configuration must be an object")
    if (
        not isinstance(required_fields, (set, frozenset, tuple, list))
        or len(required_fields) > 256
        or any(not isinstance(field, str) for field in required_fields)
    ):
        raise ValidationError("required analysis fields must be a bounded collection of names")
    if (
        {"moving_averages", "momentum", "swing", "parabolic"} | market_catalog.GROUPS.keys()
    ) & config.keys():
        canonical = validate_configuration(config)
        definitions = _configured_definitions(canonical)
        intervals, moving, momentum, swing, parabolic = definitions
        extra = {name: canonical[name] for name in market_catalog.GROUPS if name in canonical}
        intervals = tuple(sorted(set(intervals) | market_catalog.intervals(extra)))
        provided = _provided_fields(*definitions) | market_catalog.output_fields(extra)
        if missing := set(required_fields) - provided:
            raise ValidationError(
                f"saved analysis does not provide required fields: {sorted(missing)}"
            )
        if not required_fields:
            return EmptyFeatures()
        return _compose(instrument, intervals, moving, momentum, swing, parabolic, extra)
    if set(config) - {"ema_period", "mmacd_periods", "signal_period", "momentum_samples"}:
        raise ValidationError("unknown analysis configuration")
    try:
        MaConfiguration(
            0,
            config.get("ema_period", 20),
            tuple(config.get("mmacd_periods", [12, 26])),
            config.get("signal_period", 9),
        )
        MomentumConfiguration(0, "MFI", config.get("momentum_samples", 14))
    except TypeError as exc:
        raise ValidationError("invalid legacy analysis parameters") from exc
    intervals, ma, momentum, swing, parabolic = set(), {}, set(), set(), set()
    for field in sorted(required_fields):
        closed = re.fullmatch(r"CLOSED_(\d+)", field)
        average = re.fullmatch(
            r"(\d+)_(?:EMA_0_EMA|MMACD_(?:MSDELTAABSMAREL|MSDELTA|MSDELTAABSMA))", field
        )
        oscillator = re.fullmatch(r"(\d+)_(MFI|DMIxADX)_0_(\w+)", field)
        pivot = re.fullmatch(
            r"(\d+)_SWING_([0-9])_(LSTIMESTAMP|LSPRICE|LSTYPE|CONFIRMED_AT_MS)", field
        )
        trailing = re.fullmatch(r"(\d+)_PSAR_([0-9])_(PSAR|DCC)", field)
        if closed:
            intervals.add(int(closed[1]))
        elif average:
            interval = int(average[1])
            intervals.add(interval)
            ma.setdefault(interval, set()).add("ema" if "_EMA_" in field else "mmacd")
        elif oscillator:
            suffixes = (
                {"MFI", "MFIDEVABSMA", "MFIDEVABSMAREL"}
                if oscillator[2] == "MFI"
                else {"DMIxADX", "DMIxADXABSMA", "DMIxADXABSMAREL"}
            )
            if oscillator[3] not in suffixes:
                raise ValidationError(f"no installed analysis provider for {field}")
            intervals.add(int(oscillator[1]))
            momentum.add((int(oscillator[1]), oscillator[2]))
        elif pivot:
            intervals.add(int(pivot[1]))
            swing.add((int(pivot[1]), int(pivot[2])))
        elif trailing:
            intervals.add(int(trailing[1]))
            parabolic.add((int(trailing[1]), int(trailing[2])))
        else:
            raise ValidationError(f"no installed analysis provider for {field}")
    if not intervals:
        return EmptyFeatures()
    moving_definitions, momentum_definitions = (), ()
    if ma:
        moving_definitions = tuple(
            MaConfiguration(
                interval,
                config.get("ema_period", 20) if "ema" in kinds else None,
                tuple(config.get("mmacd_periods", [12, 26])) if "mmacd" in kinds else (),
                config.get("signal_period", 9),
            )
            for interval, kinds in sorted(ma.items())
        )
    if momentum:
        momentum_definitions = tuple(
            MomentumConfiguration(i, kind, config.get("momentum_samples", 14))
            for i, kind in sorted(momentum)
        )
    swing_definitions = tuple(
        SwingConfiguration(iid, line, 0.005 * (line + 1)) for iid, line in sorted(swing)
    )
    parabolic_definitions = tuple(
        ParabolicConfiguration(iid, line, acceleration=0.005 * (line + 1))
        for iid, line in sorted(parabolic)
    )
    return _compose(
        instrument,
        tuple(sorted(intervals)),
        moving_definitions,
        momentum_definitions,
        swing_definitions,
        parabolic_definitions,
    )


def _compose(instrument, intervals, moving, momentum, swing=(), parabolic=(), extra=None):
    aggregation = CandleAggregation(instrument, intervals)
    nodes = [FeatureNode("bars", aggregation, public_fields=aggregation.public_fields)]
    if moving:
        nodes.append(FeatureNode("ma", LegacyMaFeatures(instrument, moving), ("bars",)))
    if momentum:
        nodes.append(
            FeatureNode("momentum", LegacyMomentumFeatures(instrument, momentum), ("bars",))
        )
    if swing:
        nodes.append(FeatureNode("swing", LegacySwingFeatures(instrument, swing), ("bars",)))
    if parabolic:
        nodes.append(
            FeatureNode("parabolic", LegacyParabolicFeatures(instrument, parabolic), ("bars",))
        )
    if extra and any(extra.values()):
        nodes.extend(market_catalog.nodes(instrument, extra))
        return MarketFeatureGraph(instrument, tuple(nodes))
    return FeatureGraph(tuple(nodes))


# Execution Projection
def _execution_profile(configuration, required):
    groups = {"moving_averages", "momentum", "swing", "parabolic"} | market_catalog.GROUPS.keys()
    if type(configuration) is not dict or not configuration or configuration.keys() - groups:
        raise ValidationError("execution projection requires an explicit typed saved profile")
    # The default validator deliberately rejects an empty analysis. A registered
    # NN-only consumer may explicitly retain known empty groups after ownership
    # is split; this exception belongs only to the opt-in execution compiler.
    if all(type(rows) is list and not rows for rows in configuration.values()):
        if required:
            raise ValidationError("empty saved analysis cannot provide required outputs")
        return {"moving_averages": [], "momentum": [], **deepcopy(configuration)}
    return validate_configuration(configuration)


def _execution_row_fields(group, row):
    if group in market_catalog.GROUPS:
        return frozenset(market_catalog.definition_fields(group, row))
    single = {"moving_averages": [], "momentum": [], group: [row]}
    definitions = _configured_definitions(single)
    return _provided_fields(*definitions) - {f"CLOSED_{iid}" for iid in definitions[0]}


def _execution_components(group, row):
    if group != "moving_averages":
        return ((None, row),)
    components = []
    if row["ema_period"] is not None:
        components.append(("EMA", row | {"mmacd_periods": []}))
    if row["mmacd_periods"]:
        components.append(("MMACD", row | {"ema_period": None}))
    return tuple(components)


def create_execution_features(instrument, required_fields, configuration, policy):
    """Compile explicit TS dependencies without changing the complete saved CA.

    Runtime splits registered external/NN ownership before calling this pure
    boundary. The returned JSON plan is separate from ordinary graph checkpoints;
    default create_features and independent full-CA evaluation stay unchanged.
    """
    if (
        type(policy) is not dict
        or set(policy) != {"version", "policy"}
        or type(policy["version"]) is not int
        or policy["version"] != 1
        or type(policy["policy"]) is not str
        or policy["policy"] != "required_fields"
    ):
        raise ValidationError("unsupported analysis execution policy")
    if (
        type(instrument) is not Instrument
        or type(required_fields) not in {set, frozenset, tuple, list}
        or len(required_fields) > 256
        or any(type(field) is not str or not 1 <= len(field) <= 128 for field in required_fields)
    ):
        raise ValidationError("execution projection needs bounded native output requirements")
    required = frozenset(required_fields)
    full = _execution_profile(configuration, required)
    try:
        full_json = json.dumps(full, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        raise ValidationError("saved execution profile requires canonical JSON values") from None
    definitions = _configured_definitions(full)
    extra = {name: full[name] for name in market_catalog.GROUPS if name in full}
    intervals = set(definitions[0]) | market_catalog.intervals(extra)
    closures = {f"CLOSED_{iid}": iid for iid in intervals}
    provided = _provided_fields(*definitions) | market_catalog.output_fields(extra)
    if missing := required - provided:
        raise ValidationError(f"saved analysis does not provide required fields: {sorted(missing)}")

    # Exact published-field ownership selects complete configured kernels. MA's
    # independently supported EMA/MMACD components are the only narrower split.
    owners = {}
    for group, rows in full.items():
        for index, row in enumerate(rows):
            for component, definition in _execution_components(group, row):
                for field in _execution_row_fields(group, definition):
                    if field in owners:
                        raise ValidationError("analysis execution output has ambiguous owners")
                    owners[field] = (group, index, component)
    chosen = {}
    selected_intervals = {closures[field] for field in required if field in closures}
    for field in required - closures.keys():
        if field not in owners:
            raise ValidationError("required analysis output has no configured owner")
        group, index, component = owners[field]
        components = chosen.setdefault((group, index), set())
        if component is not None:
            components.add(component)

    selected, identities = {name: [] for name in full}, []
    for group, rows in full.items():
        for index, original in enumerate(rows):
            if (group, index) not in chosen:
                continue
            row = deepcopy(original)
            components = chosen[group, index]
            if group == "moving_averages":
                if "EMA" not in components:
                    row["ema_period"] = None
                if "MMACD" not in components:
                    row["mmacd_periods"] = []
            selected[group].append(row)
            selected_intervals.add(row["interval_id"])
            identities.append(
                {
                    "group": group,
                    "interval_id": row["interval_id"],
                    "analysis_code": row.get("analysis_code"),
                    "line_index": row.get("line_index"),
                    "components": sorted(components),
                }
            )
    identities.sort(
        key=lambda row: (
            row["group"],
            row["interval_id"],
            row["analysis_code"] or "",
            -1 if row["line_index"] is None else row["line_index"],
        )
    )
    if any(selected.values()):
        selected = validate_configuration(selected)
    projected = _configured_definitions(selected)
    if not required:
        graph = EmptyFeatures()
    else:
        graph = _compose(
            instrument,
            tuple(sorted(selected_intervals)),
            *projected[1:],
            extra={name: selected[name] for name in market_catalog.GROUPS if name in selected},
        )
    plan = {
        "version": 1,
        "policy": "required_fields",
        "configuration_sha256": sha256(full_json.encode()).hexdigest(),
        "required_fields": sorted(required),
        "selected_configuration": deepcopy(selected),
        "selected_definitions": identities,
        "closure_intervals": sorted(selected_intervals),
        "raw_inputs": sorted(graph.descriptor.required_fields),
    }
    return graph, plan
