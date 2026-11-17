"""Saved frame-aware and interpreted-profile definitions, independent of IO."""

from dataclasses import asdict, fields

from omega.analysis.graph import FeatureNode
from omega.analysis.market_sentiment import (
    OUTPUTS,
    LegacyMarketSentimentFeatures,
    SentimentConfiguration,
)
from omega.analysis.order_flow import LegacyOrderFlowFeatures, OrderFlowConfiguration
from omega.analysis.price_windows import (
    BollingerConfiguration,
    LegacyPriceWindowFeatures,
    WindowAverageConfiguration,
)
from omega.analysis.short_term_scalar import (
    PUBLIC_FIELDS,
    LegacyShortTermScalarFeatures,
    ScalarConfiguration,
)
from omega.analysis.temporal_probability import (
    PUBLIC_FIELDS as TEMPORAL_FIELDS,
)
from omega.analysis.temporal_probability import (
    LegacyTemporalProbabilityFeatures,
    TemporalProbabilityConfiguration,
)
from omega.analysis.volume_profile import LegacyVolumeProfileFeatures, ProfileConfiguration
from omega.domain.errors import ValidationError

GROUPS = {
    "order_flow": (OrderFlowConfiguration, LegacyOrderFlowFeatures),
    "market_sentiment": (SentimentConfiguration, LegacyMarketSentimentFeatures),
    "short_term_scalar": (ScalarConfiguration, LegacyShortTermScalarFeatures),
    "volume_profile": (ProfileConfiguration, LegacyVolumeProfileFeatures),
    "temporal_probability": (TemporalProbabilityConfiguration, LegacyTemporalProbabilityFeatures),
    "window_averages": (WindowAverageConfiguration, LegacyPriceWindowFeatures),
    "bollinger": (BollingerConfiguration, LegacyPriceWindowFeatures),
}


def canonical_groups(value):
    """Reuse each producer's parameter contract and reject overlapping outputs."""
    result, provider_configurations = {}, {}
    for name, (configuration_type, provider_type) in GROUPS.items():
        if name not in value:
            continue
        rows = value[name]
        if type(rows) is not list or len(rows) > 64:
            raise ValidationError("market analysis definitions must be a bounded list")
        keys = {field.name for field in fields(configuration_type)}
        identities, canonical, configurations = set(), [], []
        for row in rows:
            if type(row) is not dict or set(row) - keys:
                raise ValidationError("unknown market analysis parameter")
            try:
                configuration = configuration_type(**row)
            except (TypeError, ValueError) as error:
                raise ValidationError("invalid market analysis configuration") from error
            encoded = asdict(configuration)
            identity = (
                encoded["interval_id"],
                encoded.get("analysis_code"),
                encoded.get("line_index"),
            )
            if identity in identities:
                raise ValidationError("duplicate market analysis output")
            identities.add(identity)
            canonical.append(encoded)
            configurations.append(configuration)
        if configurations:
            # Groups sharing one provider also share its retained-state budget.
            # Existing single-group producers keep their original validation.
            combined = provider_configurations.setdefault(provider_type, [])
            combined.extend(configurations)
            provider_type.validate_configurations(combined)
        canonical.sort(
            key=lambda row: (
                row["interval_id"],
                row.get("analysis_code", ""),
                row.get("line_index", 0),
            )
        )
        result[name] = canonical
    return result


def intervals(groups):
    return {row["interval_id"] for rows in groups.values() for row in rows}


def definition_fields(name, row):
    iid = row["interval_id"]
    if name == "order_flow":
        code = row["analysis_code"]
        prefix = f"{iid}_{code}_{row['line_index']}_"
        return tuple(prefix + suffix for suffix in (code, code + "ABSMA", code + "ABSMAREL"))
    if name == "market_sentiment":
        return tuple(f"{iid}_MST_{row['line_index']}_{suffix}" for suffix in OUTPUTS)
    if name == "short_term_scalar":
        return tuple(f"{iid}_STS_{suffix}" for suffix in PUBLIC_FIELDS)
    if name == "volume_profile":
        return tuple(f"{iid}_IVP_NB{index}" for index in range(10))
    if name == "temporal_probability":
        return tuple(f"{iid}_TPD_{row['line_index']}_{suffix}" for suffix in TEMPORAL_FIELDS)
    if name == "window_averages":
        code = row["analysis_code"]
        return (f"{iid}_{code}_{row['line_index']}_{code}",)
    if name == "bollinger":
        return tuple(
            f"{iid}_BOL_{row['line_index']}_{suffix}" for suffix in ("BOLLOW", "BOLHIGH", "MA")
        )
    raise ValidationError("unknown market analysis group")


def output_fields(groups):
    return frozenset(
        field
        for name, rows in groups.items()
        for row in rows
        for field in definition_fields(name, row)
    ) | frozenset(f"CLOSED_{iid}" for iid in intervals(groups))


def nodes(instrument, groups):
    result = []
    for name, rows in groups.items():
        if not rows:
            continue
        configuration_type, provider_type = GROUPS[name]
        configurations = tuple(configuration_type(**row) for row in rows)
        provider = provider_type(instrument, configurations)
        # Bars publish closure once; an unavailable auxiliary node must not
        # overwrite those flags or expose a held legacy number as usable input.
        published = tuple(field for row in rows for field in definition_fields(name, row))
        dependencies = (
            ("bars",) if getattr(provider, "uses_shared_aggregation", False) is True else ()
        )
        result.append(FeatureNode(name, provider, dependencies, public_fields=published))
    return tuple(result)
