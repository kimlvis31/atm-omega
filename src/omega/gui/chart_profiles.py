"""Bounded local chart preferences. Applying a draft never writes it to disk."""

# Imports
import copy
import json
import math
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from PySide6.QtCore import QIODevice, QSaveFile

from omega.domain.chart_analysis import ANALYSIS_FIELDS, ANALYSIS_KINDS, analysis_parameters
from omega.domain.errors import ValidationError
from omega.domain.models import is_valid_symbol

# Constants
AUXILIARY_INDICATORS = ("OPENINTEREST", "LONGSHORTRATIO", "AGGTRADE", "DEPTH", "FUNDING")
AUXILIARY_FIELDS = {
    "OPENINTEREST": ("METRIC_OPENINTEREST", "METRIC_OPENINTERESTVALUE"),
    "LONGSHORTRATIO": ("METRIC_LONGSHORTRATIO", "METRIC_LONGACCOUNT", "METRIC_SHORTACCOUNT"),
    "AGGTRADE": tuple(
        "AGGTRADE_" + mode + side
        for mode in ("NOTIONAL", "QUANTITY", "NTRADES")
        for side in ("BUY", "SELL")
    ),
    "DEPTH": ("DEPTH_BIDS", "DEPTH_ASKS"),
    "FUNDING": ("FUNDING_RATE",),
}
LEGACY_DERIVED_INDICATORS = ("NES", "TPD")
LEGACY_INDICATORS = (
    "VOL",
    "MFI",
    "MMACD",
    "DMIxADX",
    *AUXILIARY_INDICATORS,
    *LEGACY_DERIVED_INDICATORS,
)
DERIVED_INDICATORS = (*LEGACY_DERIVED_INDICATORS, *ANALYSIS_KINDS)
INDICATORS = (*LEGACY_INDICATORS, *ANALYSIS_KINDS)
SI_SLOTS = 3
MAX_WORKSPACE_BYTES = 128 * 1024


def default_price_windows() -> dict:
    colors = (
        "#e6b86a",
        "#6cb8d1",
        "#c08bd8",
        "#88bc80",
        "#dd8993",
        "#869ce0",
        "#d2c278",
        "#72bca9",
        "#cf9b76",
        "#aaa6de",
    )
    result = {"origin_ms": None, "price_precision": 8}
    for code in ("sma", "wma", "bol"):
        result[code] = {
            "enabled": False,
            "lines": [
                {
                    "active": False,
                    "visible": True,
                    "nSamples": 10 * (index + 1),
                    "color": colors[index],
                    "opacity": 64 if code == "bol" else 255,
                    "stroke_width": 1.0,
                    **({"bandWidth": 2} if code == "bol" else {}),
                }
                for index in range(10)
            ],
            **({"MAType": "SMA", "show_center": True, "show_band": True} if code == "bol" else {}),
        }
    return result


def validate_price_windows(value: dict) -> dict:
    expected = default_price_windows()
    if type(value) is not dict or set(value) != set(expected):
        raise ValueError("invalid price-window settings")
    origin = value["origin_ms"]
    if (
        origin is not None
        and (type(origin) is not int or not 0 <= origin <= 253402300799000 or origin % 60000)
        or type(value["price_precision"]) is not int
        or not 0 <= value["price_precision"] <= 16
    ):
        raise ValueError("price-window origin must be a UTC minute; precision must be0..16")
    state = 0
    for code in ("sma", "wma", "bol"):
        family = value[code]
        if (
            type(family) is not dict
            or set(family) != set(expected[code])
            or type(family["enabled"]) is not bool
            or type(family["lines"]) is not list
            or len(family["lines"]) != 10
        ):
            raise ValueError("price-window families require a master and exactly ten lines")
        if code == "bol" and (
            type(family["MAType"]) is not str
            or family["MAType"] not in {"SMA", "WMA", "EMA"}
            or any(type(family[key]) is not bool for key in ("show_center", "show_band"))
        ):
            raise ValueError("invalid BOL center type or display switches")
        for line in family["lines"]:
            if (
                type(line) is not dict
                or set(line) != set(expected[code]["lines"][0])
                or any(type(line[key]) is not bool for key in ("active", "visible"))
                or type(line["nSamples"]) is not int
                or not 2 <= line["nSamples"] <= 20000
                or type(line["color"]) is not str
                or re.fullmatch(r"#[a-fA-F0-9]{6}", line["color"]) is None
                or type(line["opacity"]) is not int
                or not 0 <= line["opacity"] <= 255
                or type(line["stroke_width"]) not in {int, float}
                or not 0.25 <= line["stroke_width"] <= 10
            ):
                raise ValueError("invalid indexed price-window line")
            if code == "bol":
                width = line["bandWidth"]
                try:
                    finite = type(width) in {int, float} and math.isfinite(width)
                except OverflowError:
                    finite = False
                if not finite:
                    raise ValueError("BOL bandWidth must be a finite JSON number")
            if family["enabled"] and line["active"]:
                state += line["nSamples"] + 3
    if state > 100000:
        raise ValueError("price-window state exceeds100000samples")
    return copy.deepcopy(value)


def default_profile() -> dict:
    return {
        "schema_version": 6,
        "price_windows": default_price_windows(),
        "timezone": "UTC",
        "ema": False,
        "ema_period": 20,
        "mfi_period": 14,
        "mmacd_periods": [10, 20, 30, 40, 50, 60],
        "signal_period": 9,
        "dmi_period": 14,
        "dmi_display": "DMIxADX",
        "nes": {"nSamples": 14, "quote_precision": 8, "origin_ms": None, "display": "NES"},
        "tpd": {
            "viewLength": 10,
            "nSamples": 100,
            "nSamplesMA": 20,
            "origin_ms": None,
            "display": "TPD",
        },
        "psar": {
            "enabled": False,
            "start": 0.02,
            "acceleration": 0.005,
            "maximum": 0.2,
            "price_precision": 8,
            "color": "#b5ce62",
            "width": 1.0,
        },
        "swing": {"enabled": False, "swing_range": 0.005, "color": "#c89f58", "width": 1.5},
        "ivp": {
            "enabled": False,
            "visible": True,
            "nSamples": 500,
            "gammaFactor": 0.01,
            "deltaFactor": 1.0,
            "prominence": 0.1,
            "distance": 5,
            "height": 0.5,
            "price_precision": 8,
            "max_divisions": 2048,
            "origin_ms": None,
            "color": "#b5ce62",
            "width": 0.25,
        },
        "enabled": {code: code in {"VOL", "MFI", "MMACD"} for code in LEGACY_INDICATORS},
        "auxiliary": {
            code: {
                "source": "",
                "fields": list(
                    AUXILIARY_FIELDS[code][:2]
                    if code in {"AGGTRADE", "DEPTH"}
                    else AUXILIARY_FIELDS[code][:1]
                ),
            }
            for code in AUXILIARY_INDICATORS
        },
        "slots": [{"indicator": code, "visible": code == "VOL"} for code in INDICATORS[:SI_SLOTS]],
    }


def default_analysis_profile(kind: str) -> dict:
    """Effective defaults never insert new settings into an old saved profile."""
    if kind not in ANALYSIS_KINDS:
        raise ValueError("unsupported analysis SI")
    if kind == "STS":
        return {
            "origin_ms": None,
            "display": "S",
            "visible": True,
            "MMACDFast": 10,
            "MMACDSlow": 20,
            "MMACDSignal": 9,
            "DMIxADXNSamples": 20,
            "MFINSamples": 20,
            "WOINSamples": 5,
            "WOIBaseNSamples": 240,
            "OSCScale": 1.0,
            "WMMACD": 1.0,
            "WMFI": 1.0,
            "WDMIxADX": 0.5,
            "WWOI": 0.5,
            "price_precision": 8,
            "quote_precision": 8,
            "positive_color": "#50c896",
            "negative_color": "#f05050",
            "opacity": 255,
        }
    colors = default_price_windows()["sma"]["lines"]
    return {
        "origin_ms": None,
        "display": kind,
        "quote_precision": 8,
        **({"maximum_metric_age_ms": 330000} if kind == "MST" else {}),
        "lines": [
            {
                "active": False,
                "visible": True,
                "nSamples": 10 * (index + 1),
                "color": colors[index]["color"],
                "opacity": 255,
                "width": 1.0,
            }
            for index in range(10)
        ],
    }


def analysis_profile(profile: dict, kind: str) -> dict:
    return copy.deepcopy(profile.get(kind.lower(), default_analysis_profile(kind)))


def indicator_enabled(profile: dict, kind: str) -> bool:
    return profile["enabled"].get(kind, False)


def promote_analysis_profile(profile: dict) -> dict:
    result = copy.deepcopy(profile)
    result["schema_version"] = 7
    for kind in ANALYSIS_KINDS:
        result.setdefault(kind.lower(), default_analysis_profile(kind))
        result["enabled"].setdefault(kind, False)
    return result


def analysis_options(kind: str, config: dict) -> dict:
    if kind == "STS":
        return {
            key: value
            for key, value in config.items()
            if key
            not in {
                "origin_ms",
                "display",
                "visible",
                "positive_color",
                "negative_color",
                "opacity",
            }
        }
    return {
        "definitions": [
            {"line_index": index, "nSamples": line["nSamples"]}
            for index, line in enumerate(config["lines"])
            if line["active"]
        ],
        "quote_precision": config["quote_precision"],
        **({"maximum_metric_age_ms": config["maximum_metric_age_ms"]} if kind == "MST" else {}),
    }


def _validate_analysis_profile(kind: str, config: dict) -> None:
    expected = default_analysis_profile(kind)
    if type(config) is not dict or set(config) != set(expected):
        raise ValueError("invalid analysis SI settings")
    origin = config["origin_ms"]
    if (
        type(config["display"]) is not str
        or config["display"] not in ANALYSIS_FIELDS[kind]
        or origin is not None
        and (type(origin) is not int or not 0 <= origin <= 253402300799000 or origin % 60000)
    ):
        raise ValueError("invalid analysis SI field or UTC minute origin")

    def style(line):
        for key in ("color",) if kind != "STS" else ("positive_color", "negative_color"):
            if type(line[key]) is not str or re.fullmatch(r"#[a-fA-F0-9]{6}", line[key]) is None:
                raise ValueError("invalid analysis SI color")
        if type(line["opacity"]) is not int or not 0 <= line["opacity"] <= 255:
            raise ValueError("invalid analysis SI opacity")
        if kind != "STS" and (
            type(line["width"]) not in {int, float}
            or not 0.25 <= line["width"] <= 10
            or not math.isfinite(line["width"])
        ):
            raise ValueError("invalid analysis SI line width")

    # This pure DTO check validates configuration only; it never reads market data.
    common = {
        "instrument": "binance:usdm:BTCUSDT",
        "kind": kind,
        "start_ms": 0,
        "end_ms": 60000,
        "period_minutes": 1,
    }
    try:
        if kind == "STS":
            if type(config["visible"]) is not bool:
                raise ValueError("invalid STS display switch")
            style(config)
            analysis_parameters(kind, common | analysis_options(kind, config))
        else:
            if type(config["lines"]) is not list or len(config["lines"]) != 10:
                raise ValueError("analysis SI requires exactly ten indexed lines")
            for index, line in enumerate(config["lines"]):
                if (
                    type(line) is not dict
                    or set(line) != set(expected["lines"][index])
                    or any(type(line[key]) is not bool for key in ("active", "visible"))
                ):
                    raise ValueError("invalid indexed analysis SI line")
                style(line)
            options = analysis_options(kind, config)
            for index, line in enumerate(config["lines"]):
                analysis_parameters(
                    kind,
                    common
                    | options
                    | {"definitions": [{"line_index": index, "nSamples": line["nSamples"]}]},
                )
            if options["definitions"]:
                analysis_parameters(kind, common | options)
    except ValidationError as error:
        raise ValueError(str(error)) from None


def chart_timezone(value: str):
    if value == "LOCAL":
        return None  # datetime.astimezone() resolves the OS offset for this instant.
    if value == "UTC":
        return UTC
    match = re.fullmatch(r"UTC([+-])(\d{2}):(00|15|30|45)", str(value))
    if not match:
        raise ValueError("timezone must be LOCAL, UTC or UTC+/-HH:MM")
    minutes = int(match[2]) * 60 + int(match[3])
    if minutes > (14 * 60 if match[1] == "+" else 12 * 60):
        raise ValueError("chart timezone is outside UTC-12:00 .. UTC+14:00")
    return timezone(timedelta(minutes=minutes if match[1] == "+" else -minutes), value)


def display_date(timestamp_ms: int, zone: str = "UTC") -> datetime:
    instant = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
    try:
        return instant.astimezone(chart_timezone(zone))
    except (OverflowError, ValueError):
        # A display offset cannot represent year 10000. Preserve a valid UTC date.
        return instant


def validate_profile(value: dict) -> dict:
    if not isinstance(value, dict):
        raise ValueError("chart profile must be an object")
    version = value.get("schema_version", 1)
    if type(version) is not int or version not in {1, 2, 3, 4, 5, 6, 7}:
        raise ValueError("unsupported chart profile version")
    result = promote_analysis_profile(default_profile()) if version == 7 else default_profile()
    if set(value) - set(result):
        raise ValueError("unknown chart profile setting")
    if version == 7 and set(value) != set(result):
        raise ValueError("version7 requires complete chart settings")
    result.update(copy.deepcopy(value))
    # v1 had exactly three calculation flags and no DMI options. Migration is
    # in-memory only; opening a workspace never automatically rewrites its file.
    if version < 5 and isinstance(result["enabled"], dict):
        if set(result["enabled"]) in (
            set(LEGACY_INDICATORS[:SI_SLOTS]),
            set(LEGACY_INDICATORS[:4]),
            set(LEGACY_INDICATORS) - set(LEGACY_DERIVED_INDICATORS),
        ):
            result["enabled"] = {code: False for code in LEGACY_INDICATORS} | result["enabled"]
    result["schema_version"] = 7 if version == 7 else 6
    result["price_windows"] = validate_price_windows(result["price_windows"])
    from omega.analysis.order_flow import OrderFlowConfiguration
    from omega.analysis.temporal_probability import TemporalProbabilityConfiguration
    from omega.domain.errors import ValidationError

    for code in LEGACY_DERIVED_INDICATORS:
        options = result[code.lower()]
        if (
            type(options) is not dict
            or set(options) != set(default_profile()[code.lower()])
            or type(options["display"]) is not str
            or options["display"] not in {code, "ABSMA", "ABSMAREL"}
            or options["origin_ms"] is not None
            and (
                type(options["origin_ms"]) is not int
                or not 0 <= options["origin_ms"] <= 253402300799000
                or options["origin_ms"] % 60000
            )
        ):
            raise ValueError("invalid derived SI display/origin")
        numeric = {
            key: value for key, value in options.items() if key not in {"display", "origin_ms"}
        }
        try:
            if code == "NES":
                OrderFlowConfiguration(0, "NES", **numeric)
            else:
                TemporalProbabilityConfiguration(0, **numeric)
        except ValidationError as error:
            raise ValueError(str(error)) from None
    ivp = result["ivp"]
    if (
        type(ivp) is not dict
        or set(ivp) != set(default_profile()["ivp"])
        or any(type(ivp[key]) is not bool for key in ("enabled", "visible"))
        or type(ivp["color"]) is not str
        or re.fullmatch(r"#[a-fA-F0-9]{6}", ivp["color"]) is None
        or type(ivp["width"]) not in {int, float}
        or not 0.05 <= ivp["width"] <= 0.5
        or ivp["origin_ms"] is not None
        and (
            type(ivp["origin_ms"]) is not int
            or not 0 <= ivp["origin_ms"] <= 253402300799000
            or ivp["origin_ms"] % 60000
        )
    ):
        raise ValueError("invalid selected IVP display settings")
    from omega.analysis.volume_profile import ProfileConfiguration

    try:
        ProfileConfiguration(
            0,
            **{
                key: ivp[key]
                for key in (
                    "nSamples",
                    "gammaFactor",
                    "deltaFactor",
                    "prominence",
                    "distance",
                    "height",
                    "price_precision",
                    "max_divisions",
                )
            },
        )
    except ValidationError as error:
        raise ValueError(str(error)) from None
    except OverflowError:
        raise ValueError("IVP settings exceed finite display arithmetic") from None
    if ivp["max_divisions"] > 2048:
        raise ValueError("IVP display supports at most2048divisions")
    auxiliary = result["auxiliary"]
    if type(auxiliary) is not dict or set(auxiliary) != set(AUXILIARY_INDICATORS):
        raise ValueError("each auxiliary indicator needs a source and selected fields")
    for code, options in auxiliary.items():
        if (
            type(options) is not dict
            or set(options) != {"source", "fields"}
            or type(options["source"]) is not str
            or len(options["source"]) > 96
            or options["source"]
            and not options["source"].isprintable()
            or type(options["fields"]) is not list
            or not 1 <= len(options["fields"]) <= 3
            or any(
                type(name) is not str or name not in AUXILIARY_FIELDS[code]
                for name in options["fields"]
            )
            or len(set(options["fields"])) != len(options["fields"])
        ):
            raise ValueError("invalid auxiliary source or metric selection")
        # Different physical units must never silently share one vertical axis.
        fields = options["fields"]
        if code == "OPENINTEREST" and len(fields) != 1:
            raise ValueError("open-interest quantity and quote value need separate axes")
        if code == "LONGSHORTRATIO" and "METRIC_LONGSHORTRATIO" in fields and len(fields) != 1:
            raise ValueError("ratio and account fractions need separate axes")
        if (
            code == "AGGTRADE"
            and len(
                {
                    name.removeprefix("AGGTRADE_").removesuffix("BUY").removesuffix("SELL")
                    for name in fields
                }
            )
            != 1
        ):
            raise ValueError("trade-flow quantity, notional and counts need separate axes")
    for group in ("psar", "swing"):
        overlay = result[group]
        if (
            type(overlay) is not dict
            or set(overlay) != set(default_profile()[group])
            or type(overlay["enabled"]) is not bool
            or type(overlay["color"]) is not str
            or re.fullmatch(r"#[a-fA-F0-9]{6}", overlay["color"]) is None
        ):
            raise ValueError(f"invalid {group} overlay configuration")
        numeric = (
            ("start", "acceleration", "maximum", "width")
            if group == "psar"
            else ("swing_range", "width")
        )
        for key in numeric:
            number = overlay[key]
            if type(number) not in {int, float} or number <= 0:
                raise ValueError(f"{group}.{key} must be finite and positive")
            try:
                finite = math.isfinite(float(number))
            except OverflowError:
                finite = False
            if not finite:
                raise ValueError(f"{group}.{key} exceeds the finite display range")
        if not 0.25 <= overlay["width"] <= 10:
            raise ValueError("overlay width must be 0.25..10")
        if group == "psar":
            if (
                not overlay["start"] < overlay["maximum"]
                or type(overlay["price_precision"]) is not int
                or not 0 <= overlay["price_precision"] <= 16
            ):
                raise ValueError("PSAR start must be below maximum; precision must be 0..16")
        elif overlay["swing_range"] < 0.0001:
            raise ValueError("SWING range must be at least 0.0001")
    chart_timezone(result["timezone"])
    if type(result["ema"]) is not bool:
        raise ValueError("EMA enabled must be boolean")
    for key in ("ema_period", "mfi_period", "signal_period", "dmi_period"):
        if type(result[key]) is not int or not 2 <= result[key] <= 2000:
            raise ValueError(f"{key} must be an integer between 2 and 2000")
    periods = result["mmacd_periods"]
    if (
        not isinstance(periods, list)
        or not 2 <= len(periods) <= 10
        or any(type(period) is not int or not 2 <= period <= 2000 for period in periods)
        or len(set(periods)) != len(periods)
    ):
        raise ValueError("MMACD requires 2..10 distinct EMA periods between 2 and 2000")
    result["mmacd_periods"] = sorted(periods)
    if type(result["dmi_display"]) is not str or result["dmi_display"] not in {
        "DMIxADX",
        "ABSMA",
        "ABSMAREL",
    }:
        raise ValueError("invalid DMI display field")
    enabled = result["enabled"]
    if (
        not isinstance(enabled, dict)
        or set(enabled) != set(INDICATORS if version == 7 else LEGACY_INDICATORS)
        or any(type(flag) is not bool for flag in enabled.values())
    ):
        raise ValueError("each indicator requires a boolean calculation switch")
    slots = result["slots"]
    if (
        not isinstance(slots, list)
        or len(slots) != SI_SLOTS
        or any(
            not isinstance(slot, dict)
            or set(slot) != {"indicator", "visible"}
            or slot["indicator"] not in (INDICATORS if version == 7 else LEGACY_INDICATORS)
            or type(slot["visible"]) is not bool
            for slot in slots
        )
        or len({slot["indicator"] for slot in slots}) != SI_SLOTS
    ):
        raise ValueError("three SI slots must have unique indicator allocations")
    if version == 7:
        for kind in ANALYSIS_KINDS:
            _validate_analysis_profile(kind, result[kind.lower()])
    return result


def swap_slot(slots: list[dict], index: int, indicator: str) -> list[dict]:
    result = copy.deepcopy(slots)
    if indicator not in INDICATORS or type(index) is not int or not 0 <= index < len(result):
        raise ValueError("invalid indicator slot")
    other = next((i for i, slot in enumerate(result) if slot["indicator"] == indicator), None)
    if other is None:
        result[index]["indicator"] = indicator
        return result
    result[index]["indicator"], result[other]["indicator"] = (
        result[other]["indicator"],
        result[index]["indicator"],
    )
    return result


def profile_name(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 64:
        raise ValueError("profile name must contain 1..64 characters")
    if any(ord(character) < 32 for character in value):
        raise ValueError("profile name cannot contain control characters")
    return value.strip()


def validate_workspace(value: dict) -> dict:
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ValueError("unsupported chart workspace version")
    value = copy.deepcopy(value)
    profiles = value.get("profiles", {})
    if not isinstance(profiles, dict) or len(profiles) > 20:
        raise ValueError("at most 20 chart profiles can be saved")
    profiles = {profile_name(name): validate_profile(profile) for name, profile in profiles.items()}
    if len(profiles) != len(value.get("profiles", {})):
        raise ValueError("profile names must be unique after trimming")
    panes = value.get("panes", [])
    if not isinstance(panes, list) or not 1 <= len(panes) <= 6:
        raise ValueError("workspace requires 1..6 chart panes")
    for pane in panes:
        instrument = pane.get("instrument") if isinstance(pane, dict) else None
        parts = instrument.split(":") if type(instrument) is str else ()
        if (
            len(parts) != 3
            or parts[0] != "binance"
            or parts[1] not in {"spot", "usdm"}
            or not is_valid_symbol(parts[2])
        ):
            raise ValueError("invalid chart instrument")
        if type(pane.get("timeframe")) not in {str, int} or pane.get("timeframe") not in {
            1,
            3,
            5,
            15,
            30,
            60,
            120,
            240,
            360,
            480,
            720,
            1440,
            4320,
            "1w",
            "1M",
        }:
            raise ValueError("invalid chart timeframe")
        pane["profile"] = validate_profile(pane.get("profile", {}))
    mode = value.get("layout", "grid")
    if (
        not isinstance(mode, str)
        or mode not in {"grid", "rows", "columns"}
        or type(value.get("linked", True)) is not bool
    ):
        raise ValueError("invalid chart layout")
    return {
        "schema_version": 1,
        "profiles": profiles,
        "panes": copy.deepcopy(panes),
        "layout": mode,
        "linked": value.get("linked", True),
    }


class ChartWorkspaceStore:
    def __init__(self, root: Path):
        self.path = root / "state" / "chart.workspace.json"

    def load(self) -> dict | None:
        if not self.path.exists():
            return None
        if self.path.stat().st_size > MAX_WORKSPACE_BYTES:
            raise ValueError("saved chart workspace is too large")
        return validate_workspace(json.loads(self.path.read_text(encoding="utf-8")))

    def save(self, value: dict) -> None:
        encoded = json.dumps(validate_workspace(copy.deepcopy(value)), ensure_ascii=False).encode(
            "utf-8"
        )
        if len(encoded) > MAX_WORKSPACE_BYTES:
            raise ValueError("chart workspace is too large")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        output = QSaveFile(str(self.path))
        output.setDirectWriteFallback(False)
        if not output.open(QIODevice.OpenModeFlag.WriteOnly):
            raise OSError(output.errorString())
        if output.write(encoded) != len(encoded):
            output.cancelWriting()
            raise OSError(output.errorString())
        if not output.commit():
            raise OSError(output.errorString())
