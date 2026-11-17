"""Account-scoped USD-M bracket reference. This adapter can only issue signed GET."""

# Imports
import re
from decimal import Decimal

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import decimal, is_valid_symbol

SOURCE = "binance_signed_account"


def account_scope(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        raise ValidationError("account scope must be an explicit public account identifier")
    return value


def _number(value):
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise ProtocolError("bracket numbers require exact finite decimal values")
    if len(str(value)) > 128:
        raise ProtocolError("bracket numeric field exceeds capacity")
    try:
        result = decimal(value)
    except ValidationError:
        raise ProtocolError("invalid bracket decimal") from None
    if abs(result.adjusted()) > 64 or result.as_tuple().exponent < -32:
        raise ProtocolError("bracket numeric precision exceeds capacity")
    return result


def _integer(value):
    if type(value) is not int or not 1 <= value <= 10000:
        raise ProtocolError("invalid bracket integer field")
    return value


def normalize_brackets(payload):
    """Keep exact returned bounds; notionalCoef is metadata, never applied a second time."""
    items = [payload] if isinstance(payload, dict) else payload
    if not isinstance(items, list) or not 1 <= len(items) <= 10000:
        raise ProtocolError("invalid or empty leverage bracket response")
    result, nTiers = {}, 0
    for item in items:
        if not isinstance(item, dict) or set(item) - {"symbol", "notionalCoef", "brackets"}:
            raise ProtocolError("unexpected bracket response fields")
        symbol = item.get("symbol")
        if not is_valid_symbol(symbol) or symbol in result:
            raise ProtocolError("invalid or duplicate bracket symbol")
        coefficient = None
        if "notionalCoef" in item:
            coefficient = _number(item["notionalCoef"])
            if coefficient <= 0:
                raise ProtocolError("notional coefficient must be positive")
        rows = item.get("brackets")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 100:
            raise ProtocolError("invalid bracket tier list")
        nTiers += len(rows)
        if nTiers > 20000:
            raise ProtocolError("bracket reference exceeds tier capacity")
        tiers = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {
                "bracket",
                "initialLeverage",
                "notionalFloor",
                "notionalCap",
                "maintMarginRatio",
                "cum",
            }:
                raise ProtocolError("invalid bracket tier schema")
            bracket, leverage = _integer(row["bracket"]), _integer(row["initialLeverage"])
            floor, cap, ratio, cum = (
                _number(row[key])
                for key in (
                    "notionalFloor",
                    "notionalCap",
                    "maintMarginRatio",
                    "cum",
                )
            )
            if floor < 0 or cap <= floor or not 0 <= ratio < 1 or cum < 0:
                raise ProtocolError("invalid bracket bounds or maintenance values")
            tiers.append(
                {
                    "bracket": bracket,
                    "initial_leverage": leverage,
                    "notional_floor": format(floor, "f"),
                    "notional_cap": format(cap, "f"),
                    "maintenance_ratio": format(ratio, "f"),
                    "cumulative_adjustment": format(cum, "f"),
                }
            )
        tiers.sort(key=lambda tier: tier["bracket"])
        previous = Decimal(0)
        for index, tier in enumerate(tiers, 1):
            if tier["bracket"] != index or _number(tier["notional_floor"]) != previous:
                raise ProtocolError(
                    "bracket tiers must be unique, ordered and contiguous from zero"
                )
            previous = _number(tier["notional_cap"])
        result[symbol] = {
            "notional_coefficient": format(coefficient, "f") if coefficient is not None else None,
            "coefficient_applied": False,
            "tiers": tiers,
        }
    return dict(sorted(result.items()))


def validate_envelope(value, account_id):
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "provider", "market", "account_id", "source", "table"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["provider"] != "binance"
        or value["market"] != "usdm"
        or value["account_id"] != account_scope(account_id)
        or value["source"] != SOURCE
        or not isinstance(value["table"], dict)
    ):
        raise ProtocolError("leverage reference schema, source or account scope mismatch")
    payload = []
    for symbol, item in value["table"].items():
        if (
            not isinstance(item, dict)
            or set(item) != {"notional_coefficient", "coefficient_applied", "tiers"}
            or item["coefficient_applied"] is not False
        ):
            raise ProtocolError("invalid normalized coefficient metadata")
        if not isinstance(item["tiers"], list):
            raise ProtocolError("invalid normalized tier list")
        candidate = {"symbol": symbol, "brackets": []}
        if item["notional_coefficient"] is not None:
            candidate["notionalCoef"] = item["notional_coefficient"]
        for tier in item["tiers"]:
            if not isinstance(tier, dict) or set(tier) != {
                "bracket",
                "initial_leverage",
                "notional_floor",
                "notional_cap",
                "maintenance_ratio",
                "cumulative_adjustment",
            }:
                raise ProtocolError("invalid normalized tier schema")
            candidate["brackets"].append(
                {
                    "bracket": tier["bracket"],
                    "initialLeverage": tier["initial_leverage"],
                    "notionalFloor": tier["notional_floor"],
                    "notionalCap": tier["notional_cap"],
                    "maintMarginRatio": tier["maintenance_ratio"],
                    "cum": tier["cumulative_adjustment"],
                }
            )
        payload.append(candidate)
    return {**value, "table": normalize_brackets(payload)}


class BinanceLeverageBrackets:
    def __init__(self, client, account_id):
        if client.base_url != "https://fapi.binance.com":
            raise ValidationError("leverage brackets require the USD-M HTTPS origin")
        self.client, self.account_id = client, account_scope(account_id)

    def fetch(self):
        payload = self.client.request("GET", "/fapi/v1/leverageBracket", signed=True, weight=1)
        return {
            "schema_version": 1,
            "provider": "binance",
            "market": "usdm",
            "account_id": self.account_id,
            "source": SOURCE,
            "table": normalize_brackets(payload),
        }
