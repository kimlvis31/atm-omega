"""Thin terminal commands for the core-owned public-data collection API."""

import argparse
import http.client
import re
import shlex
from uuid import uuid4

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.runtime.launcher import LauncherError

HELP = """Collection commands (public Binance data; no accounts or orders)
  collection status [ID] [--market spot|usdm]
      Read a database snapshot, or one original database action by ID.
  collection start KEY... [--historical] [--lookback-minutes N] [--id ID]
      Start selected streams. History defaults off; lookback defaults to 500.
  collection stop KEY... [--id ID]
      Stop only the selected instruments; other subscriptions remain requested.
  collection set KEY... --stream on|off --historical on|off --lookback-minutes N [--id ID]
      Apply all modes explicitly; no read/modify/write of possibly stale settings.
KEY is canonical, e.g. binance:usdm:BTCUSDT or binance:spot:ETHUSDT.
Select 1..100 keys; history requires streaming; lookback is 1..20000 closed minutes.
A queued/configured worker is not proof that current market data is ready.
For an unconfirmed request, use collection status ID. Never blindly resubmit.
--id reuses an original ID only with its original command parameters.
"""


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError("collection: " + message + "; use collection --help")


def _identity(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", value):
        raise ValueError("collection requires an action ID of 1..96 letters/digits/_/-")
    return value


def _keys(values):
    if not 1 <= len(values) <= 100:
        raise ValueError("collection selects 1..100 canonical instrument keys")
    result = []
    for value in values:
        try:
            provider, market, symbol = value.split(":")
            instrument = Instrument(provider, Market(market), symbol)
        except (ValueError, ValidationError):
            raise ValueError(
                "collection requires canonical Binance spot/usdm instrument keys"
            ) from None
        if provider != "binance" or instrument.market not in {Market.SPOT, Market.USDM}:
            raise ValueError("collection supports public Binance spot/usdm only")
        result.append(instrument.key)
    return sorted(set(result))


def execute_collection(text, *, request, output):
    """Reuse authenticated console transport and the database action journal."""
    tokens = shlex.split(text)
    if not tokens or tokens == ["help"] or "--help" in tokens or "-h" in tokens:
        return HELP
    parser = _Parser(prog="collection", add_help=False)
    commands = parser.add_subparsers(dest="command", required=True)
    status = commands.add_parser("status", add_help=False)
    status.add_argument("identity", nargs="?")
    status.add_argument("--market", choices=("spot", "usdm"))
    for kind in ("start", "stop", "set"):
        command = commands.add_parser(kind, add_help=False)
        command.add_argument("keys", nargs="+")
        command.add_argument("--id", type=_identity)
        if kind == "start":
            command.add_argument("--historical", action="store_true")
            command.add_argument("--lookback-minutes", type=int, default=500)
        elif kind == "set":
            command.add_argument("--stream", choices=("on", "off"), required=True)
            command.add_argument("--historical", choices=("on", "off"), required=True)
            command.add_argument("--lookback-minutes", type=int, required=True)
    args = parser.parse_args(tokens)
    if args.command == "status":
        if args.identity is not None:
            if args.market is not None:
                raise ValueError("collection status ID cannot include --market")
            return request("GET", "/api/database/actions/" + _identity(args.identity))
        return request("GET", "/api/database?market=" + (args.market or "usdm"))
    keys = _keys(args.keys)
    stream = args.command != "stop" and (args.command != "set" or args.stream == "on")
    historical = args.command != "stop" and (
        args.historical is True if args.command == "start" else args.historical == "on"
    )
    lookback = 500 if args.command == "stop" else args.lookback_minutes
    if not 1 <= lookback <= 20_000 or historical and not stream:
        raise ValueError("lookback must be 1..20000; history requires streaming")
    identity = args.id or uuid4().hex
    body = {
        "id": identity,
        "kind": "collection.set",
        "parameters": {
            "instruments": keys,
            "stream": stream,
            "historical": historical,
            "lookback_minutes": lookback,
        },
    }
    output("Collection operation " + identity)
    try:
        return request("POST", "/api/database/actions", body)
    except (OSError, ValueError, LauncherError, http.client.HTTPException):
        output(
            "Collection outcome unconfirmed. Use: collection status "
            + identity
            + ". Do not repost blindly."
        )
        raise
