"""Optional exact Polars fast path for plain Binance Vision aggTrade CSV days."""

from __future__ import annotations

import csv
import io
import re
from decimal import Decimal

from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.vision import (
    DAY,
    MAX_DAILY_ROWS,
    MAX_LINE_CHARS,
    SPOT_VISION_US_START_MS,
    TRADE_BATCH_CSV_BYTES,
)

MINUTE_MS = 60_000
MAX_EVENTS_PER_MINUTE = 150_000
MAX_INT128 = (1 << 127) - 1
_HEADER_NAMES = {"aggregatetradeid", "aggtradeid"}


def _decimal_from_coefficient(coefficient: int, scale: int) -> Decimal:
    if coefficient == 0:
        return Decimal(0)
    digits = tuple(ord(char) - 48 for char in str(coefficient))
    return Decimal((0, digits, -scale))


def aggregate_trade_csv(
    data: bytes, instrument, day_start: int, start: int, end: int, *, check_stop=None
) -> list[tuple[int, tuple, bool]] | None:
    """Aggregate a plain 7/8-column file, or return None for reference replay.

    The returned value tuple matches ``trade_flow_values`` field order and keeps
    each side's original maximum decimal scale, including trailing zeroes.
    """
    if check_stop is not None:
        check_stop()
    try:
        import polars as pl
    except ImportError:
        return None

    try:
        if (
            type(data) is not bytes
            or len(data) > TRADE_BATCH_CSV_BYTES
            or not isinstance(instrument, Instrument)
            or instrument.provider != "binance"
            or instrument.market not in {Market.SPOT, Market.USDM}
            or type(day_start) is not int
            or day_start < 0
            or day_start % DAY
            or type(start) is not int
            or type(end) is not int
            or start % MINUTE_MS
            or end % MINUTE_MS
            or not day_start <= start < end <= day_start + DAY
        ):
            return None
        if not data:
            return None

        # Keep only the ordinary unquoted CSV fast path. The archive parser remains
        # authoritative for quoted, blank-line, or unusual line-ending variants.
        if b'"' in data or re.search(rb"\r(?!\n)", data) or re.search(rb"(?:\A|\n)\r?\n", data):
            return None
        if data.count(b"\n") > MAX_DAILY_ROWS + 1:
            return None
        payload = data.removeprefix(b"\xef\xbb\xbf")
        line_end = payload.find(b"\n")
        first_line = (payload if line_end < 0 else payload[:line_end]).removesuffix(b"\r")
        if not first_line or len(first_line) > MAX_LINE_CHARS:
            return None
        try:
            first_fields = next(csv.reader([first_line.decode("ascii")], strict=True))
        except (UnicodeError, csv.Error, StopIteration):
            return None
        field_count = len(first_fields)
        allowed = {8} if instrument.market == Market.SPOT else {7, 8}
        if field_count not in allowed or len(first_line) + 2 > MAX_LINE_CHARS:
            return None
        if check_stop is not None:
            check_stop()

        header = re.sub("[^a-z]", "", first_fields[0].lower()) in _HEADER_NAMES
        row_names = [
            "agg_id",
            "price",
            "quantity",
            "first_id",
            "last_id",
            "timestamp",
            "maker",
        ]
        if field_count == 8:
            row_names.append("extra")
        frame = pl.read_csv(
            io.BytesIO(payload),
            has_header=False,
            new_columns=row_names,
            skip_rows=1 if header else 0,
            infer_schema=False,
            ignore_errors=False,
            truncate_ragged_lines=False,
            raise_if_empty=False,
        )
        if check_stop is not None:
            check_stop()
        if frame.width != field_count or frame.height + int(header) > MAX_DAILY_ROWS:
            return None
        if frame.height == 0:
            return []

        # Validate every row in the descriptor, before applying the requested range.
        # This mirrors VisionArchive's full-file ordering and timestamp checks.
        max_line = pl.sum_horizontal(
            [pl.col(name).str.len_chars() for name in row_names]
        ) + (field_count - 1) + 2
        price_lexeme = pl.col("price").str.contains(r"^\d+(?:\.\d+)?$")
        quantity_lexeme = pl.col("quantity").str.contains(r"^\d+(?:\.\d+)?$")
        id_lexeme = {
            name: pl.col(name).str.contains(r"^\d+$")
            for name in ("agg_id", "first_id", "last_id", "timestamp")
        }
        maker_lexeme = pl.col("maker").str.to_lowercase().is_in(
            ["true", "false", "1", "0"]
        )
        checks = [
            *[pl.col(name).is_null().any() for name in row_names],
            (max_line > MAX_LINE_CHARS).any(),
            (~price_lexeme).any(),
            (~quantity_lexeme).any(),
            (~maker_lexeme).any(),
            *[(~valid).any() for valid in id_lexeme.values()],
        ]
        if frame.select(pl.any_horizontal(checks).alias("_invalid")).item():
            return None
        if check_stop is not None:
            check_stop()

        if field_count == 8 and instrument.market == Market.SPOT:
            placeholder = (
                (pl.col("price") == "0")
                & (pl.col("quantity") == "0")
                & (pl.col("first_id") == "-1")
                & (pl.col("last_id") == "-1")
            )
            # Placeholder and duplicate records go through the reference semantics.
            if frame.select(placeholder.any()).item():
                return None

        price_scale_expr = (
            pl.col("price")
            .str.split_exact(".", 1)
            .struct.field("field_1")
            .fill_null("")
            .str.len_chars()
        )
        quantity_scale_expr = (
            pl.col("quantity")
            .str.split_exact(".", 1)
            .struct.field("field_1")
            .fill_null("")
            .str.len_chars()
        )
        scales = frame.select(
            price_scale_expr.max().alias("price_scale"),
            quantity_scale_expr.max().alias("quantity_scale"),
        ).row(0)
        price_scale, quantity_scale = scales
        if (
            price_scale is None
            or quantity_scale is None
            or price_scale > 37
            or quantity_scale > 37
        ):
            return None

        timestamp_raw = pl.col("timestamp").cast(pl.Int64, strict=True)
        timestamp_ms = (
            timestamp_raw // 1000
            if instrument.market == Market.SPOT and day_start >= SPOT_VISION_US_START_MS
            else timestamp_raw
        )
        frame = frame.with_columns(
            pl.col("agg_id").cast(pl.Int64, strict=True).alias("_agg"),
            pl.col("first_id").cast(pl.Int64, strict=True).alias("_first"),
            pl.col("last_id").cast(pl.Int64, strict=True).alias("_last"),
            timestamp_ms.alias("_time"),
            pl.col("maker").str.to_lowercase().is_in(["true", "1"]).alias("_maker"),
            price_scale_expr.alias("_price_scale"),
            quantity_scale_expr.alias("_quantity_scale"),
            pl.col("price")
            .cast(pl.Decimal(precision=38, scale=price_scale), strict=True)
            .alias("_price_decimal"),
            pl.col("quantity")
            .cast(pl.Decimal(precision=38, scale=quantity_scale), strict=True)
            .alias("_quantity_decimal"),
        ).with_columns(
            pl.col("_price_decimal").to_physical().cast(pl.Int128).alias("_price"),
            pl.col("_quantity_decimal").to_physical().cast(pl.Int128).alias("_quantity"),
        )
        if frame.select(
            pl.any_horizontal(
                pl.col("_price").is_null(),
                pl.col("_quantity").is_null(),
                (pl.col("_price") <= 0),
                (pl.col("_quantity") <= 0),
                (pl.col("_first") > pl.col("_last")),
                (pl.col("_time") < day_start),
                (pl.col("_time") >= day_start + DAY),
            ).any()
        ).item():
            return None

        if frame.select(
            ((pl.col("_agg").diff() <= 0).fill_null(False)).any()
            | ((pl.col("_time").diff() < 0).fill_null(False)).any()
        ).item():
            return None

        scoped = frame.filter((pl.col("_time") >= start) & (pl.col("_time") < end))
        if check_stop is not None:
            check_stop()
        if scoped.height == 0:
            return []
        if check_stop is not None:
            check_stop()
        per_minute = scoped.group_by(
            (pl.col("_time") // MINUTE_MS * MINUTE_MS).alias("_opening")
        ).len()
        if per_minute.select((pl.col("len").max() > MAX_EVENTS_PER_MINUTE).any()).item():
            return None

        max_price = int(scoped.select(pl.col("_price").max()).item())
        max_quantity = int(scoped.select(pl.col("_quantity").max()).item())
        max_events = int(per_minute.select(pl.col("len").max()).item())
        if (
            max_quantity * max_events > MAX_INT128
            or max_price * max_quantity * max_events > MAX_INT128
        ):
            return None

        scoped = scoped.with_columns(
            (
                pl.col("_last").cast(pl.Int128)
                - pl.col("_first").cast(pl.Int128)
                + 1
            ).alias("_trade_count"),
            (pl.col("_price") * pl.col("_quantity")).alias("_notional"),
            (pl.col("_price_scale") + pl.col("_quantity_scale")).alias("_notional_scale"),
        )
        side_expressions = []
        for side, maker in (("buy", False), ("sell", True)):
            selected = pl.col("_maker") == maker
            side_expressions.extend(
                [
                    pl.when(selected)
                    .then(pl.col("_quantity"))
                    .otherwise(pl.lit(0, dtype=pl.Int128))
                    .sum()
                    .alias(f"_{side}_quantity"),
                    pl.when(selected)
                    .then(pl.col("_notional"))
                    .otherwise(pl.lit(0, dtype=pl.Int128))
                    .sum()
                    .alias(f"_{side}_notional"),
                    pl.when(selected)
                    .then(pl.col("_trade_count"))
                    .otherwise(pl.lit(0, dtype=pl.Int128))
                    .sum()
                    .alias(f"_{side}_trades"),
                    pl.when(selected)
                    .then(pl.col("_quantity_scale"))
                    .otherwise(None)
                    .max()
                    .fill_null(0)
                    .alias(f"_{side}_quantity_scale"),
                    pl.when(selected)
                    .then(pl.col("_notional_scale"))
                    .otherwise(None)
                    .max()
                    .fill_null(0)
                    .alias(f"_{side}_notional_scale"),
                ]
            )
        if check_stop is not None:
            check_stop()
        grouped = scoped.group_by(
            (pl.col("_time") // MINUTE_MS * MINUTE_MS).alias("_opening"),
            maintain_order=True,
        ).agg(
            *side_expressions,
            pl.len().alias("_event_count"),
            (pl.col("_agg").diff().fill_null(1) == 1).all().alias("_consecutive"),
        )
        if check_stop is not None:
            check_stop()
        rows = []
        for row in grouped.iter_rows(named=True):
            if check_stop is not None:
                check_stop()
            buy_quantity_scale = int(row["_buy_quantity_scale"])
            sell_quantity_scale = int(row["_sell_quantity_scale"])
            buy_notional_scale = int(row["_buy_notional_scale"])
            sell_notional_scale = int(row["_sell_notional_scale"])
            buy_quantity_coefficient = int(row["_buy_quantity"])
            sell_quantity_coefficient = int(row["_sell_quantity"])
            buy_notional_coefficient = int(row["_buy_notional"])
            sell_notional_coefficient = int(row["_sell_notional"])
            product_scale = price_scale + quantity_scale

            def exact_value(coefficient, total_scale, output_scale):
                shift = total_scale - output_scale
                divisor = 10**shift
                if coefficient % divisor:
                    raise ArithmeticError("fixed point rescale was not exact")
                return _decimal_from_coefficient(coefficient // divisor, output_scale)

            values = (
                (
                    "AGGTRADE_QUANTITYBUY",
                    exact_value(buy_quantity_coefficient, quantity_scale, buy_quantity_scale),
                ),
                (
                    "AGGTRADE_QUANTITYSELL",
                    exact_value(sell_quantity_coefficient, quantity_scale, sell_quantity_scale),
                ),
                (
                    "AGGTRADE_NOTIONALBUY",
                    exact_value(buy_notional_coefficient, product_scale, buy_notional_scale),
                ),
                (
                    "AGGTRADE_NOTIONALSELL",
                    exact_value(sell_notional_coefficient, product_scale, sell_notional_scale),
                ),
                ("AGGTRADE_NTRADESBUY", int(row["_buy_trades"])),
                ("AGGTRADE_NTRADESSELL", int(row["_sell_trades"])),
                ("AGGTRADE_EVENTCOUNT", int(row["_event_count"])),
            )
            rows.append((int(row["_opening"]), values, bool(row["_consecutive"])))
        if check_stop is not None:
            check_stop()
        return rows
    except InterruptedError:
        raise
    except (
        ArithmeticError,
        csv.Error,
        ValueError,
        pl.exceptions.PolarsError,
    ):
        # Any parser/API/cast/overflow uncertainty is replayed by VisionArchive's
        # established CSV path, which preserves its protocol errors and edge cases.
        return None
