"""Derived observation bytes preserve dtype, availability and revision identities."""

import json
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, localcontext
from hashlib import sha256

import pytest

from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import (
    MAX_PAYLOAD_BYTES,
    decode_observation,
    encode_observation,
    observation_checksum,
)
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def observation(**changes):
    return AuxiliaryInput(
        **{
            "channel": "trade_flow",
            "values": (("count", 2), ("notional", Decimal("123.4500"))),
            "window_start_ms": 60_000,
            "window_end_ms": 120_000,
            "event_time_ms": 120_000,
            "available_at_ms": 120_010,
            "quality": "observed",
            "coverage": Decimal(1),
            "source": "fixture:normalized",
            **changes,
        }
    )


def document():
    return json.loads(encode_observation(INSTRUMENT, observation()))


def encoded(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def test_canonical_encoding_is_stable_typed_and_matches_explicit_schema_bytes():
    payload = encode_observation(INSTRUMENT, observation())
    expected = (
        b'{"instrument":{"market":"usdm","provider":"binance","symbol":"ETHUSDT"},'
        b'"observation":{"available_at_ms":120010,"channel":"trade_flow","coverage":"1",'
        b'"event_time_ms":120000,"quality":"observed","source":"fixture:normalized",'
        b'"values":[{"name":"count","type":"int","value":2},'
        b'{"name":"notional","type":"decimal","value":"123.4500"}],'
        b'"window_end_ms":120000,"window_start_ms":60000},"schema_version":1}'
    )
    assert payload == expected and observation_checksum(payload) == sha256(expected).hexdigest()
    instrument, decoded = decode_observation(payload)
    assert instrument == INSTRUMENT and decoded == observation()
    assert type(decoded.mapping()["count"]) is int
    assert type(decoded.mapping()["notional"]) is Decimal


def test_field_order_is_canonical_but_decimal_scale_is_not_context_normalized():
    first = observation(values=(("z", Decimal("1.00")), ("a", 1)))
    second = observation(values=(("a", 1), ("z", Decimal("1.00"))))
    assert encode_observation(INSTRUMENT, first) == encode_observation(INSTRUMENT, second)
    different_scale = replace(first, values=(("z", Decimal("1.0")), ("a", 1)))
    left, right = (
        encode_observation(INSTRUMENT, first),
        encode_observation(INSTRUMENT, different_scale),
    )
    assert left != right and observation_checksum(left) != observation_checksum(right)
    assert decode_observation(left) == decode_observation(
        right
    )  # Store may compare decoded values.
    exact = observation(values=(("value", Decimal("12345678901234567890.123456789")),))
    with localcontext() as context:
        context.prec = 2
        payload = encode_observation(INSTRUMENT, exact)
        assert decode_observation(payload)[1] == exact
    assert b"12345678901234567890.123456789" in payload


def test_equal_numeric_value_keeps_int_and_decimal_dtype_and_utf8_source():
    original = observation(
        values=(("integer", 1), ("decimal", Decimal(1))), source="fixture:관측📈"
    )
    payload = encode_observation(INSTRUMENT, original)
    decoded = decode_observation(payload)[1]
    assert "관측📈".encode() in payload
    assert (
        type(decoded.mapping()["integer"]) is int and type(decoded.mapping()["decimal"]) is Decimal
    )


@pytest.mark.parametrize("quality", ["observed", "partial", "missing", "unsupported", "carried"])
def test_quality_coverage_and_source_times_roundtrip_without_fabricating_readiness(quality):
    empty = quality in {"missing", "unsupported"}
    original = observation(
        quality=quality,
        coverage=Decimal(0) if empty else Decimal(".5"),
        values=() if empty else (("count", 3),),
    )
    result = decode_observation(encode_observation(INSTRUMENT, original))[1]
    assert result == original and result.available_at_ms == 120_010
    assert result.event_time_ms == 120_000 and not result.usable_at(120_009)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(schema_version=True),
        lambda row: row.update(schema_version=2),
        lambda row: row.update(schema_version=1.0),
        lambda row: row.update(raw="not exchange bytes"),
        lambda row: row.update(credentials={"token": "synthetic-only"}),
        lambda row: row["instrument"].update(market="unsupported"),
        lambda row: row["instrument"].update(symbol="bad symbol"),
        lambda row: row["instrument"].update(api_key="synthetic-only"),
        lambda row: row["observation"].update(available_at_ms=119_999),
        lambda row: row["observation"].update(window_start_ms=60_001),
        lambda row: row["observation"].update(event_time_ms=True),
        lambda row: row["observation"].update(quality="complete-maybe"),
        lambda row: row["observation"].update(coverage="1.01"),
        lambda row: row["observation"].update(coverage=1),
        lambda row: row["observation"].update(source="bad\nsource"),
        lambda row: row["observation"].update(extra=0),
        lambda row: row["observation"]["values"][0].update(type="float"),
        lambda row: row["observation"]["values"][0].update(value=True),
        lambda row: row["observation"]["values"][0].update(value="2"),
        lambda row: row["observation"]["values"][1].update(value=123.45),
        lambda row: row["observation"]["values"][1].update(value="NaN"),
        lambda row: row["observation"]["values"][1].update(value=" 1.0 "),
        lambda row: row["observation"]["values"][1].update(name="count"),
        lambda row: row["observation"]["values"][1].update(extra=1),
    ],
)
def test_unknown_schema_keys_bad_dtypes_and_domain_corruption_are_rejected(mutation):
    row = document()
    mutation(row)
    with pytest.raises(ValidationError):
        decode_observation(encoded(row))


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\xff",
        b"[]",
        b"{}",
        b"true",
        b"{",
        None,
        "{}",
        bytearray(b"{}"),
        b'{"x":NaN}',
        b'{"x":Infinity}',
    ],
)
def test_malformed_encoding_requires_strict_nonempty_utf8_json_bytes(payload):
    with pytest.raises(ValidationError):
        decode_observation(payload)


def test_duplicate_keys_are_rejected_in_all_object_layers():
    payload = encode_observation(INSTRUMENT, observation())
    for before, after in (
        (b'"schema_version":1', b'"schema_version":1,"schema_version":1'),
        (b'"market":"usdm"', b'"market":"spot","market":"usdm"'),
        (b'"quality":"observed"', b'"quality":"partial","quality":"observed"'),
        (b'"type":"int"', b'"type":"decimal","type":"int"'),
    ):
        with pytest.raises(ValidationError, match="duplicate"):
            decode_observation(payload.replace(before, after))


def test_byte_size_and_field_count_boundaries_are_enforced_without_long_pytest_ids():
    payload = encode_observation(INSTRUMENT, observation())
    exactly = payload + b" " * (MAX_PAYLOAD_BYTES - len(payload))
    assert decode_observation(exactly)[1] == observation()
    assert observation_checksum(exactly) == sha256(exactly).hexdigest()
    with pytest.raises(ValidationError):
        decode_observation(exactly + b" ")
    with pytest.raises(ValidationError):
        observation_checksum(exactly + b" ")
    row = document()
    row["observation"]["values"] = [
        {"name": f"field{i}", "type": "int", "value": i} for i in range(64)
    ]
    assert len(decode_observation(encoded(row))[1].values) == 64
    row["observation"]["values"].append({"name": "overflow", "type": "int", "value": 0})
    with pytest.raises(ValidationError):
        decode_observation(encoded(row))
    oversized = observation(values=(("value", Decimal("1." + "1" * MAX_PAYLOAD_BYTES)),))
    with pytest.raises(ValidationError):
        encode_observation(INSTRUMENT, oversized)


def test_signed_and_scientific_decimals_keep_exact_sign_exponent_and_scale():
    original = observation(values=(("a", Decimal("-1.25E-30")), ("b", Decimal("-0.00"))))
    result = decode_observation(encode_observation(INSTRUMENT, original))[1]
    assert result.mapping()["a"].as_tuple() == original.mapping()["a"].as_tuple()
    assert result.mapping()["b"].as_tuple() == original.mapping()["b"].as_tuple()


def test_revision_receipts_are_typed_immutable_and_checksum_is_bytes_not_schema_proof():
    payload = encode_observation(INSTRUMENT, observation())
    digest = observation_checksum(payload)
    stored = StoredAuxiliary(INSTRUMENT, observation(), 1, digest, None)
    receipt = AuxiliaryWriteReceipt(1, digest, True)
    with pytest.raises(FrozenInstanceError):
        receipt.inserted = False
    assert stored.checksum == receipt.checksum
    assert observation_checksum(b"not a valid schema") == sha256(b"not a valid schema").hexdigest()
    with pytest.raises(ValidationError):
        decode_observation(b"not a valid schema")


@pytest.mark.parametrize(
    "revision,checksum,inserted",
    [
        (True, "a" * 64, True),
        (0, "a" * 64, True),
        (2**63, "a" * 64, True),
        (1, "A" * 64, True),
        (1, "a" * 63, True),
        (1, "a" * 64, 1),
    ],
)
def test_invalid_revision_and_checksum_receipts_fail_before_store_use(revision, checksum, inserted):
    with pytest.raises(ValidationError):
        AuxiliaryWriteReceipt(revision, checksum, inserted)


def test_stored_record_rejects_invalid_identity_observation_or_previous_checksum():
    for instrument, value, previous in (
        ("ETHUSDT", observation(), None),
        (INSTRUMENT, {}, None),
        (INSTRUMENT, observation(), "not-a-sha256"),
    ):
        with pytest.raises(ValidationError):
            StoredAuxiliary(instrument, value, 1, "a" * 64, previous)
    with pytest.raises(ValidationError):
        encode_observation(INSTRUMENT, {})


@pytest.mark.parametrize("revision,previous", [(1, "b" * 64), (2, None)])
def test_stored_revision_predecessor_presence_matches_append_only_chain(revision, previous):
    with pytest.raises(ValidationError, match="predecessor"):
        StoredAuxiliary(INSTRUMENT, observation(), revision, "a" * 64, previous)
