"""Real actor/file evidence, real TS/financial owners; fake source/model/clock.

This integration boundary is deterministic and offline. It does not substitute
for actual PostgreSQL/child/wall-clock GUI acceptance.
"""

from decimal import Decimal as D

import pytest
from test_causal_feature_delivery import ready
from test_causal_inference_service import command, terminal
from test_margin_session import tables

from omega.domain.external_features import ExternalFeatureDelivery, ExternalFeaturePin
from omega.domain.models import Candle
from omega.runtime.margin_session import MarginTradingSession


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_actual_typed_pin_enters_real_neural_ts_then_retirement_keeps_original_and_stop(
    tmp_path, monkeypatch, mode
):
    service, state, provider, binding, first, cutoff, snapshot = ready(tmp_path, monkeypatch)
    session = None
    try:
        account = {
            "account_id": "typed-financial",
            "balance": "1000",
            "positions": [
                {
                    "instrument": first.instrument.key,
                    "owner_id": "neural",
                    "margin_mode": mode,
                    "leverage": 10,
                    "budget_margin": "100",
                    **({"isolated_collateral": "100"} if mode == "isolated" else {}),
                }
            ],
        }
        row = {
            "id": "neural",
            "instrument": first.instrument.key,
            "module": "neural_signal",
            "configuration": {"quantity": "1", "local_stop_rate": ".05"},
            "analysis": {},
            "constraints": {"leverage": "10", "can_short": True},
            "external_features": [binding.document()],
        }
        session = MarginTradingSession(
            tmp_path,
            account,
            [row],
            first.open_time_ms,
            create=True,
            external_feature_provider=provider,
            clock_ms=lambda: state["clock"][0],
        )
        session.resume("explicit-start")
        session.run_once((first,), as_of_ms=cutoff, brackets=tables())
        pending = session.execution.snapshot().pending
        assert len(pending) == 1 and not pending[0].original.proposal.reducing
        original = pending[0]
        frozen = session._meta["last_frame"]
        entry = frozen["external_features"]["neural"][0]
        proof, pin = (
            ExternalFeatureDelivery.parse(entry["delivery"]),
            ExternalFeaturePin.parse(entry["pin"]),
        )
        assert proof.snapshot == snapshot and proof.snapshot.request_id == snapshot.request_id
        assert provider.authorize(pin, snapshot, first, decision_at_ms=cutoff)
        assert pin.frame_ref == frozen["digest"] and pin.durable is True
        command(service, "retire", "retire", deployment_id=binding.instance_id)
        assert terminal(service, "retire")["state"] == "complete"
        assert not provider.authorize(pin, snapshot, first, decision_at_ms=cutoff)
        for index in range(11, 16):
            # Keep the original proxy mark until entry settles; changing it to
            # 100 here would exceed the deliberately fixed entry-loss bound
            # of the original 101-price reservation and correctly reject it.
            price = D(90) if index >= 13 else first.close
            opening = first.close if index == 13 else price
            bar = Candle(
                first.instrument,
                index * 60000,
                (index + 1) * 60000,
                opening,
                max(opening, price),
                min(opening, price),
                price,
                D(2),
            )
            state["clock"][0] = bar.close_time_ms + 10
            session.run_once((bar,), as_of_ms=state["clock"][0], brackets=tables())
        financial = session.execution.snapshot()
        assert len(financial.account.ledger) == 2
        assert financial.account.ledger[0].fact.order_id == original.order_id
        assert financial.account.positions[0].quantity == 0 and not financial.pending
        assert session.execution.original_intent(original.order_id) == original
        assert session._adapters["neural"].module.snapshot()["halted"] is True
        assert (
            session._meta["last_frame"]["external_features"]["neural"][0]["delivery"]["disposition"]
            == "unavailable"
        )
        assert state["source_calls"] == 1
    finally:
        if session is not None:
            session.close()
        service.close()
