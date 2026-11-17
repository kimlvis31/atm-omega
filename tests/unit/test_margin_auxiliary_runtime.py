"""Source-read/mutation-fence separation and optional-input failure truth; no DB/thread/network."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_margin_session_auxiliary import DESTINATION, prepared
from test_margin_trading import Session

from omega.domain.errors import ProtocolError, StorageError
from omega.runtime.margin_trading import MarginTradingService


class Fence:
    def __init__(self):
        self.depth = 0

    def acquire(self, **_kwargs):
        self.depth += 1
        return True

    def release(self):
        self.depth -= 1


class AuxiliarySession(Session):
    analysis_input_channels = {"binance:usdm:BTCUSDT": ("trade_flow",)}

    def __init__(self, *args, pending=False, financial_only=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.pending, self.financial_only = pending, financial_only
        self.instruments = (SimpleNamespace(instrument=prepared(0).frame.candle.instrument),)

    def snapshot(self):
        return {**super().snapshot(), "pending_frame": self.pending}

    def shutdown_status(self):
        return {
            "recovery_required": self.financial_only,
            "needs_settlement": self.financial_only,
            "next_open_ms": self.next,
        }

    def shutdown_run_once(self, candles, **parameters):
        return self.run_once(candles, **parameters)


class Source:
    def __init__(self, fence, *, failure=None, swap=False):
        self.fence, self.failure, self.swap = fence, failure, swap
        self.destination_id, self.reads, self.frame_reads = DESTINATION, [], []

    def read(self, *args, **kwargs):
        assert self.fence.depth == 0
        self.reads.append((args, kwargs))
        return (prepared(0).frame.candle,)

    def read_frames(self, *args, **kwargs):
        assert self.fence.depth == 0
        self.frame_reads.append((args, kwargs))
        if self.swap:
            self.destination_id = "e" * 64
        if self.failure is not None:
            raise self.failure
        return (prepared(0),)


def runtime(root, *, session_type=AuxiliarySession, failure=None, swap=False, **options):
    fence = Fence()
    source = Source(fence, failure=failure, swap=swap)
    session = session_type(root, {"account_id": "demo"}, [], 0, **options)
    owner = MarginTradingService(
        root, None, lambda: source, core_epoch="epoch", clock_ms=lambda: 60000, mutation_lock=fence
    )
    owner.source = source
    owner.sessions["demo"] = session
    owner.data = {
        "accounts": {
            "demo": {
                "feed_enabled": True,
                "parameters": {"strategies": [], "brackets": []},
            }
        }
    }
    return owner, source, session, fence


def test_optional_frames_are_read_once_outside_mutation_and_forwarded_only_for_aux(tmp_path):
    owner, source, session, fence = runtime(tmp_path)
    owner._tick()
    assert len(source.reads) == len(source.frame_reads) == len(session.calls) == 1
    args, options = source.frame_reads[0]
    assert args == (prepared(0).frame.candle.instrument, 0, 60000)
    assert options == {
        "as_of_ms": 60000,
        "channels": ("trade_flow",),
        "maximum_metric_age_ms": 0,
        "limit": 1,
    }
    assert session.calls[0][1]["prepared_frames"] == (prepared(0),)
    assert session.calls[0][1]["auxiliary_destination"] == DESTINATION
    assert fence.depth == 0


@pytest.mark.parametrize(
    "failure", [StorageError("auxiliary store failed"), OSError("read"), TimeoutError("late read")]
)
def test_optional_read_failure_exposes_reason_and_forwards_typed_missing_input(tmp_path, failure):
    owner, _source, session, _fence = runtime(tmp_path, failure=failure)
    owner._tick()
    assert len(session.calls) == 1 and "demo" not in owner.blocked_accounts
    value = session.calls[0][1]["prepared_frames"][0]
    assert not value.pins and value.frame.inputs[0].quality == "missing"
    assert value.frame.inputs[0].coverage == 0
    assert owner.views["demo"]["feed_reason"] == "auxiliary_read_" + type(failure).__name__


@pytest.mark.parametrize("failure", [ProtocolError("bad pin"), None])
def test_integrity_error_or_destination_swap_does_not_become_missing_data(tmp_path, failure):
    owner, _source, session, fence = runtime(tmp_path, failure=failure, swap=failure is None)
    owner._tick()
    assert not session.calls and fence.depth == 0
    assert owner.views["demo"]["feed_state"] == "blocked"
    assert owner.views["demo"]["feed_reason"] == "ProtocolError"


def test_ordinary_ohlcv_feed_keeps_original_run_arguments_and_no_optional_io(tmp_path):
    owner, source, session, _fence = runtime(tmp_path, session_type=Session)
    owner._tick()
    assert not source.frame_reads
    assert session.calls[0][1] == {"as_of_ms": 60000, "brackets": (), "entries_allowed": True}


def test_pending_replay_never_reads_new_candles_or_auxiliary_revisions(tmp_path):
    owner, source, session, _fence = runtime(tmp_path, pending=True)
    owner._tick()
    assert not source.reads and not source.frame_reads
    assert session.calls[0][0] == ()
    assert "prepared_frames" not in session.calls[0][1]


def test_financial_only_shutdown_never_requires_auxiliary_fetch(tmp_path):
    owner, source, session, _fence = runtime(tmp_path, financial_only=True)
    owner._tick()
    assert len(source.reads) == 1 and not source.frame_reads
    assert "prepared_frames" not in session.calls[0][1]


def test_financial_storage_error_after_optional_read_is_not_swallowed(tmp_path):
    class FailedCommit(AuxiliarySession):
        def run_once(self, _candles, **_parameters):
            raise StorageError("financial commit unconfirmed")

    owner, source, _session, _fence = runtime(tmp_path, session_type=FailedCommit)
    owner._tick()
    assert len(source.frame_reads) == 1
    assert "demo" in owner.blocked_accounts
    assert owner.views["demo"]["manual_recovery_required"] is True


def test_persisted_destination_is_checked_before_the_optional_read(tmp_path):
    owner, source, session, _fence = runtime(tmp_path)
    view = deepcopy(session.snapshot())
    view["market_inputs"] = {"destination": "e" * 64}
    with pytest.raises(ProtocolError, match="destination changed"):
        owner._analysis_inputs(session, view, (prepared(0).frame.candle,), as_of_ms=60000)
    assert not source.frame_reads and not session.calls
