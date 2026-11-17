"""One fixed critical delivery per due set; independent revision, ACK and clocks."""

import threading
from copy import deepcopy
from types import SimpleNamespace

import pytest

from omega.runtime.control_notifications import NotificationLoop
from omega.runtime.speech import announcement


def incident(identity, *, severity="critical", revision=1, acknowledged=False, **details):
    return {
        "id": identity,
        "severity": severity,
        "revision": revision,
        "acknowledged": acknowledged,
        **details,
    }


class Speech:
    def __init__(self):
        self.calls, self.configurations, self.cancels = [], [], 0
        self.failure, self.accepted = None, True

    def configure(self, preferences):
        self.configurations.append(deepcopy(preferences))
        if self.failure == "configure":
            raise OSError("inert configuration failure")

    def say(self, *, critical, repeat):
        self.calls.append((critical, repeat))
        if self.failure == "say":
            raise OSError("inert delivery failure")
        return self.accepted

    def cancel(self):
        self.cancels += 1
        if self.failure == "cancel":
            raise OSError("inert cancellation failure")

    def close(self):
        pass


def setup(rows, *, sound=True, speech=True, emit=None):
    now, beeps, voice = [0], [], Speech()
    preferences = {"sound": sound, "speech": speech, "repeat_critical_s": 30}
    control = SimpleNamespace(
        changed=threading.Event(),
        snapshot=lambda: {"incidents": deepcopy(rows)},
    )
    loop = NotificationLoop(
        control,
        lambda: {"notifications": preferences},
        emit=beeps.append if emit is None else emit,
        clock=lambda: now[0],
        speech=voice,
    )
    return loop, now, beeps, voice, preferences


def test_current_retained_unknown_startup_group_and_repeat_keep_each_member_bookkeeping():
    rows = [
        incident("retained", provenance="previous_core", message="private-token-path"),
        incident("current", provenance="current_core"),
        incident("legacy", provenance="unknown"),
    ]
    loop, now, beeps, speech, _ = setup(rows)
    before = deepcopy(rows)
    assert loop.tick() == 30
    assert beeps == [True] and speech.calls == [(True, False)]
    assert loop.last_emitted == dict.fromkeys(("retained", "current", "legacy"), 0)
    assert loop.last_revision == dict.fromkeys(("retained", "current", "legacy"), 1)
    assert loop.tick() == 30 and beeps == [True]
    now[0] = 29
    assert loop.tick() == 1 and speech.calls == [(True, False)]
    now[0] = 30
    assert loop.tick() == 30
    assert beeps == [True, True] and speech.calls == [(True, False), (True, True)]
    assert rows == before  # No automatic ACK, clear, provenance or recovery mutation.


def test_ack_one_record_cancels_queued_speech_but_remaining_records_repeat():
    rows = [incident("a"), incident("b"), incident("c")]
    loop, now, beeps, speech, _ = setup(rows)
    loop.tick()
    rows[0]["acknowledged"] = True
    now[0] = 5
    assert loop.tick() == 25 and speech.cancels == 1
    assert set(loop.last_emitted) == set(loop.last_revision) == {"b", "c"}
    now[0] = 30
    loop.tick()
    assert beeps == [True, True] and speech.calls[-1] == (True, True)
    rows[1]["acknowledged"] = rows[2]["acknowledged"] = True
    assert loop.tick() is None
    assert loop.last_emitted == loop.last_revision == {} and speech.cancels == 2


def test_distinct_due_times_do_not_reset_not_due_records_to_the_group_clock():
    rows = [incident("a")]
    loop, now, beeps, speech, _ = setup(rows)
    loop.tick()
    now[0] = 10
    rows.append(incident("b"))
    assert loop.tick() == 20
    assert loop.last_emitted == {"a": 0, "b": 10}
    now[0] = 30
    assert loop.tick() == 10
    assert loop.last_emitted == {"a": 30, "b": 10}
    now[0] = 40
    assert loop.tick() == 20
    assert loop.last_emitted == {"a": 30, "b": 40}
    assert beeps == [True] * 4
    assert speech.calls == [(True, False), (True, False), (True, True), (True, True)]


def test_new_revision_is_immediate_and_mixed_repeat_fresh_group_uses_neutral_first_text():
    rows = [incident("a"), incident("b")]
    loop, now, beeps, speech, _ = setup(rows)
    loop.tick()
    now[0] = 5
    rows[0]["revision"] = 2
    assert loop.tick() == 25
    assert loop.last_revision == {"a": 2, "b": 1}
    assert loop.last_emitted == {"a": 5, "b": 0}
    now[0] = 30
    rows.append(incident("fresh"))
    assert loop.tick() == 5
    assert speech.calls == [(True, False)] * 3
    assert beeps == [True] * 3
    assert loop.last_emitted == {"a": 5, "b": 30, "fresh": 30}


def test_warning_once_escalation_and_noncritical_order_remain_individual():
    rows = [
        incident("a"),
        incident("warning-1", severity="warning"),
        incident("b"),
        incident("warning-2", severity="warning"),
    ]
    loop, now, beeps, speech, _ = setup(rows)
    loop.tick()
    assert beeps == [True, False, False]
    assert speech.calls == [(True, False), (False, False), (False, False)]
    loop.tick()
    assert len(beeps) == 3
    now[0] = 1
    rows[1].update(severity="critical", revision=2)
    assert loop.tick() == 29
    assert beeps[-1] is True and speech.calls[-1] == (True, False)
    assert loop.last_revision["warning-1"] == 2
    assert loop.last_emitted["a"] == loop.last_emitted["b"] == 0


@pytest.mark.parametrize("failure", ("configure", "emit", "say", "reject"))
def test_group_failure_stays_sticky_and_marks_only_the_attempted_members(failure):
    rows = [incident("a"), incident("b")]
    loop, now, beeps, speech, _ = setup(rows)
    if failure == "emit":

        def broken_emit(critical):
            beeps.append(critical)
            raise OSError("inert beep failure")

        loop.emit = broken_emit
    elif failure == "reject":
        speech.accepted = False
    else:
        speech.failure = failure
    assert loop.tick() == 30
    assert loop.delivery_failed and beeps == [True] and speech.calls == [(True, False)]
    assert loop.last_emitted == {"a": 0, "b": 0}
    assert loop.last_revision == {"a": 1, "b": 1}
    speech.failure, speech.accepted = None, True
    loop.emit = beeps.append
    now[0] = 1
    assert loop.tick() == 29 and len(beeps) == 1
    now[0] = 30
    loop.tick()
    assert loop.delivery_failed and speech.calls[-1] == (True, True)


@pytest.mark.parametrize("sound,speech", ((False, False), (False, True), (True, False)))
def test_muted_delivery_remains_optional_without_changing_record_schedule(sound, speech):
    loop, now, beeps, voice, _ = setup([incident("a"), incident("b")], sound=sound, speech=speech)
    assert loop.tick() == 30 and loop.last_emitted == {"a": 0, "b": 0}
    assert beeps == ([True] if sound else [])
    assert voice.calls == ([(True, False)] if speech else [])
    assert not loop.delivery_failed
    now[0] = 30
    loop.tick()
    assert len(beeps) == (2 if sound else 0) and len(voice.calls) == (2 if speech else 0)


def test_stop_before_delivery_does_not_mark_future_group_members():
    rows = [incident("warning", severity="warning"), incident("critical")]
    loop, _now, beeps, speech, _ = setup(rows)

    def stop_after_warning(critical):
        beeps.append(critical)
        loop.stopped.set()

    loop.emit = stop_after_warning
    loop.tick()
    assert beeps == [False] and speech.calls == [(False, False)]
    assert loop.last_emitted == {"warning": 0} and "critical" not in loop.last_revision
    assert loop.tick() is None


@pytest.mark.parametrize("language", ("ko_KR", "auto", "en_US"))
def test_first_critical_announcement_is_fixed_neutral_and_repeat_remains_unacknowledged(language):
    text = announcement(language, critical=True)
    if language in {"ko_KR", "auto"}:
        assert text == "확인이 필요한 중요한 알림이 있습니다. Omega 알림을 확인하세요."
        assert "감지" not in text and "새" not in text
        assert (
            announcement(language, critical=True, repeat=True)
            == "Omega 중요 알림이 아직 확인되지 않았습니다."
        )
    else:
        assert text == "A critical incident needs attention. Check Omega notifications."
        assert (
            announcement(language, critical=True, repeat=True)
            == "Omega has an unacknowledged critical incident."
        )
