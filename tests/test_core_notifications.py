"""Critical audio repeats without a GUI and audio failures do not stop the core."""

from omega.runtime.control import ControlState
from omega.runtime.control_notifications import NotificationLoop
from omega.runtime.settings import SettingsStore


def test_critical_repeats_until_acknowledged_but_warning_sounds_once(tmp_path):
    control = ControlState(tmp_path / "control.json")
    settings = SettingsStore(tmp_path / "settings.json")
    now, delivered = [0], []
    loop = NotificationLoop(control, settings.snapshot, emit=delivered.append, clock=lambda: now[0])
    assert loop.tick() is None
    control.incident("NORMAL", "warning")
    assert loop.tick() is None
    assert delivered == [False]
    loop.tick()
    assert delivered == [False]
    control.incident("DANGER")
    assert loop.tick() == 30
    assert delivered == [False, True]
    now[0] += 29
    assert loop.tick() == 1
    now[0] += 1
    loop.tick()
    assert delivered == [False, True, True]
    identity = next(
        item["id"] for item in control.snapshot()["incidents"] if item["code"] == "DANGER"
    )
    control.acknowledge(identity)
    assert loop.tick() is None


def test_audio_failure_is_visible_and_optional_sound_disable_is_respected(tmp_path):
    control = ControlState(tmp_path / "control.json")
    settings = SettingsStore(tmp_path / "settings.json")

    def failure(_critical):
        raise OSError("audio unavailable")

    loop = NotificationLoop(control, settings.snapshot, emit=failure)
    control.incident("DANGER")
    loop.tick()
    assert loop.delivery_failed
    settings.update({"notifications": {"sound": False}}, revision=0)
    silent = NotificationLoop(control, settings.snapshot, emit=failure)
    silent.tick()
    assert not silent.delivery_failed


def test_severity_escalation_realerts_immediately_and_volatile_ack_silences(tmp_path, monkeypatch):
    import omega.runtime.control as module

    control = ControlState(tmp_path / "control.json")
    settings = SettingsStore(tmp_path / "settings.json")
    delivered = []
    loop = NotificationLoop(control, settings.snapshot, emit=delivered.append, clock=lambda: 0)
    control.incident("DATA", "warning")
    loop.tick()
    control.incident("DATA", "critical")
    loop.tick()
    assert delivered == [False, True]
    incident = control.snapshot()["incidents"][0]
    assert incident["severity"] == "critical" and incident["revision"] == 2
    monkeypatch.setattr(module, "atomic_json", lambda *_: (_ for _ in ()).throw(OSError("full")))
    control.storage_failed = True
    result = control.command(
        "admin",
        {"id": "ack", "kind": "incident.acknowledge", "payload": {"incident_id": incident["id"]}},
        lambda *_: None,
    )
    assert result["status"] == "volatile"
    assert result["result"]["durable"] is False
    assert loop.tick() is None
    assert control.snapshot()["paused"]
    assert not control.snapshot()["storage_available"]
