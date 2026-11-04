"""Tests for the enable_events broadcast.

Workers started without -E send no task events. Phlower turns them on with
a periodic enable_events broadcast, so these tests lock in the flag parsing
and that a failed broadcast does not break the consumer loop.
"""

from __future__ import annotations

from phlower.config import Config
from phlower.events import CeleryEventConsumer
from phlower.store import Store


class FakeControl:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls = 0
        self.error = error

    def enable_events(self) -> None:
        self.calls += 1
        if self.error:
            raise self.error


class FakeApp:
    def __init__(self, control: FakeControl) -> None:
        self.control = control


def _make_consumer() -> CeleryEventConsumer:
    config = Config()
    return CeleryEventConsumer(config, Store(config))


def test_enable_worker_events_defaults_to_on(monkeypatch):
    monkeypatch.delenv("ENABLE_WORKER_EVENTS", raising=False)
    assert Config().enable_worker_events is True


def test_enable_worker_events_can_be_turned_off(monkeypatch):
    for raw in ("false", "0", "no", "off", " False "):
        monkeypatch.setenv("ENABLE_WORKER_EVENTS", raw)
        assert Config().enable_worker_events is False


def test_enable_worker_events_broadcasts():
    control = FakeControl()
    _make_consumer()._enable_worker_events(FakeApp(control))
    assert control.calls == 1


def test_enable_worker_events_swallows_broker_errors():
    control = FakeControl(error=ConnectionError("broker down"))
    _make_consumer()._enable_worker_events(FakeApp(control))
    assert control.calls == 1


def test_control_app_bounds_socket_waits():
    options = _make_consumer()._make_control_app().conf.broker_transport_options
    assert options["socket_timeout"] > 0
    assert options["socket_connect_timeout"] > 0
