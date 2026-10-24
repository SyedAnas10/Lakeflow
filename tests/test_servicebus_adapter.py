"""Unit test of the Service Bus adapter's settlement calls, using a fake receiver.

The end-to-end Service Bus path runs against the emulator in docker-compose; this test
checks that each worker decision maps to the right SDK call.
"""

from __future__ import annotations

from lakeflow.queue import JobMessage, _SbReceived


class FakeMessage:
    def __init__(self, body: str, delivery_count: int) -> None:
        self._body = body
        self.delivery_count = delivery_count

    def __str__(self) -> str:
        return self._body


class FakeReceiver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def complete_message(self, m) -> None:
        self.calls.append(("complete", m))

    def abandon_message(self, m) -> None:
        self.calls.append(("abandon", m))

    def dead_letter_message(self, m, reason: str) -> None:
        self.calls.append(("dead_letter", reason))

    def renew_message_lock(self, m) -> None:
        self.calls.append(("renew", m))


def test_settlement_mapping() -> None:
    msg = FakeMessage(JobMessage("job-1", "abc").to_json(), delivery_count=2)
    receiver = FakeReceiver()
    r = _SbReceived(receiver, msg)

    assert r.body == JobMessage("job-1", "abc")
    assert r.delivery_count == 2
    r.renew_lock()
    r.abandon()
    r.complete()
    r.dead_letter("x" * 5000)

    kinds = [c[0] for c in receiver.calls]
    assert kinds == ["renew", "abandon", "complete", "dead_letter"]
    assert len(receiver.calls[-1][1]) == 4096  # long error text is capped before it is sent
