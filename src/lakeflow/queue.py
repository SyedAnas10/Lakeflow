"""Job queue behind one interface: in-memory for tests, Azure Service Bus for real.

Service Bus semantics we rely on (and the in-memory queue imitates):
- PEEK-LOCK: receiving a message locks it; it is only removed when we call complete().
  If the worker crashes, the lock expires and the message is delivered again.
  => delivery is AT-LEAST-ONCE, so the worker must be idempotent.
- abandon(): give the message back now, delivery_count goes up (used for transient errors).
  It is available again IMMEDIATELY, so abandon is no way to "wait": use a scheduled send.
- dead_letter(reason): move it to the dead-letter sub-queue for a human to inspect
  (used for permanent errors, and automatically once MaxDeliveryCount is reached).
- send(delay_s=...): a scheduled message only becomes visible at that time.
- message_id = send_id, a new id per deliberate send. Duplicate detection then drops only
  the SDK's own network retries of the SAME send. Using the content hash as message_id
  would also silently drop a deliberate retry or reschedule inside the detection window.
  Duplicate files are already stopped earlier, by the unique content_hash in the database.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol


@dataclass(frozen=True)
class JobMessage:
    job_id: str
    content_hash: str
    send_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> JobMessage:
        return cls(**json.loads(raw))


class Received(Protocol):
    body: JobMessage
    delivery_count: int

    def complete(self) -> None: ...
    def abandon(self) -> None: ...
    def dead_letter(self, reason: str) -> None: ...
    def renew_lock(self) -> None: ...


class JobQueue(Protocol):
    def send(self, message: JobMessage, delay_s: float = 0) -> None: ...
    def receive(self, max_messages: int = 1, max_wait_s: float = 5) -> Iterator[Received]: ...


# ---------------------------------------------------------------- in-memory (tests)

@dataclass
class _MemReceived:
    queue: InMemoryQueue
    body: JobMessage
    delivery_count: int
    settled: bool = False

    def complete(self) -> None:
        self.settled = True
        self.queue.completed.append(self.body)

    def abandon(self) -> None:
        self.settled = True
        self.queue._redeliver(self.body, self.delivery_count)  # visible again at once

    def dead_letter(self, reason: str) -> None:
        self.settled = True
        self.queue.dead_letters.append((self.body, reason))

    def renew_lock(self) -> None:
        pass


@dataclass
class InMemoryQueue:
    max_delivery_count: int = 5
    clock: Callable[[], float] = time.monotonic  # tests swap in a fake clock
    _items: deque[tuple[JobMessage, int, float]] = field(default_factory=deque)
    completed: list[JobMessage] = field(default_factory=list)
    dead_letters: list[tuple[JobMessage, str]] = field(default_factory=list)

    def send(self, message: JobMessage, delay_s: float = 0) -> None:
        self._items.append((message, 0, self.clock() + delay_s))

    def _redeliver(self, message: JobMessage, delivery_count: int) -> None:
        if delivery_count >= self.max_delivery_count:
            self.dead_letters.append((message, "MaxDeliveryCountExceeded"))
        else:
            self._items.append((message, delivery_count, self.clock()))

    def _next_visible(self) -> tuple[JobMessage, int] | None:
        now = self.clock()
        for item in list(self._items):
            if item[2] <= now:
                self._items.remove(item)
                return item[0], item[1]
        return None

    def receive(self, max_messages: int = 1, max_wait_s: float = 0) -> Iterator[Received]:
        for _ in range(max_messages):
            nxt = self._next_visible()
            if nxt is None:
                return
            message, count = nxt
            received = _MemReceived(self, message, count + 1)
            yield received
            if not received.settled:  # simulate the lock expiring after a crash
                self._redeliver(message, count + 1)

    def __len__(self) -> int:
        return len(self._items)


# ---------------------------------------------------------------- Azure Service Bus

class _SbReceived:
    def __init__(self, receiver, msg) -> None:  # type: ignore[no-untyped-def]
        self._receiver = receiver
        self._msg = msg
        self.body = JobMessage.from_json(str(msg))
        self.delivery_count = msg.delivery_count

    def complete(self) -> None:
        self._receiver.complete_message(self._msg)

    def abandon(self) -> None:
        self._receiver.abandon_message(self._msg)

    def dead_letter(self, reason: str) -> None:
        self._receiver.dead_letter_message(self._msg, reason=reason[:4096])

    def renew_lock(self) -> None:
        self._receiver.renew_message_lock(self._msg)


class ServiceBusJobQueue:
    def __init__(self, connection_string: str | None, queue_name: str,
                 namespace: str | None = None) -> None:
        from azure.servicebus import ServiceBusClient

        if namespace:  # in Azure: managed identity, no shared access keys
            from azure.identity import DefaultAzureCredential

            self._client = ServiceBusClient(namespace, credential=DefaultAzureCredential())
        elif connection_string:
            self._client = ServiceBusClient.from_connection_string(connection_string)
        else:
            raise ValueError("set SERVICEBUS_NAMESPACE or SERVICEBUS_CONNECTION_STRING")
        self._queue_name = queue_name

    def send(self, message: JobMessage, delay_s: float = 0) -> None:
        from azure.servicebus import ServiceBusMessage

        sb_message = ServiceBusMessage(
            message.to_json(), message_id=message.send_id, content_type="application/json"
        )
        if delay_s > 0:
            sb_message.scheduled_enqueue_time_utc = (
                datetime.now(UTC) + timedelta(seconds=delay_s))
        with self._client.get_queue_sender(self._queue_name) as sender:
            sender.send_messages(sb_message)

    def receive(self, max_messages: int = 1, max_wait_s: float = 5) -> Iterator[Received]:
        from azure.servicebus import AutoLockRenewer

        # AutoLockRenewer keeps the lock alive while a long file (minutes) is processed,
        # otherwise the lock (e.g. 60 s) would expire and another worker would start it too.
        renewer = AutoLockRenewer(max_lock_renewal_duration=3600)
        with self._client.get_queue_receiver(self._queue_name, max_wait_time=max_wait_s,
                                             auto_lock_renewer=renewer) as receiver:
            for msg in receiver.receive_messages(max_message_count=max_messages,
                                                 max_wait_time=max_wait_s):
                yield _SbReceived(receiver, msg)
        renewer.close()


def make_queue(backend: str, *, connection_string: str | None, queue_name: str,
               max_delivery_count: int, namespace: str | None = None) -> JobQueue:
    if backend == "memory":
        return InMemoryQueue(max_delivery_count=max_delivery_count)
    if backend == "servicebus":
        return ServiceBusJobQueue(connection_string, queue_name, namespace)
    raise ValueError(f"unknown queue backend: {backend}")
