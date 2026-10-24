"""Job queue behind one interface: in-memory for tests, Azure Service Bus for real.

Service Bus semantics we rely on (and the in-memory queue imitates):
- PEEK-LOCK: receiving a message locks it; it is only removed when we call complete().
  If the worker crashes, the lock expires and the message is delivered again.
  => delivery is AT-LEAST-ONCE, so the worker must be idempotent.
- abandon(): give the message back now, delivery_count goes up (used for transient errors).
- dead_letter(reason): move it to the dead-letter sub-queue for a human to inspect
  (used for permanent errors, and automatically once MaxDeliveryCount is reached).
- message_id = content hash: with duplicate detection enabled on the queue, Service Bus
  itself drops a second send of the same file inside the detection window.
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class JobMessage:
    job_id: str
    content_hash: str

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
    def send(self, message: JobMessage) -> None: ...
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
        self.queue._redeliver(self.body, self.delivery_count)

    def dead_letter(self, reason: str) -> None:
        self.settled = True
        self.queue.dead_letters.append((self.body, reason))

    def renew_lock(self) -> None:
        pass


@dataclass
class InMemoryQueue:
    max_delivery_count: int = 5
    _items: deque[tuple[JobMessage, int]] = field(default_factory=deque)
    completed: list[JobMessage] = field(default_factory=list)
    dead_letters: list[tuple[JobMessage, str]] = field(default_factory=list)

    def send(self, message: JobMessage) -> None:
        self._items.append((message, 0))

    def _redeliver(self, message: JobMessage, delivery_count: int) -> None:
        if delivery_count >= self.max_delivery_count:
            self.dead_letters.append((message, "MaxDeliveryCountExceeded"))
        else:
            self._items.append((message, delivery_count))

    def receive(self, max_messages: int = 1, max_wait_s: float = 0) -> Iterator[Received]:
        for _ in range(max_messages):
            if not self._items:
                return
            message, count = self._items.popleft()
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

    def send(self, message: JobMessage) -> None:
        from azure.servicebus import ServiceBusMessage

        with self._client.get_queue_sender(self._queue_name) as sender:
            sender.send_messages(ServiceBusMessage(
                message.to_json(),
                message_id=message.content_hash,
                content_type="application/json",
            ))

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
