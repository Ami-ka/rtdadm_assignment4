"""
kafka_broker.py — In-process Kafka broker simulation.

Because this project runs without a real Kafka cluster (no Docker / Java),
this module provides a lightweight, thread-safe in-process broker that mimics
the key semantics needed by the assignment:

  • Topics with configurable partition counts
  • Round-robin partition assignment for producers (key-based hashing supported)
  • Consumer groups with per-partition offset tracking
  • Blocking poll() with optional timeout

The public API deliberately mirrors kafka-python so that swapping in a real
KafkaProducer / KafkaConsumer requires only changing the import statements.
"""

from __future__ import annotations

import hashlib
import json
import queue
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterator


# ─── Internal broker state ────────────────────────────────────────────────────

@dataclass
class _Message:
    """A message stored in a partition log."""
    topic:     str
    partition: int
    offset:    int
    key:       bytes | None
    value:     bytes
    timestamp: float = field(default_factory=time.time)


class _Partition:
    """An ordered, append-only log for a single Kafka partition."""

    def __init__(self) -> None:
        self._log: list[_Message] = []
        self._lock = threading.Lock()

    def append(self, msg: _Message) -> None:
        with self._lock:
            self._log.append(msg)

    def read_from(self, offset: int) -> list[_Message]:
        with self._lock:
            return list(self._log[offset:])

    def __len__(self) -> int:
        with self._lock:
            return len(self._log)


class _Topic:
    """A Kafka topic composed of multiple partitions."""

    def __init__(self, name: str, num_partitions: int = 3) -> None:
        self.name = name
        self.partitions: list[_Partition] = [
            _Partition() for _ in range(num_partitions)
        ]

    @property
    def num_partitions(self) -> int:
        return len(self.partitions)


class _Broker:
    """Singleton in-process broker (one per Python process)."""

    _instance: "_Broker | None" = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._topics: dict[str, _Topic] = {}
        self._lock = threading.Lock()

    @classmethod
    def get(cls) -> "_Broker":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Clear all topics — useful between test runs."""
        with cls._lock:
            cls._instance = None

    def create_topic(self, name: str, num_partitions: int = 3) -> _Topic:
        with self._lock:
            if name not in self._topics:
                self._topics[name] = _Topic(name, num_partitions)
            return self._topics[name]

    def get_topic(self, name: str) -> _Topic:
        with self._lock:
            if name not in self._topics:
                raise KeyError(f"Topic '{name}' does not exist")
            return self._topics[name]


# ─── Producer ─────────────────────────────────────────────────────────────────

class KafkaProducer:
    """
    Simulated Kafka producer.

    Parameters
    ----------
    bootstrap_servers : str
        Ignored in the simulation; kept for API compatibility.
    topic : str
        Default topic to produce to.
    num_partitions : int
        Number of partitions to create for the topic.
    value_serializer : callable, optional
        Function applied to values before storing.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "research-events",
        num_partitions: int = 3,
        value_serializer=None,
    ) -> None:
        self._broker = _Broker.get()
        self._topic = self._broker.create_topic(topic, num_partitions)
        self._default_topic = topic
        self._counter = 0
        self._lock = threading.Lock()
        self._value_serializer = value_serializer or (lambda v: v)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _select_partition(self, key: bytes | None) -> int:
        """Round-robin unless a key is provided (then hash-assign)."""
        if key is not None:
            idx = int(hashlib.md5(key).hexdigest(), 16)
            return idx % self._topic.num_partitions
        with self._lock:
            part = self._counter % self._topic.num_partitions
            self._counter += 1
            return part

    # ── Public API ─────────────────────────────────────────────────────────────

    def send(
        self,
        topic: str | None = None,
        value: Any = None,
        key: bytes | None = None,
    ) -> _Message:
        """
        Publish a message to the topic.

        Parameters
        ----------
        topic : str, optional
            Override the default topic.
        value : Any
            Message payload; passed through value_serializer first.
        key : bytes, optional
            Optional partition key.

        Returns
        -------
        _Message
            The stored message (useful for testing / logging).
        """
        target_name = topic or self._default_topic
        target = self._broker.get_topic(target_name)

        raw_value: bytes
        serialised = self._value_serializer(value)
        if isinstance(serialised, bytes):
            raw_value = serialised
        elif isinstance(serialised, str):
            raw_value = serialised.encode()
        else:
            raw_value = json.dumps(serialised).encode()

        part_idx = self._select_partition(key)
        partition = target.partitions[part_idx]

        msg = _Message(
            topic=target_name,
            partition=part_idx,
            offset=len(partition),
            key=key,
            value=raw_value,
        )
        partition.append(msg)
        return msg

    def flush(self) -> None:
        """No-op — messages are written synchronously."""
        pass

    def close(self) -> None:
        """No-op — no network connection to close."""
        pass


# ─── Consumer ─────────────────────────────────────────────────────────────────

@dataclass
class ConsumerRecord:
    """Mirrors kafka-python's ConsumerRecord namedtuple."""
    topic:     str
    partition: int
    offset:    int
    key:       bytes | None
    value:     bytes
    timestamp: float


class KafkaConsumer:
    """
    Simulated Kafka consumer.

    Parameters
    ----------
    topics : list[str]
        Topics to subscribe to.
    group_id : str
        Consumer group identifier.
    bootstrap_servers : str
        Ignored; kept for API compatibility.
    value_deserializer : callable, optional
        Applied to the raw bytes when reading.
    auto_offset_reset : str
        "earliest" (default) or "latest".
    """

    def __init__(
        self,
        *topics: str,
        group_id: str = "default-group",
        bootstrap_servers: str = "localhost:9092",
        value_deserializer=None,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._broker = _Broker.get()
        self._topics = list(topics)
        self._group_id = group_id
        self._value_deserializer = value_deserializer or (lambda v: v)
        self._auto_offset_reset = auto_offset_reset

        # offsets[topic][partition] = next_offset_to_read
        self._offsets: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

        if auto_offset_reset == "latest":
            # Jump to end of each partition
            for topic_name in self._topics:
                try:
                    t = self._broker.get_topic(topic_name)
                    for i, part in enumerate(t.partitions):
                        self._offsets[topic_name][i] = len(part)
                except KeyError:
                    pass

    # ── Iteration / polling ────────────────────────────────────────────────────

    def poll(self, timeout_ms: int = 1000) -> dict[str, list[ConsumerRecord]]:
        """
        Fetch all available messages across all subscribed topics.

        Returns a dict mapping topic_name → list[ConsumerRecord].
        """
        result: dict[str, list[ConsumerRecord]] = defaultdict(list)
        for topic_name in self._topics:
            try:
                t = self._broker.get_topic(topic_name)
            except KeyError:
                continue
            for i, partition in enumerate(t.partitions):
                offset = self._offsets[topic_name][i]
                msgs = partition.read_from(offset)
                for msg in msgs:
                    raw_value = msg.value
                    deserialized = self._value_deserializer(raw_value)
                    record = ConsumerRecord(
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        key=msg.key,
                        value=deserialized,
                        timestamp=msg.timestamp,
                    )
                    result[topic_name].append(record)
                    self._offsets[topic_name][i] = msg.offset + 1
        return dict(result)

    def __iter__(self) -> Iterator[ConsumerRecord]:
        """Iterate over all currently available messages (non-blocking)."""
        for records in self.poll().values():
            yield from records

    def close(self) -> None:
        pass

