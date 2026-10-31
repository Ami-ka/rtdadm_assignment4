"""
producer.py — Kafka Producer for Assignment 4 (Part B).

Reads the 10 initial research-job events and publishes each one as a JSON
message to the 'research-events' Kafka topic.  The topic is partitioned by
research_group so that events from the same group always land on the same
partition (enabling ordered per-group processing downstream).

Partition assignment (hash-based, deterministic):
  G01 → partition 0
  G02 → partition 1
  G03 → partition 2
"""

from __future__ import annotations

from .kafka_broker import KafkaProducer, _Broker
from .schema import INITIAL_EVENTS, ResearchJobEvent
from .fmt import print_rule, print_table, cprint

TOPIC_NAME     = "research-events"
NUM_PARTITIONS = 3


def build_producer() -> KafkaProducer:
    """Create and return a configured KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        topic=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        value_serializer=lambda v: v,   # values are already bytes
    )


def send_events(
    producer: KafkaProducer,
    events: list[ResearchJobEvent],
) -> list[dict]:
    """
    Publish all events to Kafka.

    Each event is keyed by its research_group so that the broker assigns it
    to a deterministic partition.

    Returns
    -------
    list[dict]  – send metadata per event: event_id, topic, partition, offset.
    """
    sent_meta: list[dict] = []
    for event in events:
        key   = event.research_group.encode()
        value = event.to_json().encode()
        msg   = producer.send(value=value, key=key)
        sent_meta.append({
            "event_id":  event.event_id,
            "topic":     msg.topic,
            "partition": msg.partition,
            "offset":    msg.offset,
        })
    producer.flush()
    return sent_meta


def run_producer(verbose: bool = True) -> list[dict]:
    """
    Full producer run: initialise topic, publish events, report results.
    """
    producer = build_producer()
    meta     = send_events(producer, INITIAL_EVENTS)
    producer.close()

    if verbose:
        print_rule("Part B — Kafka Producer")
        cprint(
            f"✓ Published {len(meta)} events to topic '{TOPIC_NAME}'"
            f" ({NUM_PARTITIONS} partitions, keyed by research_group)",
            "green",
        )
        print_table(
            title="Produced Messages",
            headers=["event_id", "partition", "offset"],
            rows=[[m["event_id"], str(m["partition"]), str(m["offset"])] for m in meta],
            col_align=["l", "c", "c"],
        )

    return meta
