"""
consumer.py — Kafka Consumer for Assignment 4 (Part B).

Subscribes to the 'research-events' topic, validates every incoming event
against the schema, then computes:

  1. Count of completed / failed jobs per job_type
  2. Average duration_sec per job_type
  3. Average cpu_pct per research_group
"""

from __future__ import annotations

from collections import defaultdict

from .kafka_broker import KafkaConsumer
from .schema import ResearchJobEvent
from .fmt import print_rule, print_table, cprint

TOPIC_NAME = "research-events"
GROUP_ID   = "analytics-consumer-group"


def _mean(values: list[int | float]) -> float:
    return sum(values) / len(values) if values else 0.0


def consume_and_aggregate(verbose: bool = True) -> dict:
    """
    Read all messages from the topic and build aggregation tables.

    Returns
    -------
    dict with keys:
      "events"       – list[ResearchJobEvent]  (valid events)
      "invalid"      – list[tuple[str, list[str]]]  (raw JSON + errors)
      "job_stats"    – dict[job_type, {"completed", "failed", "durations"}]
      "group_stats"  – dict[group, {"cpu_pcts"}]
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        group_id=GROUP_ID,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda raw: raw.decode("utf-8"),
        auto_offset_reset="earliest",
    )

    events:      list[ResearchJobEvent]        = []
    invalid:     list[tuple[str, list[str]]]   = []
    job_stats:   dict[str, dict]               = defaultdict(
        lambda: {"completed": 0, "failed": 0, "durations": []}
    )
    group_stats: dict[str, dict]               = defaultdict(lambda: {"cpu_pcts": []})

    records_by_topic = consumer.poll()
    all_records = []
    for rec_list in records_by_topic.values():
        all_records.extend(rec_list)

    # Deterministic order: sort by (partition, offset)
    all_records.sort(key=lambda r: (r.partition, r.offset))

    for record in all_records:
        raw_json: str = record.value

        try:
            event = ResearchJobEvent.from_json(raw_json)
        except (KeyError, TypeError, ValueError) as exc:
            invalid.append((raw_json, [f"Deserialisation error: {exc}"]))
            continue

        errors = event.validate()
        if errors:
            invalid.append((raw_json, errors))
            continue

        events.append(event)
        js = job_stats[event.job_type]
        js[event.status] += 1
        js["durations"].append(event.duration_sec)
        group_stats[event.research_group]["cpu_pcts"].append(event.cpu_pct)

    consumer.close()

    result = {
        "events":      events,
        "invalid":     invalid,
        "job_stats":   dict(job_stats),
        "group_stats": dict(group_stats),
    }

    if verbose:
        _print_consumer_report(result)

    return result


def _print_consumer_report(result: dict) -> None:
    events      = result["events"]
    invalid     = result["invalid"]
    job_stats   = result["job_stats"]
    group_stats = result["group_stats"]

    print_rule("Part B — Kafka Consumer")
    cprint(
        f"✓ Consumed {len(events)} valid events, {len(invalid)} invalid",
        "green",
    )

    # ── Job-type table ────────────────────────────────────────────────────────
    job_rows = []
    for jtype in ["ml", "image", "text", "simulation"]:
        js = job_stats.get(jtype, {"completed": 0, "failed": 0, "durations": []})
        avg_dur = _mean(js["durations"])
        job_rows.append([
            jtype,
            str(js.get("completed", 0)),
            str(js.get("failed",    0)),
            f"{avg_dur:.1f} s",
        ])

    print_table(
        title="Job Statistics by Type",
        headers=["Job type", "Completed", "Failed", "Avg duration (s)"],
        rows=job_rows,
        col_align=["l", "c", "c", "r"],
    )

    # ── Group CPU table ────────────────────────────────────────────────────────
    group_rows = [
        [group, f"{_mean(group_stats[group]['cpu_pcts']):.1f}%"]
        for group in sorted(group_stats)
    ]
    print_table(
        title="Average CPU by Research Group",
        headers=["Research group", "Avg CPU %"],
        rows=group_rows,
        col_align=["l", "r"],
    )

    if invalid:
        cprint(f"⚠  {len(invalid)} invalid events:", "red")
        for raw, errs in invalid:
            print(f"  {raw[:60]}… → {errs}")
