"""
run_all.py — Master orchestrator for Assignment 4.

Runs all five parts in order:
  Part A — Architecture diagram (ASCII art + component table)
  Part B — Kafka Producer + Consumer
  Part C — Windowed Stream Processing
  Part D — Spark Structured Streaming
  Part E — Throughput and Latency benchmarks

Usage:
    uv run python -m assignment4.run_all
"""

from __future__ import annotations

from .kafka_broker import _Broker
from .producer import run_producer
from .consumer import consume_and_aggregate
from .windowed import compute_windows
from .spark_streaming import run_spark_streaming
from .performance import run_all_benchmarks
from .fmt import print_rule, print_table, print_panel, cprint

# ─── Part A — Architecture ────────────────────────────────────────────────────

ARCH_DIAGRAM = """
           AITU Research Analytics — Kafka Streaming Architecture
           ════════════════════════════════════════════════════════

 ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
 │  Producer 1 │   │  Producer 2 │   │  Producer 3 │  ← research nodes
 │  (G01 jobs) │   │  (G02 jobs) │   │  (G03 jobs) │
 └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │  JSON (keyed by research_group)
                          ▼
           ╔══════════════════════════════════╗
           ║        Kafka Broker              ║
           ║  Topic: research-events          ║
           ║  ┌──────────┬──────────┬───────┐ ║
           ║  │ Part. 0  │ Part. 1  │Part.2 │ ║  ← 3 partitions
           ║  │  (G01)   │  (G02)   │ (G03) │ ║
           ║  └──────────┴──────────┴───────┘ ║
           ╚═══════════════╤══════════════════╝
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
    ┌────────────────────────────────────────┐
    │      Consumer Group: analytics-cg      │
    │  ┌──────────────┐  ┌────────────────┐  │
    │  │  Python      │  │ Window         │  │
    │  │  Processor   │─►│ Processor      │  │
    │  │  (schema val,│  │ (5-sec tumbling│  │
    │  │  aggregation)│  │  event windows)│  │
    │  └──────────────┘  └───────┬────────┘  │
    └──────────────────────────────│──────────┘
                                  │
              ┌───────────────────┤
              │                   │
              ▼                   ▼
 ┌─────────────────┐  ┌─────────────────────┐
 │  Aggregation    │  │  Spark Structured   │
 │  Layer          │  │  Streaming          │
 │  (job counts,   │  │  (window, job_type, │
 │   avg duration, │  │   count,            │
 │   avg CPU)      │  │   avg_duration)     │
 └────────┬────────┘  └─────────────────────┘
          │
          ▼
 ┌─────────────────┐
 │   Monitoring    │
 │  failure rate   │
 │  throughput     │
 │  latency (ms)   │
 │  CPU %          │
 └─────────────────┘
"""

COMPONENT_TABLE_DATA = [
    ("Producer",
     "Converts research-job events to JSON and publishes to Kafka keyed by research_group.",
     "If a producer crashes, remaining producers continue; Kafka persists messages."),
    ("Kafka topic",
     "research-events — durable, ordered log with configurable retention.",
     "Replication factor >= 2 ensures the topic survives a single broker failure."),
    ("Partition",
     "3 partitions keyed by research_group — parallel per-group consumption.",
     "Add partitions to scale throughput; note re-keying may reorder in-flight messages."),
    ("Consumer group",
     "analytics-consumer-group — multiple consumers share partition load.",
     "Kafka rebalances automatically when a consumer joins or leaves the group."),
    ("Processor",
     "Python processor validates schema, deserialises JSON, feeds the aggregation layer.",
     "Schema validation rejects malformed events early; dead-letter queue prevents stalls."),
    ("Monitoring",
     "Tracks failure rate, throughput, average latency, CPU utilisation per window.",
     "High failure rate / latency spikes trigger alert; scale consumers or add partitions."),
]


def print_architecture() -> None:
    print_rule("Part A — Streaming Architecture")
    print_panel(ARCH_DIAGRAM, title="Architecture Diagram")

    print_table(
        title="Component Descriptions",
        headers=["Component", "Purpose", "Failure / scale consideration"],
        rows=[[c, p, s] for c, p, s in COMPONENT_TABLE_DATA],
        col_align=["l", "l", "l"],
    )


# ─── Main orchestrator ────────────────────────────────────────────────────────

def main() -> None:
    print_panel(
        "Assignment 4 — Distributed Stream Processing\n"
        "AITU Research Event Analytics with Kafka and Spark\n"
        "Astana IT University | Real-time Data Analysis | 2026-2027",
        title="Assignment 4",
    )

    # Part A
    print_architecture()

    # Shared broker for Parts B–D
    _Broker.reset()

    # Part B — Producer
    run_producer(verbose=True)

    # Part B — Consumer
    agg_result = consume_and_aggregate(verbose=True)
    events = agg_result["events"]

    # Part C
    compute_windows(events, verbose=True)

    # Part D
    run_spark_streaming(events, verbose=True)

    # Part E
    run_all_benchmarks(verbose=True)

    print_rule("All parts complete")
    cprint("✓ All five assignment parts executed successfully.", "green", "bold")
    print()


if __name__ == "__main__":
    main()
