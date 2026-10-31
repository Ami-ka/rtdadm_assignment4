# Assignment 4 — Distributed Stream Processing
## AITU Research Event Analytics with Kafka and Spark

**Astana IT University | Real-time Data Analysis | 2026–2027**

---

## Project Structure

```
assignment4/
├── pyproject.toml
├── README.md
└── src/
    └── assignment4/
        ├── __init__.py
        ├── fmt.py              # stdlib-only terminal formatting (tables, panels)
        ├── schema.py           # ResearchJobEvent dataclass + validation + dataset
        ├── kafka_broker.py     # In-process Kafka broker simulation
        ├── producer.py         # Part B — Kafka Producer
        ├── consumer.py         # Part B — Kafka Consumer + aggregations
        ├── windowed.py         # Part C — 5-second tumbling window processor
        ├── spark_streaming.py  # Part D — Spark Structured Streaming simulation
        ├── performance.py      # Part E — Throughput & latency benchmarks
        └── run_all.py          # Master orchestrator (runs all 5 parts)
tests/
    ├── test_schema.py
    ├── test_broker.py
    └── test_pipeline.py
```

---

## How to Run

> **Requirements:** Python 3.11+, [uv](https://docs.astral.sh/uv/) package manager

### Run all five parts at once

```bash
cd path/to/assignment4
PYTHONPATH=src uv run --no-project python3 -m assignment4.run_all
```

### Run individual parts

```bash
# Part B only (producer + consumer)
PYTHONPATH=src uv run --no-project python3 -c "
from assignment4.kafka_broker import _Broker
from assignment4.producer import run_producer
from assignment4.consumer import consume_and_aggregate
_Broker.reset()
run_producer()
consume_and_aggregate()
"

# Part C only (windowed)
PYTHONPATH=src uv run --no-project python3 -c "
from assignment4.kafka_broker import _Broker
from assignment4.producer import run_producer
from assignment4.consumer import consume_and_aggregate
from assignment4.windowed import compute_windows
_Broker.reset()
run_producer(verbose=False)
events = consume_and_aggregate(verbose=False)['events']
compute_windows(events)
"
```

### Run tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

---

## Parts Summary

### Part A — Streaming Architecture
- Three producers (one per research group G01, G02, G03)
- Kafka topic `research-events` with **3 partitions** (keyed by `research_group`)
- Consumer group with Python processor → aggregation layer → windowed processor
- Spark Structured Streaming for batch-equivalent analytics
- Monitoring layer: failure rate, throughput, latency, CPU %

### Part B — Kafka Producer / Consumer

| Job type   | Completed | Failed | Avg duration |
|------------|:---------:|:------:|:------------:|
| ml         | 2         | 1      | 52.3 s       |
| image      | 3         | 0      | 73.3 s       |
| text       | 2         | 0      | 20.0 s       |
| simulation | 1         | 1      | 107.5 s      |

| Research group | Avg CPU % |
|:--------------:|:---------:|
| G01            | 68.5%     |
| G02            | 62.0%     |
| G03            | 86.3%     |

### Part C — Windowed Stream Processing (5-second tumbling windows)

| Window              | Events                     | Count | Failure rate | Avg CPU |
|---------------------|----------------------------|:-----:|:------------:|:-------:|
| 14:00:00–14:00:05   | E001, E002, E003, E004     | 4     | 25.0%        | ~66.3%  |
| 14:00:05–14:00:10   | E005, E006, E007, E008, E009 | 5   | 20.0%        | ~75.2%  |
| 14:00:10–14:00:15   | E010                       | 1     | 0.0%         | 78.0%   |

> **Window boundary note:** Timestamp `14:00:05` (E005) is the **start** of the
> second window `[14:00:05, 14:00:10)` using standard half-open interval semantics
> (same as Apache Spark / Flink). Timestamp `14:00:10` (E010) starts window 3.

### Part D — Spark Structured Streaming
The `spark_streaming.py` module implements the identical transformation in pure
Python stdlib, and prints the exact equivalent PySpark code that would run
against a real Kafka + Spark cluster (Java 11+ required).

**Output schema:**

| Field        | Type              | Transformation                       |
|--------------|-------------------|--------------------------------------|
| window       | struct{start,end} | `window(event_time, '5 seconds')`    |
| job_type     | string            | groupBy key                          |
| count        | long              | `count(*)` per (window, job_type)    |
| avg_duration | double            | `avg(duration_sec)`                  |

### Part E — Throughput & Latency

| Input rate (ev/s) | Observed throughput | Avg latency | CPU %  | Bottleneck                              |
|:-----------------:|:-------------------:|:-----------:|:------:|------------------------------------------|
| 10                | ~90K ev/s           | ~0.01 ms    | ~100%  | Fixed overhead; broker under-utilised   |
| 100               | ~140K ev/s          | ~0.006 ms   | ~100%  | JSON serialisation cost                 |
| 500               | ~190K ev/s          | ~0.005 ms   | ~100%  | Python GIL + queue contention           |

**Proposed optimisation:** Replace JSON with Apache Avro or MessagePack (binary
serialisation), reducing per-message CPU cost by ~60–70% and halving latency at
high throughput.

---

## Design Notes

### Why an in-process broker?
A real Kafka cluster requires Java 11+, ZooKeeper (or KRaft), and Docker or a
running JVM. Since the assignment is evaluated in a pure Python environment, the
`kafka_broker.py` module provides an in-process simulation with the same public
API as `kafka-python`, so the producer and consumer code is identical to what
would run against a real cluster.

### Swapping in real Kafka
Replace the import in `producer.py` / `consumer.py`:

```python
# Change:
from .kafka_broker import KafkaProducer, KafkaConsumer

# To (requires: pip install kafka-python, running Kafka broker):
from kafka import KafkaProducer, KafkaConsumer
```

The rest of the code is unchanged.

### Individual design decision
Events are partitioned by `research_group` (not round-robin) so that all events
from the same group are consumed **in order** by a single consumer instance.
This avoids out-of-order aggregation without requiring a windowing watermark.
