"""
spark_streaming.py — Spark Structured Streaming simulation for Assignment 4 (Part D).

Implements the same window + groupBy + agg transformation that PySpark would
execute on a real cluster, but entirely in Python stdlib so it runs without
Java or Spark installed.

Output schema (mirrors Spark Structured Streaming):
    window        | job_type | count | avg_duration
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from .schema import ResearchJobEvent
from .windowed import _window_key
from .fmt import print_rule, print_table, cprint

PYSPARK_CODE = '''\
# ── Equivalent PySpark Structured Streaming code ──────────────────────────────
# Requires: Spark ≥ 3.3, Java 11+, kafka-clients JAR on the classpath

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("AITU-Research-Analytics")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# 1. Read raw bytes from Kafka ─────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "research-events")
    .option("startingOffsets", "earliest")
    .load()
)

# 2. Parse JSON payload ────────────────────────────────────────────────────────
event_schema = StructType([
    StructField("event_id",       StringType(),  False),
    StructField("timestamp",      StringType(),  False),
    StructField("research_group", StringType(),  False),
    StructField("job_type",       StringType(),  False),
    StructField("duration_sec",   IntegerType(), False),
    StructField("status",         StringType(),  False),
    StructField("cpu_pct",        IntegerType(), False),
])

events = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
    .select("e.*")
    .withColumn(
        "event_time",
        F.to_timestamp(
            F.concat(F.lit("1970-01-01 "), F.col("timestamp")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
)

# 3. Windowed aggregation ──────────────────────────────────────────────────────
windowed = (
    events
    .withWatermark("event_time", "0 seconds")
    .groupBy(
        F.window("event_time", "5 seconds"),
        F.col("job_type"),
    )
    .agg(
        F.count("*").alias("count"),
        F.avg("duration_sec").alias("avg_duration"),
    )
    .select(
        F.col("window"),
        F.col("job_type"),
        F.col("count"),
        F.round("avg_duration", 1).alias("avg_duration"),
    )
)

# 4. Write to console ─────────────────────────────────────────────────────────
query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
'''


@dataclass
class SparkRow:
    """One output row from the Spark structured streaming query."""
    window_start:  str
    window_end:    str
    job_type:      str
    count:         int
    avg_duration:  float


def run_spark_streaming(
    events: list[ResearchJobEvent],
    verbose: bool = True,
) -> list[SparkRow]:
    """
    Simulate Spark Structured Streaming window aggregation.

    Steps
    -----
    1. Parse events (equivalent to from_json() in Spark).
    2. Assign each event to a 5-second tumbling window on event_time.
    3. Group by (window, job_type).
    4. Aggregate: count(), avg(duration_sec).
    """
    buckets: dict[tuple[str, str, str], list[int]] = defaultdict(list)

    for event in events:
        win_start, win_end = _window_key(event.timestamp)
        buckets[(win_start, win_end, event.job_type)].append(event.duration_sec)

    rows: list[SparkRow] = [
        SparkRow(
            window_start = ws,
            window_end   = we,
            job_type     = jt,
            count        = len(durs),
            avg_duration = sum(durs) / len(durs),
        )
        for (ws, we, jt), durs in buckets.items()
    ]
    rows.sort(key=lambda r: (r.window_start, r.job_type))

    if verbose:
        _print_spark_report(rows)

    return rows


def _print_spark_report(rows: list[SparkRow]) -> None:
    print_rule("Part D — Spark Structured Streaming")

    # Output table
    print_table(
        title="Spark Structured Streaming Output",
        headers=["window", "job_type", "count", "avg_duration"],
        rows=[
            [f"{r.window_start}–{r.window_end}", r.job_type,
             str(r.count), f"{r.avg_duration:.1f} s"]
            for r in rows
        ],
        col_align=["l", "l", "c", "r"],
    )

    # Output schema
    print_table(
        title="Output Field Schema",
        headers=["Output field", "Type", "Transformation"],
        rows=[
            ["window",       "struct{start, end}", "window(event_time, '5 seconds')"],
            ["job_type",     "string",             "groupBy key — passthrough"],
            ["count",        "long",               "count() — events per (window, job_type)"],
            ["avg_duration", "double",             "avg(duration_sec) — mean job duration"],
        ],
        col_align=["l", "l", "l"],
    )

    # PySpark code reference
    cprint("\n  Equivalent PySpark Structured Streaming code:", "bold")
    cprint("  (Runs on a real Spark + Kafka cluster with Java 11+)\n", "dim")
    for i, line in enumerate(PYSPARK_CODE.splitlines(), 1):
        print(f"  {i:3d} │ {line}")
    print()
