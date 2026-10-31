"""
windowed.py — Windowed Stream Processing for Assignment 4 (Part C).

Implements a 5-second event-time tumbling window over the consumed events.
For each window the processor calculates:
  • event count
  • failure rate  (failed / total)
  • average CPU utilisation
"""

from __future__ import annotations

from collections import defaultdict

from .schema import ResearchJobEvent
from .fmt import print_rule, print_table, cprint

WINDOW_SIZE_SEC = 5


def _to_seconds(ts: str) -> int:
    """Convert 'HH:MM:SS' to total seconds since midnight."""
    h, m, s = map(int, ts.split(":"))
    return h * 3600 + m * 60 + s


def _window_key(ts: str) -> tuple[str, str]:
    """Return the (start, end) window label for an event timestamp."""
    secs = _to_seconds(ts)
    ws   = (secs // WINDOW_SIZE_SEC) * WINDOW_SIZE_SEC
    we   = ws + WINDOW_SIZE_SEC

    def fmt(s: int) -> str:
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    return fmt(ws), fmt(we)


class _WindowBucket:
    def __init__(self) -> None:
        self.events: list[ResearchJobEvent] = []

    def add(self, event: ResearchJobEvent) -> None:
        self.events.append(event)

    @property
    def count(self) -> int:
        return len(self.events)

    @property
    def failure_rate(self) -> float:
        if not self.events:
            return 0.0
        return sum(1 for e in self.events if e.status == "failed") / self.count

    @property
    def avg_cpu(self) -> float:
        if not self.events:
            return 0.0
        return sum(e.cpu_pct for e in self.events) / self.count

    @property
    def event_ids(self) -> list[str]:
        return [e.event_id for e in self.events]


def compute_windows(
    events: list[ResearchJobEvent],
    verbose: bool = True,
) -> dict[tuple[str, str], _WindowBucket]:
    """
    Assign events to 5-second tumbling windows and compute statistics.
    """
    buckets: dict[tuple[str, str], _WindowBucket] = defaultdict(_WindowBucket)
    for event in events:
        key = _window_key(event.timestamp)
        buckets[key].add(event)

    if verbose:
        _print_window_report(buckets)

    return dict(buckets)


def _print_window_report(buckets: dict[tuple[str, str], _WindowBucket]) -> None:
    print_rule("Part C — Windowed Stream Processing (5-second tumbling windows)")

    rows = []
    for (start, end) in sorted(buckets.keys()):
        b = buckets[(start, end)]
        rows.append([
            f"{start}–{end}",
            ", ".join(b.event_ids),
            str(b.count),
            f"{b.failure_rate * 100:.1f}%",
            f"{b.avg_cpu:.1f}%",
        ])

    print_table(
        title="Window Results",
        headers=["Window", "Events", "Count", "Failure rate", "Avg CPU %"],
        rows=rows,
        col_align=["l", "l", "c", "r", "r"],
    )

    # Detailed breakdown
    print("  Event-level detail per window:")
    for (start, end) in sorted(buckets.keys()):
        b = buckets[(start, end)]
        cprint(f"\n  Window {start}–{end}", "yellow", "bold")
        for ev in b.events:
            color = "green" if ev.status == "completed" else "red"
            cprint(
                f"    {ev.event_id:5s}  {ev.job_type:10s}  {ev.status:9s}"
                f"  dur={ev.duration_sec:3d}s  cpu={ev.cpu_pct:2d}%"
                f"  group={ev.research_group}",
                color,
            )
    print()
