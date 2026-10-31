"""
schema.py — Event schema definition and validation for Assignment 4.

Defines the ResearchJobEvent dataclass that represents the stream schema:
  event_id, timestamp, research_group, job_type, duration_sec, status, cpu_pct
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Literal


# ─── Valid enum values ────────────────────────────────────────────────────────
VALID_JOB_TYPES = frozenset({"simulation", "ml", "image", "text"})
VALID_STATUSES  = frozenset({"completed", "failed"})


@dataclass
class ResearchJobEvent:
    """Represents a single research-job event in the streaming pipeline."""

    event_id:       str    # Unique event identifier (e.g. "E001")
    timestamp:      str    # Event time as "HH:MM:SS"
    research_group: str    # Group identifier (e.g. "G01")
    job_type:       str    # One of: simulation / ml / image / text
    duration_sec:   int    # Job duration in seconds
    status:         str    # "completed" or "failed"
    cpu_pct:        int    # Worker CPU utilisation 0–100

    # ── Serialisation ──────────────────────────────────────────────────────────

    def to_json(self) -> str:
        """Serialise to a JSON string (used as the Kafka message value)."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "ResearchJobEvent":
        """Deserialise from a JSON string (used by the Kafka consumer)."""
        return cls(**json.loads(raw))

    # ── Schema validation ──────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """
        Validate the event against the schema.

        Returns
        -------
        list[str]
            A list of validation error strings.  Empty list means the event
            is valid.
        """
        errors: list[str] = []

        if not self.event_id:
            errors.append("event_id is empty")

        # timestamp must be parseable as HH:MM:SS
        try:
            datetime.strptime(self.timestamp, "%H:%M:%S")
        except ValueError:
            errors.append(f"timestamp '{self.timestamp}' is not in HH:MM:SS format")

        if not self.research_group:
            errors.append("research_group is empty")

        if self.job_type not in VALID_JOB_TYPES:
            errors.append(
                f"job_type '{self.job_type}' is invalid "
                f"(must be one of {sorted(VALID_JOB_TYPES)})"
            )

        if self.duration_sec < 0:
            errors.append(f"duration_sec {self.duration_sec} must be ≥ 0")

        if self.status not in VALID_STATUSES:
            errors.append(
                f"status '{self.status}' is invalid "
                f"(must be one of {sorted(VALID_STATUSES)})"
            )

        if not (0 <= self.cpu_pct <= 100):
            errors.append(f"cpu_pct {self.cpu_pct} must be in 0–100")

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0


# ─── Initial dataset (from the assignment) ───────────────────────────────────

INITIAL_EVENTS: list[ResearchJobEvent] = [
    ResearchJobEvent("E001", "14:00:01", "G01", "ml",         42,  "completed", 61),
    ResearchJobEvent("E002", "14:00:02", "G02", "image",      75,  "completed", 74),
    ResearchJobEvent("E003", "14:00:03", "G01", "text",       18,  "completed", 38),
    ResearchJobEvent("E004", "14:00:04", "G03", "simulation", 95,  "failed",    92),
    ResearchJobEvent("E005", "14:00:05", "G02", "ml",         51,  "completed", 68),
    ResearchJobEvent("E006", "14:00:06", "G01", "image",      88,  "completed", 81),
    ResearchJobEvent("E007", "14:00:07", "G03", "ml",         64,  "failed",    89),
    ResearchJobEvent("E008", "14:00:08", "G02", "text",       22,  "completed", 44),
    ResearchJobEvent("E009", "14:00:09", "G01", "simulation", 120, "completed", 94),
    ResearchJobEvent("E010", "14:00:10", "G03", "image",      70,  "completed", 78),
]

