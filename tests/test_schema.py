"""
tests/test_schema.py — Unit tests for the event schema and validation.
Compatible with both pytest and Python's built-in unittest.
"""

import json
import unittest
from assignment4.schema import ResearchJobEvent, INITIAL_EVENTS


class TestResearchJobEvent(unittest.TestCase):
    """Tests for ResearchJobEvent serialisation and validation."""

    def test_to_json_roundtrip(self):
        evt = INITIAL_EVENTS[0]
        serialised = evt.to_json()
        restored = ResearchJobEvent.from_json(serialised)
        self.assertEqual(restored, evt)

    def test_to_json_is_valid_json(self):
        evt = INITIAL_EVENTS[0]
        data = json.loads(evt.to_json())
        self.assertEqual(data["event_id"], "E001")
        self.assertEqual(data["job_type"], "ml")

    def test_valid_event_passes_validation(self):
        evt = INITIAL_EVENTS[0]
        self.assertTrue(evt.is_valid())
        self.assertEqual(evt.validate(), [])

    def test_all_initial_events_are_valid(self):
        for evt in INITIAL_EVENTS:
            errors = evt.validate()
            self.assertEqual(errors, [],
                             msg=f"{evt.event_id} failed validation: {errors}")

    def test_invalid_job_type(self):
        evt = ResearchJobEvent("X001", "14:00:01", "G01", "bad_type", 10, "completed", 50)
        errors = evt.validate()
        self.assertTrue(any("job_type" in e for e in errors))

    def test_invalid_status(self):
        evt = ResearchJobEvent("X002", "14:00:01", "G01", "ml", 10, "running", 50)
        errors = evt.validate()
        self.assertTrue(any("status" in e for e in errors))

    def test_invalid_cpu_pct_over_100(self):
        evt = ResearchJobEvent("X003", "14:00:01", "G01", "ml", 10, "completed", 150)
        errors = evt.validate()
        self.assertTrue(any("cpu_pct" in e for e in errors))

    def test_invalid_cpu_pct_negative(self):
        evt = ResearchJobEvent("X004", "14:00:01", "G01", "ml", 10, "completed", -5)
        errors = evt.validate()
        self.assertTrue(any("cpu_pct" in e for e in errors))

    def test_invalid_timestamp_format(self):
        evt = ResearchJobEvent("X005", "2:00 PM", "G01", "ml", 10, "completed", 50)
        errors = evt.validate()
        self.assertTrue(any("timestamp" in e for e in errors))

    def test_negative_duration(self):
        evt = ResearchJobEvent("X006", "14:00:01", "G01", "ml", -1, "completed", 50)
        errors = evt.validate()
        self.assertTrue(any("duration_sec" in e for e in errors))


class TestInitialDataset(unittest.TestCase):
    """Tests for the given 10-event dataset."""

    def test_ten_events_present(self):
        self.assertEqual(len(INITIAL_EVENTS), 10)

    def test_event_ids_unique(self):
        ids = [e.event_id for e in INITIAL_EVENTS]
        self.assertEqual(len(ids), len(set(ids)))

    def test_two_failed_events(self):
        failed = [e for e in INITIAL_EVENTS if e.status == "failed"]
        self.assertEqual(len(failed), 2)
        self.assertEqual({e.event_id for e in failed}, {"E004", "E007"})


if __name__ == "__main__":
    unittest.main()
