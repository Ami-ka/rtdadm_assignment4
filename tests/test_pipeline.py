"""
tests/test_pipeline.py — Integration tests for the full pipeline.
Compatible with both pytest and Python's built-in unittest.
"""

import unittest
from assignment4.kafka_broker import _Broker
from assignment4.producer import run_producer
from assignment4.consumer import consume_and_aggregate
from assignment4.windowed import compute_windows
from assignment4.spark_streaming import run_spark_streaming


class TestProducerConsumerIntegration(unittest.TestCase):
    def setUp(self):
        _Broker.reset()

    def tearDown(self):
        _Broker.reset()

    def test_producer_sends_all_10_events(self):
        meta = run_producer(verbose=False)
        self.assertEqual(len(meta), 10)

    def test_consumer_receives_all_events(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        self.assertEqual(len(result["events"]), 10)

    def test_no_invalid_events_in_dataset(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        self.assertEqual(len(result["invalid"]), 0)

    def test_job_type_counts_ml(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        js = result["job_stats"]
        # ml: E001(completed), E005(completed), E007(failed)
        self.assertEqual(js["ml"]["completed"], 2)
        self.assertEqual(js["ml"]["failed"],    1)

    def test_job_type_counts_image(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        js = result["job_stats"]
        # image: E002, E006, E010 all completed
        self.assertEqual(js["image"]["completed"], 3)
        self.assertEqual(js["image"]["failed"],    0)

    def test_job_type_counts_text(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        js = result["job_stats"]
        # text: E003, E008 both completed
        self.assertEqual(js["text"]["completed"], 2)
        self.assertEqual(js["text"]["failed"],    0)

    def test_job_type_counts_simulation(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        js = result["job_stats"]
        # simulation: E004(failed), E009(completed)
        self.assertEqual(js["simulation"]["completed"], 1)
        self.assertEqual(js["simulation"]["failed"],    1)

    def test_avg_duration_ml_values(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        js = result["job_stats"]
        # ml durations: E001=42, E005=51, E007=64
        self.assertEqual(sorted(js["ml"]["durations"]), [42, 51, 64])

    def test_group_g01_cpu_values(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        gs = result["group_stats"]
        # G01: E001(61), E003(38), E006(81), E009(94)
        self.assertEqual(sorted(gs["G01"]["cpu_pcts"]), [38, 61, 81, 94])

    def test_group_g01_avg_cpu(self):
        run_producer(verbose=False)
        result = consume_and_aggregate(verbose=False)
        gs = result["group_stats"]
        avg = sum(gs["G01"]["cpu_pcts"]) / len(gs["G01"]["cpu_pcts"])
        self.assertAlmostEqual(avg, 68.5, places=1)


class TestWindowedProcessing(unittest.TestCase):
    def setUp(self):
        _Broker.reset()

    def tearDown(self):
        _Broker.reset()

    def _get_events(self):
        run_producer(verbose=False)
        return consume_and_aggregate(verbose=False)["events"]

    def test_two_windows_produced(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        self.assertGreaterEqual(len(buckets), 2)

    def test_first_window_has_5_events(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        w = buckets[("14:00:00", "14:00:05")]
        # E001, E002, E003, E004 → 4 events (E005 @ 14:00:05 starts the next window)
        self.assertEqual(w.count, 4)

    def test_first_window_failure_rate(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        w = buckets[("14:00:00", "14:00:05")]
        # E004 is the only failure in window 1 → 1/4 = 0.25
        self.assertAlmostEqual(w.failure_rate, 0.25, places=3)

    def test_second_window_has_5_events(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        w = buckets[("14:00:05", "14:00:10")]
        # E005, E006, E007, E008, E009 → 5 events (E010 @ 14:00:10 starts the next window)
        self.assertEqual(w.count, 5)

    def test_second_window_failure_rate(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        w = buckets[("14:00:05", "14:00:10")]
        # E007 is the only failure in window 2 → 1/5 = 0.20
        self.assertAlmostEqual(w.failure_rate, 0.20, places=3)

    def test_first_window_event_ids(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        w = buckets[("14:00:00", "14:00:05")]
        ids = set(w.event_ids)
        # 14:00:01-04 → E001, E002, E003, E004; E005 @ 14:00:05 is in window 2
        self.assertEqual(ids, {"E001", "E002", "E003", "E004"})

    def test_second_window_event_ids(self):
        events = self._get_events()
        buckets = compute_windows(events, verbose=False)
        w = buckets[("14:00:05", "14:00:10")]
        ids = set(w.event_ids)
        # 14:00:05-09 → E005, E006, E007, E008, E009; E010 @ 14:00:10 is in window 3
        self.assertEqual(ids, {"E005", "E006", "E007", "E008", "E009"})


class TestSparkStreaming(unittest.TestCase):
    def setUp(self):
        _Broker.reset()

    def tearDown(self):
        _Broker.reset()

    def _get_events(self):
        run_producer(verbose=False)
        return consume_and_aggregate(verbose=False)["events"]

    def test_output_has_correct_fields(self):
        events = self._get_events()
        rows = run_spark_streaming(events, verbose=False)
        self.assertGreater(len(rows), 0)
        row = rows[0]
        self.assertTrue(hasattr(row, "window_start"))
        self.assertTrue(hasattr(row, "window_end"))
        self.assertTrue(hasattr(row, "job_type"))
        self.assertTrue(hasattr(row, "count"))
        self.assertTrue(hasattr(row, "avg_duration"))

    def test_total_count_equals_10(self):
        events = self._get_events()
        rows = run_spark_streaming(events, verbose=False)
        self.assertEqual(sum(r.count for r in rows), 10)

    def test_avg_duration_ml_window1(self):
        events = self._get_events()
        rows = run_spark_streaming(events, verbose=False)
        # window 14:00:00-05, ml: E001(42) only — E005 @ 14:00:05 is in window 2
        ml_w1 = [r for r in rows if r.job_type == "ml"
                 and r.window_start == "14:00:00"]
        self.assertEqual(len(ml_w1), 1)
        self.assertAlmostEqual(ml_w1[0].avg_duration, 42.0, places=2)

    def test_sorted_by_window_then_jobtype(self):
        events = self._get_events()
        rows = run_spark_streaming(events, verbose=False)
        keys = [(r.window_start, r.job_type) for r in rows]
        self.assertEqual(keys, sorted(keys))


if __name__ == "__main__":
    unittest.main()
