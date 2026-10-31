"""
tests/test_broker.py — Unit tests for the in-process Kafka broker simulation.
Compatible with both pytest and Python's built-in unittest.
"""

import unittest
from assignment4.kafka_broker import KafkaProducer, KafkaConsumer, _Broker


class TestKafkaProducer(unittest.TestCase):
    def setUp(self):
        _Broker.reset()

    def tearDown(self):
        _Broker.reset()

    def test_send_single_message(self):
        p = KafkaProducer(topic="t1", num_partitions=3)
        msg = p.send(value=b"hello")
        self.assertEqual(msg.topic, "t1")
        self.assertEqual(msg.offset, 0)
        self.assertEqual(msg.value, b"hello")

    def test_send_multiple_messages_increments_offset(self):
        p = KafkaProducer(topic="t1", num_partitions=1)
        for i in range(5):
            p.send(value=f"msg{i}".encode())
        broker = _Broker.get()
        topic = broker.get_topic("t1")
        self.assertEqual(len(topic.partitions[0]), 5)

    def test_key_based_partition_assignment_is_deterministic(self):
        p = KafkaProducer(topic="t1", num_partitions=3)
        m1 = p.send(value=b"v", key=b"G01")
        m2 = p.send(value=b"v", key=b"G01")
        self.assertEqual(m1.partition, m2.partition)

    def test_different_keys_may_land_on_different_partitions(self):
        p = KafkaProducer(topic="t1", num_partitions=3)
        partitions = set()
        for key in [b"G01", b"G02", b"G03"]:
            msg = p.send(value=b"x", key=key)
            partitions.add(msg.partition)
        self.assertGreaterEqual(len(partitions), 1)


class TestKafkaConsumer(unittest.TestCase):
    def setUp(self):
        _Broker.reset()

    def tearDown(self):
        _Broker.reset()

    def test_consume_all_messages(self):
        topic = "t2"
        p = KafkaProducer(topic=topic, num_partitions=1)
        for i in range(5):
            p.send(value=f"msg{i}".encode())
        c = KafkaConsumer(topic, group_id="g1", auto_offset_reset="earliest")
        records = list(c)
        self.assertEqual(len(records), 5)

    def test_consumer_deserializer_applied(self):
        topic = "t3"
        p = KafkaProducer(topic=topic, num_partitions=1)
        p.send(value=b"hello")
        c = KafkaConsumer(
            topic, group_id="g1",
            value_deserializer=lambda b: b.decode("utf-8"),
            auto_offset_reset="earliest",
        )
        records = list(c)
        self.assertEqual(records[0].value, "hello")

    def test_consumer_does_not_replay_messages(self):
        topic = "t4"
        p = KafkaProducer(topic=topic, num_partitions=1)
        p.send(value=b"msg1")
        c = KafkaConsumer(topic, group_id="g1", auto_offset_reset="earliest")
        first  = list(c)
        second = list(c)
        self.assertEqual(len(first),  1)
        self.assertEqual(len(second), 0)

    def test_latest_offset_skips_existing_messages(self):
        topic = "t5"
        p = KafkaProducer(topic=topic, num_partitions=1)
        p.send(value=b"old-msg")
        c = KafkaConsumer(topic, group_id="g2", auto_offset_reset="latest")
        self.assertEqual(list(c), [])

    def test_poll_returns_dict_by_topic(self):
        topic = "t6"
        p = KafkaProducer(topic=topic, num_partitions=1)
        p.send(value=b"data")
        c = KafkaConsumer(topic, group_id="g1", auto_offset_reset="earliest")
        result = c.poll()
        self.assertIn(topic, result)
        self.assertEqual(len(result[topic]), 1)


if __name__ == "__main__":
    unittest.main()
