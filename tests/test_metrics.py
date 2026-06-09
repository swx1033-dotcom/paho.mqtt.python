"""Tests for MQTT Prometheus metrics collection."""
import unittest
from prometheus_client import REGISTRY
from paho.mqtt.metrics import MQTTMetrics, get_metrics


class TestMQTTMetrics(unittest.TestCase):
    """Test cases for MQTT metrics collection."""

    def setUp(self):
        """Set up test fixtures."""
        self.metrics = get_metrics()

    def test_singleton_pattern(self):
        """Test that MQTTMetrics follows singleton pattern."""
        metrics1 = MQTTMetrics()
        metrics2 = MQTTMetrics()
        self.assertIs(metrics1, metrics2)

    def test_get_metrics_returns_singleton(self):
        """Test that get_metrics returns the singleton instance."""
        metrics1 = get_metrics()
        metrics2 = get_metrics()
        self.assertIs(metrics1, metrics2)

    def test_record_connection_success(self):
        """Test recording successful connection."""
        initial_count = REGISTRY.get_sample_value(
            'mqtt_connections_total', {'status': 'success'}
        ) or 0
        self.metrics.record_connection(success=True)
        new_count = REGISTRY.get_sample_value(
            'mqtt_connections_total', {'status': 'success'}
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_record_connection_failure(self):
        """Test recording failed connection."""
        initial_count = REGISTRY.get_sample_value(
            'mqtt_connections_total', {'status': 'failed'}
        ) or 0
        self.metrics.record_connection(success=False)
        new_count = REGISTRY.get_sample_value(
            'mqtt_connections_total', {'status': 'failed'}
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_record_reconnection(self):
        """Test recording reconnection."""
        initial_count = REGISTRY.get_sample_value(
            'mqtt_reconnections_total'
        ) or 0
        self.metrics.record_reconnection()
        new_count = REGISTRY.get_sample_value(
            'mqtt_reconnections_total'
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_record_message_published(self):
        """Test recording message publish."""
        topic = "test/topic"
        qos = 1
        initial_count = REGISTRY.get_sample_value(
            'mqtt_messages_published_total', {'qos': str(qos), 'topic': topic}
        ) or 0
        self.metrics.record_message_published(topic, qos)
        new_count = REGISTRY.get_sample_value(
            'mqtt_messages_published_total', {'qos': str(qos), 'topic': topic}
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_record_message_publish_error(self):
        """Test recording message publish error."""
        error_type = "no_connection"
        initial_count = REGISTRY.get_sample_value(
            'mqtt_messages_publish_errors_total', {'error_type': error_type}
        ) or 0
        self.metrics.record_message_publish_error(error_type)
        new_count = REGISTRY.get_sample_value(
            'mqtt_messages_publish_errors_total', {'error_type': error_type}
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_record_message_received(self):
        """Test recording message receive."""
        topic = "test/topic"
        qos = 1
        payload_size = 100
        initial_count = REGISTRY.get_sample_value(
            'mqtt_messages_received_total', {'qos': str(qos), 'topic': topic}
        ) or 0
        initial_bytes = REGISTRY.get_sample_value(
            'mqtt_messages_received_bytes_total', {'topic': topic}
        ) or 0
        self.metrics.record_message_received(topic, qos, payload_size)
        new_count = REGISTRY.get_sample_value(
            'mqtt_messages_received_total', {'qos': str(qos), 'topic': topic}
        )
        new_bytes = REGISTRY.get_sample_value(
            'mqtt_messages_received_bytes_total', {'topic': topic}
        )
        self.assertEqual(new_count, initial_count + 1)
        self.assertEqual(new_bytes, initial_bytes + payload_size)

    def test_record_message_acknowledged(self):
        """Test recording message acknowledgment."""
        qos = 1
        ack_type = "puback"
        initial_count = REGISTRY.get_sample_value(
            'mqtt_messages_acknowledged_total', {'qos': str(qos), 'ack_type': ack_type}
        ) or 0
        self.metrics.record_message_acknowledged(qos, ack_type)
        new_count = REGISTRY.get_sample_value(
            'mqtt_messages_acknowledged_total', {'qos': str(qos), 'ack_type': ack_type}
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_set_inflight_messages(self):
        """Test setting inflight messages gauge."""
        count = 5
        self.metrics.set_inflight_messages(count)
        value = REGISTRY.get_sample_value('mqtt_messages_inflight')
        self.assertEqual(value, count)

    def test_set_queued_messages(self):
        """Test setting queued messages gauge."""
        count = 10
        self.metrics.set_queued_messages(count)
        value = REGISTRY.get_sample_value('mqtt_messages_queued')
        self.assertEqual(value, count)

    def test_record_bytes_sent(self):
        """Test recording bytes sent."""
        byte_count = 1024
        initial_count = REGISTRY.get_sample_value(
            'mqtt_bytes_sent_total'
        ) or 0
        self.metrics.record_bytes_sent(byte_count)
        new_count = REGISTRY.get_sample_value(
            'mqtt_bytes_sent_total'
        )
        self.assertEqual(new_count, initial_count + byte_count)

    def test_record_bytes_received(self):
        """Test recording bytes received."""
        byte_count = 2048
        initial_count = REGISTRY.get_sample_value(
            'mqtt_bytes_received_total'
        ) or 0
        self.metrics.record_bytes_received(byte_count)
        new_count = REGISTRY.get_sample_value(
            'mqtt_bytes_received_total'
        )
        self.assertEqual(new_count, initial_count + byte_count)

    def test_record_disconnection(self):
        """Test recording disconnection."""
        reason = "Normal disconnection"
        initial_count = REGISTRY.get_sample_value(
            'mqtt_disconnections_total', {'reason': reason}
        ) or 0
        self.metrics.record_disconnection(reason=reason)
        new_count = REGISTRY.get_sample_value(
            'mqtt_disconnections_total', {'reason': reason}
        )
        self.assertEqual(new_count, initial_count + 1)

    def test_record_subscription(self):
        """Test recording subscription."""
        initial_success = REGISTRY.get_sample_value(
            'mqtt_subscriptions_total', {'status': 'success'}
        ) or 0
        initial_failed = REGISTRY.get_sample_value(
            'mqtt_subscriptions_total', {'status': 'failed'}
        ) or 0
        self.metrics.record_subscription(success=True)
        self.metrics.record_subscription(success=False)
        new_success = REGISTRY.get_sample_value(
            'mqtt_subscriptions_total', {'status': 'success'}
        )
        new_failed = REGISTRY.get_sample_value(
            'mqtt_subscriptions_total', {'status': 'failed'}
        )
        self.assertEqual(new_success, initial_success + 1)
        self.assertEqual(new_failed, initial_failed + 1)

    def test_set_active_subscriptions(self):
        """Test setting active subscriptions gauge."""
        count = 3
        self.metrics.set_active_subscriptions(count)
        value = REGISTRY.get_sample_value('mqtt_subscriptions_active')
        self.assertEqual(value, count)

    def test_record_publish_latency(self):
        """Test recording publish latency."""
        qos = 1
        latency = 0.5
        self.metrics.record_publish_latency(qos, latency)
        count = REGISTRY.get_sample_value(
            'mqtt_publish_latency_seconds_count', {'qos': str(qos)}
        )
        self.assertIsNotNone(count)
        self.assertGreater(count, 0)


if __name__ == '__main__':
    unittest.main()
