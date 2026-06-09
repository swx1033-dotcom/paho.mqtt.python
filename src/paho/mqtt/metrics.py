"""
Prometheus metrics collection for paho-mqtt client.

This module provides Prometheus metrics for monitoring MQTT client operations,
including message throughput, reconnection counts, message latency, and more.
"""
from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from prometheus_client import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from typing import Optional


class MQTTMetrics:
    """Prometheus metrics collector for MQTT client operations."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls) -> MQTTMetrics:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        self._initialized = True
        self._setup_metrics()

    def _setup_metrics(self) -> None:
        # Connection metrics
        self.connections_total = Counter(
            'mqtt_connections_total',
            'Total number of MQTT connections',
            ['status']  # success, failed
        )

        self.reconnections_total = Counter(
            'mqtt_reconnections_total',
            'Total number of MQTT reconnection attempts'
        )

        self.connection_duration_seconds = Histogram(
            'mqtt_connection_duration_seconds',
            'Duration of MQTT connections',
            buckets=(60, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400, float('inf'))
        )

        # Message publish metrics
        self.messages_published_total = Counter(
            'mqtt_messages_published_total',
            'Total number of messages published',
            ['qos', 'topic']
        )

        self.messages_publish_errors_total = Counter(
            'mqtt_messages_publish_errors_total',
            'Total number of message publish errors',
            ['error_type']
        )

        self.publish_latency_seconds = Histogram(
            'mqtt_publish_latency_seconds',
            'Latency for message publishing (from publish to ack)',
            ['qos'],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))
        )

        # Message receive metrics
        self.messages_received_total = Counter(
            'mqtt_messages_received_total',
            'Total number of messages received',
            ['qos', 'topic']
        )

        self.messages_received_bytes = Counter(
            'mqtt_messages_received_bytes_total',
            'Total bytes of messages received',
            ['topic']
        )

        # Message acknowledgment metrics
        self.messages_acknowledged_total = Counter(
            'mqtt_messages_acknowledged_total',
            'Total number of messages acknowledged',
            ['qos', 'ack_type']  # puback, pubrec, pubrel, pubcomp
        )

        # Queue metrics
        self.messages_inflight = Gauge(
            'mqtt_messages_inflight',
            'Current number of inflight messages'
        )

        self.messages_queued = Gauge(
            'mqtt_messages_queued',
            'Current number of queued messages'
        )

        # Network metrics
        self.bytes_sent_total = Counter(
            'mqtt_bytes_sent_total',
            'Total bytes sent over MQTT connection'
        )

        self.bytes_received_total = Counter(
            'mqtt_bytes_received_total',
            'Total bytes received over MQTT connection'
        )

        # Disconnection metrics
        self.disconnections_total = Counter(
            'mqtt_disconnections_total',
            'Total number of disconnections',
            ['reason']
        )

        # Subscription metrics
        self.subscriptions_total = Counter(
            'mqtt_subscriptions_total',
            'Total number of subscriptions',
            ['status']  # success, failed
        )

        self.subscriptions_active = Gauge(
            'mqtt_subscriptions_active',
            'Current number of active subscriptions'
        )

    def record_connection(self, success: bool) -> None:
        """Record a connection attempt."""
        status = 'success' if success else 'failed'
        self.connections_total.labels(status=status).inc()

    def record_reconnection(self) -> None:
        """Record a reconnection attempt."""
        self.reconnections_total.inc()

    def record_message_published(self, topic: str, qos: int) -> None:
        """Record a message publish."""
        self.messages_published_total.labels(qos=str(qos), topic=topic).inc()

    def record_message_publish_error(self, error_type: str) -> None:
        """Record a message publish error."""
        self.messages_publish_errors_total.labels(error_type=error_type).inc()

    def record_publish_latency(self, qos: int, latency: float) -> None:
        """Record message publish latency."""
        self.publish_latency_seconds.labels(qos=str(qos)).observe(latency)

    def record_message_received(self, topic: str, qos: int, payload_size: int) -> None:
        """Record a message receive."""
        self.messages_received_total.labels(qos=str(qos), topic=topic).inc()
        self.messages_received_bytes.labels(topic=topic).inc(payload_size)

    def record_message_acknowledged(self, qos: int, ack_type: str) -> None:
        """Record a message acknowledgment."""
        self.messages_acknowledged_total.labels(qos=str(qos), ack_type=ack_type).inc()

    def set_inflight_messages(self, count: int) -> None:
        """Set the current number of inflight messages."""
        self.messages_inflight.set(count)

    def set_queued_messages(self, count: int) -> None:
        """Set the current number of queued messages."""
        self.messages_queued.set(count)

    def record_bytes_sent(self, byte_count: int) -> None:
        """Record bytes sent."""
        self.bytes_sent_total.inc(byte_count)

    def record_bytes_received(self, byte_count: int) -> None:
        """Record bytes received."""
        self.bytes_received_total.inc(byte_count)

    def record_disconnection(self, reason: str) -> None:
        """Record a disconnection."""
        self.disconnections_total.labels(reason=reason).inc()

    def record_subscription(self, success: bool) -> None:
        """Record a subscription attempt."""
        status = 'success' if success else 'failed'
        self.subscriptions_total.labels(status=status).inc()

    def set_active_subscriptions(self, count: int) -> None:
        """Set the current number of active subscriptions."""
        self.subscriptions_active.set(count)


def get_metrics() -> MQTTMetrics:
    """Get the singleton MQTTMetrics instance."""
    return MQTTMetrics()
