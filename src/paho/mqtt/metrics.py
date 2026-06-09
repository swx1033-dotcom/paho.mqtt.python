"""
Prometheus metrics instrumentation for paho-mqtt.

This module provides optional Prometheus metrics for monitoring MQTT client
operations. To enable metrics, install the ``prometheus`` extra::

    pip install paho-mqtt[prometheus]

All metrics are registered at module import time and are safe to use across
multiple Client instances (labels allow distinguishing between clients).

A convenience function ``start_metrics_server()`` is provided to expose
metrics via an HTTP endpoint for Prometheus scraping.
"""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

_log = logging.getLogger(__name__)

try:
    import prometheus_client
    from prometheus_client import Counter, Gauge, Histogram
    from prometheus_client.metrics import MetricWrapperBase

    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    MetricWrapperBase = type("MetricWrapperBase", (), {})  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from collections.abc import Callable


_LABEL_NAMES = ("client_id",)


class _NoopMetric:
    """No-op metric that silently discards all calls when prometheus_client is not installed."""

    def labels(self, *args: object, **kwargs: object) -> "_NoopMetric":
        return self

    def inc(self, amount: float = 1, **kwargs: object) -> None:
        pass

    def dec(self, amount: float = 1, **kwargs: object) -> None:
        pass

    def set(self, value: float, **kwargs: object) -> None:
        pass

    def observe(self, amount: float, **kwargs: object) -> None:
        pass

    def time(self) -> "_NoopTimer":
        return _NoopTimer()


class _NoopTimer:
    def __enter__(self) -> "_NoopTimer":
        return self

    def __exit__(self, *args: object) -> None:
        pass


if HAS_PROMETHEUS:
    MESSAGES_PUBLISHED = Counter(
        "paho_mqtt_messages_published_total",
        "Total number of messages published",
        ("client_id", "qos"),
    )

    MESSAGES_RECEIVED = Counter(
        "paho_mqtt_messages_received_total",
        "Total number of messages received",
        ("client_id", "qos"),
    )

    MESSAGES_PUBLISH_ACK = Counter(
        "paho_mqtt_messages_publish_ack_total",
        "Total number of publish acknowledgments received",
        ("client_id", "qos"),
    )

    BYTES_SENT = Counter(
        "paho_mqtt_bytes_sent_total",
        "Total bytes sent over the network",
        ("client_id",),
    )

    BYTES_RECEIVED = Counter(
        "paho_mqtt_bytes_received_total",
        "Total bytes received over the network",
        ("client_id",),
    )

    CONNECTIONS_TOTAL = Counter(
        "paho_mqtt_connections_total",
        "Total connection attempts",
        ("client_id", "result"),
    )

    RECONNECTS_TOTAL = Counter(
        "paho_mqtt_reconnects_total",
        "Total reconnection attempts",
        ("client_id",),
    )

    CONNECTION_LOST_TOTAL = Counter(
        "paho_mqtt_connection_lost_total",
        "Total number of times the connection was lost",
        ("client_id",),
    )

    CONNECTION_STATUS = Gauge(
        "paho_mqtt_connection_status",
        "Current connection status (1=connected, 0=disconnected)",
        ("client_id",),
    )

    PUBLISH_LATENCY = Histogram(
        "paho_mqtt_message_publish_latency_seconds",
        "Latency from publish to acknowledgment",
        ("client_id", "qos"),
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    )

    INFLIGHT_MESSAGES = Gauge(
        "paho_mqtt_inflight_messages",
        "Current number of in-flight (unacknowledged) messages",
        ("client_id",),
    )

    OUTGOING_QUEUE_SIZE = Gauge(
        "paho_mqtt_outgoing_queue_size",
        "Current number of queued outgoing messages",
        ("client_id",),
    )

else:
    MESSAGES_PUBLISHED = _NoopMetric()
    MESSAGES_RECEIVED = _NoopMetric()
    MESSAGES_PUBLISH_ACK = _NoopMetric()
    BYTES_SENT = _NoopMetric()
    BYTES_RECEIVED = _NoopMetric()
    CONNECTIONS_TOTAL = _NoopMetric()
    RECONNECTS_TOTAL = _NoopMetric()
    CONNECTION_LOST_TOTAL = _NoopMetric()
    CONNECTION_STATUS = _NoopMetric()
    PUBLISH_LATENCY = _NoopMetric()
    INFLIGHT_MESSAGES = _NoopMetric()
    OUTGOING_QUEUE_SIZE = _NoopMetric()


class PahoMetrics:
    """Per-client metrics collector.

    Each MQTT Client instance creates a ``PahoMetrics`` to record its own
    metrics.  If prometheus_client is not installed, all operations are no-ops.
    """

    def __init__(self, client_id: str = "") -> None:
        self._client_id = client_id or ""
        self._publish_start_times: dict[int, float] = {}

    @property
    def client_id(self) -> str:
        return self._client_id

    @client_id.setter
    def client_id(self, value: str) -> None:
        self._client_id = value or ""

    def _labels(self, **extra: str) -> dict[str, str]:
        labels: dict[str, str] = {"client_id": self._client_id}
        labels.update(extra)
        return labels

    def record_publish(self, qos: int, payload_size: int) -> None:
        MESSAGES_PUBLISHED.labels(**self._labels(qos=str(qos))).inc()

    def record_message_received(self, qos: int, payload_size: int) -> None:
        MESSAGES_RECEIVED.labels(**self._labels(qos=str(qos))).inc()

    def record_publish_ack(self, qos: int) -> None:
        MESSAGES_PUBLISH_ACK.labels(**self._labels(qos=str(qos))).inc()

    def record_bytes_sent(self, count: int) -> None:
        BYTES_SENT.labels(**self._labels()).inc(count)

    def record_bytes_received(self, count: int) -> None:
        BYTES_RECEIVED.labels(**self._labels()).inc(count)

    def record_connect(self, success: bool) -> None:
        result = "success" if success else "failure"
        CONNECTIONS_TOTAL.labels(**self._labels(result=result)).inc()
        if success:
            CONNECTION_STATUS.labels(**self._labels()).set(1)
        else:
            CONNECTION_STATUS.labels(**self._labels()).set(0)

    def record_disconnect(self) -> None:
        CONNECTION_STATUS.labels(**self._labels()).set(0)

    def record_reconnect(self) -> None:
        RECONNECTS_TOTAL.labels(**self._labels()).inc()

    def record_connection_lost(self) -> None:
        CONNECTION_LOST_TOTAL.labels(**self._labels()).inc()
        CONNECTION_STATUS.labels(**self._labels()).set(0)

    def track_publish_start(self, mid: int) -> None:
        if HAS_PROMETHEUS:
            import time as _time
            self._publish_start_times[mid] = _time.monotonic()

    def record_publish_complete(self, mid: int, qos: int) -> None:
        if HAS_PROMETHEUS:
            import time as _time
            start = self._publish_start_times.pop(mid, None)
            if start is not None:
                elapsed = _time.monotonic() - start
                PUBLISH_LATENCY.labels(**self._labels(qos=str(qos))).observe(elapsed)

    def set_inflight(self, count: int) -> None:
        INFLIGHT_MESSAGES.labels(**self._labels()).set(count)

    def set_outgoing_queue_size(self, count: int) -> None:
        OUTGOING_QUEUE_SIZE.labels(**self._labels()).set(count)


_metrics_server: _MetricsServer | None = None


class _MetricsServer:
    """Simple HTTP server exposing Prometheus metrics."""

    def __init__(self, port: int = 9095, addr: str = ""):
        self._port = port
        self._addr = addr
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not HAS_PROMETHEUS:
            _log.warning("prometheus-client not installed, metrics server will not start")
            return
        if self._thread is not None:
            _log.warning("Metrics server already running")
            return
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        try:
            from prometheus_client import start_http_server as _start
            _start(self._port, self._addr)
            _log.info("Prometheus metrics server started on %s:%s", self._addr or "0.0.0.0", self._port)
            threading.Event().wait()
        except Exception as exc:
            _log.error("Failed to start metrics server: %s", exc)


def start_metrics_server(port: int = 9095, addr: str = "") -> None:
    """Start an HTTP server exposing Prometheus metrics.

    This is a convenience function that starts a lightweight HTTP server
    on the given port and address.  The server runs in a daemon thread.

    Metrics are exposed at ``/metrics``, the standard Prometheus scrape path.

    :param port: TCP port to listen on (default 9095).
    :param addr: Address to bind to (default all interfaces).
    """
    global _metrics_server
    if _metrics_server is None:
        _metrics_server = _MetricsServer(port, addr)
    _metrics_server.start()