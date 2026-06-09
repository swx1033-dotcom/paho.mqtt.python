"""
Prometheus metrics for paho-mqtt client.
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Metric definitions
MQTT_MESSAGES_PUBLISHED = Counter(
    "mqtt_messages_published_total",
    "Total number of MQTT messages published",
    ["client_id", "topic", "qos"]
)

MQTT_MESSAGES_RECEIVED = Counter(
    "mqtt_messages_received_total",
    "Total number of MQTT messages received",
    ["client_id", "topic", "qos"]
)

MQTT_RECONNECTS = Counter(
    "mqtt_reconnects_total",
    "Total number of MQTT reconnections",
    ["client_id"]
)

MQTT_PUBLISH_LATENCY = Histogram(
    "mqtt_publish_latency_seconds",
    "Latency of MQTT message publish (from publish to PUBACK/PUBCOMP)",
    ["client_id", "topic", "qos"]
)

MQTT_CONNECTED = Gauge(
    "mqtt_connected_status",
    "Current connection status of the MQTT client (1 = connected, 0 = disconnected)",
    ["client_id"]
)

def start_metrics_server(port: int = 8000, addr: str = "0.0.0.0"):
    """
    Start the Prometheus metrics HTTP server.
    """
    start_http_server(port, addr)
