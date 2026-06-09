"""
Example: MQTT client with Prometheus metrics monitoring.

This example demonstrates how to use the paho-mqtt client with built-in
Prometheus metrics collection and HTTP endpoint exposure.
"""
import time

import paho.mqtt.client as mqtt
from paho.mqtt.metrics import get_metrics, start_metrics_server


def main():
    # Start the Prometheus metrics server
    metrics_server = start_metrics_server(host='0.0.0.0', port=9000)
    print("Metrics server started at http://localhost:9000/metrics")

    # Get the metrics instance (singleton)
    metrics = get_metrics()

    # Create MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    # Define callbacks
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"Connected with result code {reason_code}")
        if reason_code.isFailure:
            print(f"Connection failed: {reason_code}")
        else:
            # Subscribe to a topic
            client.subscribe("test/topic", qos=1)

    def on_message(client, userdata, msg):
        print(f"Received message on {msg.topic}: {msg.payload.decode()}")
        # Metrics are automatically recorded

    def on_publish(client, userdata, mid, reason_code, properties):
        print(f"Message {mid} published")

    def on_disconnect(client, userdata, flags, reason_code, properties):
        print(f"Disconnected with result code {reason_code}")

    # Register callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    # Connect to broker
    client.connect("localhost", 1883, 60)

    # Start loop in background
    client.loop_start()

    try:
        # Publish some messages
        for i in range(10):
            topic = "test/topic"
            payload = f"Message {i}"
            client.publish(topic, payload, qos=1)
            print(f"Published: {payload}")
            time.sleep(1)

        # Keep running to receive messages
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        client.loop_stop()
        client.disconnect()
        metrics_server.stop()


if __name__ == "__main__":
    main()
