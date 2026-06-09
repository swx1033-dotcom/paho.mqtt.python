#!/usr/bin/env python3

import selectors
import socket
import time
import uuid

import context as _context  # Ensures paho is in PYTHONPATH

import paho.mqtt.client as mqtt

_ = _context

BROKER_HOST = "mqtt.eclipseprojects.io"
BROKER_PORT = 1883
KEEPALIVE = 60
MISC_INTERVAL = 1.0
RECONNECT_DELAY = 3.0
MAX_PUBLISHES = 3
TOPIC_ROOT = "paho-mqtt-python/selectors-multi-client/{}".format(uuid.uuid4())

print("Using topic root: {}".format(TOPIC_ROOT))


class SelectorManagedClient:
    def __init__(self, selector, name, publish_interval, force_reconnect=False):
        self.selector = selector
        self.name = name
        self.publish_interval = publish_interval
        self.force_reconnect = force_reconnect

        self.topic = "{}/{}".format(TOPIC_ROOT, name)
        self.client_id = "paho-mqtt-python/selectors/{}/{}".format(name, uuid.uuid4())
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)

        self.sock = None
        self.want_write = False
        self.connected = False
        self.stopping = False

        self.publish_count = 0
        self.received_count = 0
        self.next_publish = None
        self.next_misc = None
        self.reconnect_at = None

        self.has_connected_once = False
        self.reconnected_once = False
        self.reconnect_demo_done = False

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def log(self, message):
        print("[{}] {}".format(self.name, message))

    def connect(self):
        self.log("Connecting as {}".format(self.client_id))
        self.client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)

    def on_connect(self, client, _userdata, _flags, reason_code, _properties):
        if self.has_connected_once:
            self.reconnected_once = True
            self.log("Reconnected with reason_code={}".format(reason_code))
        else:
            self.has_connected_once = True
            self.log("Connected with reason_code={}".format(reason_code))

        self.connected = True
        self.reconnect_at = None

        # Subscribe again on every successful connect. With MQTT this is the
        # safest default for examples because a reconnect may start with a new
        # session depending on broker settings and protocol version.
        self.log("Subscribing to {}".format(self.topic))
        client.subscribe(self.topic, qos=1)

        if self.publish_count < MAX_PUBLISHES:
            self.next_publish = time.monotonic() + self.publish_interval
        else:
            self.next_publish = None

    def on_disconnect(self, _client, _userdata, _flags, reason_code, _properties):
        self.connected = False

        if self.stopping:
            self.log("Disconnected with reason_code={}".format(reason_code))
            return

        # Reconnect is scheduled outside the callback. Calling reconnect()
        # directly from on_disconnect() can work, but letting the main selector
        # loop own all timing decisions keeps the example easier to reason about.
        self.reconnect_at = time.monotonic() + RECONNECT_DELAY
        self.log(
            "Disconnected with reason_code={}; reconnect scheduled in {:.1f}s".format(
                reason_code, RECONNECT_DELAY
            )
        )

    def on_message(self, client, _userdata, msg):
        self.received_count += 1
        payload = msg.payload.decode("utf-8", errors="replace")
        self.log("Received message {} on {}: {}".format(self.received_count, msg.topic, payload))

        if self.force_reconnect and not self.reconnect_demo_done and self.received_count == 1:
            self.reconnect_demo_done = True
            self.log("Forcing a disconnect to demonstrate selector cleanup and reconnect")
            client.disconnect()

    def on_socket_open(self, client, _userdata, sock):
        self.log("Socket opened")
        self.sock = sock
        self.want_write = client.want_write()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        self.next_misc = time.monotonic() + MISC_INTERVAL

        # Register the exact socket object provided by the callback. On a later
        # reconnect the client gets a different socket, and reusing client.socket()
        # carelessly can leave a stale file descriptor in the selector.
        self._sync_selector(sock)

    def on_socket_close(self, _client, _userdata, sock):
        self.log("Socket closed")

        # Always unregister here, before forgetting the socket. File descriptor
        # numbers can be reused by the OS, so leaving an old registration behind
        # is a classic source of very confusing cross-client bugs.
        self._remove_socket(sock)

        if self.sock is sock:
            self.sock = None

        self.want_write = False
        self.next_misc = None

    def on_socket_register_write(self, _client, _userdata, sock):
        self.log("Watching socket for writability")
        self.want_write = True

        # Only ask the selector for EVENT_WRITE while the library explicitly
        # requests it. Monitoring write readiness all the time looks harmless,
        # but most sockets stay writable almost constantly and the loop will spin.
        self._sync_selector(sock)

    def on_socket_unregister_write(self, _client, _userdata, sock):
        self.log("Stop watching socket for writability")
        self.want_write = False
        self._sync_selector(sock)

    def _sync_selector(self, sock):
        if sock is None or sock.fileno() < 0:
            return

        events = selectors.EVENT_READ
        if self.want_write:
            events |= selectors.EVENT_WRITE

        try:
            self.selector.modify(sock, events, self)
        except KeyError:
            self.selector.register(sock, events, self)

    def _remove_socket(self, sock):
        if sock is None:
            return

        try:
            self.selector.unregister(sock)
        except KeyError:
            pass

    def next_deadline(self):
        deadlines = []

        if self.next_misc is not None and self.sock is not None:
            deadlines.append(self.next_misc)

        if self.connected and self.next_publish is not None:
            deadlines.append(self.next_publish)

        if self.reconnect_at is not None:
            deadlines.append(self.reconnect_at)

        if not deadlines:
            return None

        return min(deadlines)

    def service_misc_if_needed(self, now):
        if self.sock is None or self.next_misc is None or now < self.next_misc:
            return

        self.log("Calling loop_misc")
        rc = self.client.loop_misc()
        self.next_misc = now + MISC_INTERVAL

        if rc != mqtt.MQTT_ERR_SUCCESS:
            self.log("loop_misc returned {}".format(rc))

    def publish_if_needed(self, now):
        if not self.connected or self.next_publish is None or now < self.next_publish:
            return

        payload = "{} message {}".format(self.name, self.publish_count + 1)
        self.log("Publishing to {}: {}".format(self.topic, payload))
        info = self.client.publish(self.topic, payload, qos=1)

        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.log("publish returned {}".format(info.rc))
            self.next_publish = now + self.publish_interval
            return

        self.publish_count += 1

        if self.publish_count < MAX_PUBLISHES:
            self.next_publish = now + self.publish_interval
        else:
            self.next_publish = None

    def reconnect_if_needed(self, now):
        if self.stopping or self.reconnect_at is None or now < self.reconnect_at:
            return

        self.reconnect_at = None
        self.log("Attempting reconnect")

        try:
            rc = self.client.reconnect()
        except OSError as err:
            self.log("Reconnect raised {!r}; retrying later".format(err))
            self.reconnect_at = time.monotonic() + RECONNECT_DELAY
            return

        if rc != mqtt.MQTT_ERR_SUCCESS:
            self.log("Reconnect returned {}; retrying later".format(rc))
            self.reconnect_at = time.monotonic() + RECONNECT_DELAY

    def handle_socket_event(self, sock, mask):
        # selector.select() may return an event for a socket that was closed and
        # replaced between the poll and this handler. Ignore anything that does
        # not match the current live socket for this client.
        if sock is not self.sock:
            return

        if mask & selectors.EVENT_READ:
            self.log("Socket is readable, calling loop_read")
            rc = self.client.loop_read()
            if rc != mqtt.MQTT_ERR_SUCCESS:
                self.log("loop_read returned {}".format(rc))

        if sock is not self.sock:
            return

        if mask & selectors.EVENT_WRITE and self.want_write:
            self.log("Socket is writable, calling loop_write")
            rc = self.client.loop_write()
            if rc != mqtt.MQTT_ERR_SUCCESS:
                self.log("loop_write returned {}".format(rc))

    def is_demo_complete(self):
        reconnect_done = (not self.force_reconnect) or self.reconnected_once
        return (
            reconnect_done
            and self.publish_count >= MAX_PUBLISHES
            and self.received_count >= MAX_PUBLISHES
            and self.reconnect_at is None
        )

    def stop(self):
        self.stopping = True
        self.reconnect_at = None
        self.next_publish = None

        if self.sock is not None:
            self.log("Requesting graceful disconnect")
            self.client.disconnect()


class SelectorsMultiClientExample:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.clients = [
            SelectorManagedClient(self.selector, "alpha", publish_interval=4.0),
            SelectorManagedClient(
                self.selector,
                "beta",
                publish_interval=6.0,
                force_reconnect=True,
            ),
        ]

    def run_due_work(self):
        now = time.monotonic()

        for managed_client in self.clients:
            managed_client.reconnect_if_needed(now)

        now = time.monotonic()
        for managed_client in self.clients:
            managed_client.service_misc_if_needed(now)
            managed_client.publish_if_needed(now)

    def compute_timeout(self):
        now = time.monotonic()
        deadlines = []

        for managed_client in self.clients:
            deadline = managed_client.next_deadline()
            if deadline is not None:
                deadlines.append(deadline)

        if not deadlines:
            return 1.0

        # The timeout must never exceed the nearest scheduled loop_misc,
        # publish, or reconnect deadline. This keeps keepalive pings and retry
        # timers accurate even when no socket becomes readable or writable.
        return max(0.0, min(deadlines) - now)

    def all_clients_done(self):
        return all(managed_client.is_demo_complete() for managed_client in self.clients)

    def shutdown(self):
        for managed_client in self.clients:
            managed_client.stop()

        deadline = time.monotonic() + 5.0
        while any(managed_client.sock is not None for managed_client in self.clients):
            now = time.monotonic()
            if now >= deadline:
                break

            timeout = min(self.compute_timeout(), deadline - now)
            events = self.selector.select(timeout)
            for key, mask in events:
                key.data.handle_socket_event(key.fileobj, mask)

            self.run_due_work()

    def main(self):
        for managed_client in self.clients:
            managed_client.connect()

        try:
            while not self.all_clients_done():
                self.run_due_work()
                timeout = self.compute_timeout()
                events = self.selector.select(timeout)

                for key, mask in events:
                    key.data.handle_socket_event(key.fileobj, mask)
        finally:
            self.shutdown()
            self.selector.close()


print("Starting")
SelectorsMultiClientExample().main()
print("Finished")
