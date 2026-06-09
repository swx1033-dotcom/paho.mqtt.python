import threading

import paho.mqtt.client as client

from tests import paho_test
from tests.testsupport.broker import fake_broker  # noqa: F401


class TestSessionPersistence:
    def test_restores_outgoing_messages_and_subscriptions(self, tmp_path, fake_broker):
        session_path = tmp_path / "mqtt-session.sqlite"
        connected = threading.Event()

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION2,
            "persistent-session-client",
            clean_session=False,
            transport=fake_broker.transport,
        )
        mqttc.enable_session_persistence(str(session_path))
        mqttc.on_connect = lambda *_args: connected.set()
        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()
            fake_broker.expect_packet(
                "connect",
                paho_test.gen_connect("persistent-session-client", clean_session=False),
            )
            fake_broker.send_packet(paho_test.gen_connack(rc=0, flags=0))
            assert connected.wait(1)

            subscribe_result = mqttc.subscribe("persistent/topic", 1)
            assert subscribe_result[0] == client.MQTT_ERR_SUCCESS
            fake_broker.expect_packet(
                "subscribe",
                paho_test.gen_subscribe(1, "persistent/topic", 1),
            )
            fake_broker.send_packet(paho_test.gen_suback(1, 1))

            publish_info = mqttc.publish("persistent/topic", "payload", qos=1)
            assert publish_info.rc == client.MQTT_ERR_SUCCESS
            fake_broker.expect_packet(
                "publish",
                paho_test.gen_publish("persistent/topic", qos=1, payload="payload", mid=2),
            )

            assert mqttc.disconnect() == client.MQTT_ERR_SUCCESS
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

        fake_broker.finish()

        restored_connected = threading.Event()
        restored_client = client.Client(
            client.CallbackAPIVersion.VERSION2,
            "persistent-session-client",
            clean_session=False,
            transport=fake_broker.transport,
        )
        restored_client.enable_session_persistence(str(session_path))
        restored_client.on_connect = lambda *_args: restored_connected.set()
        restored_client.connect_async("localhost", fake_broker.port)
        restored_client.loop_start()

        try:
            fake_broker.start()
            fake_broker.expect_packet(
                "connect",
                paho_test.gen_connect("persistent-session-client", clean_session=False),
            )
            fake_broker.send_packet(paho_test.gen_connack(rc=0, flags=0))
            assert restored_connected.wait(1)

            fake_broker.expect_packet(
                "publish",
                paho_test.gen_publish(
                    "persistent/topic",
                    qos=1,
                    payload="payload",
                    mid=2,
                    dup=True,
                ),
            )
            fake_broker.send_packet(paho_test.gen_puback(2))

            fake_broker.expect_packet(
                "subscribe",
                paho_test.gen_subscribe(3, "persistent/topic", 1),
            )
            fake_broker.send_packet(paho_test.gen_suback(3, 1))

            assert restored_client.disconnect() == client.MQTT_ERR_SUCCESS
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            restored_client.loop_stop()

    def test_restores_incoming_qos2_messages(self, tmp_path, fake_broker):
        session_path = tmp_path / "mqtt-session.sqlite"
        connected = threading.Event()
        received_payloads = []
        message_received = threading.Event()

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION2,
            "persistent-qos2-client",
            clean_session=False,
            transport=fake_broker.transport,
        )
        mqttc.enable_session_persistence(str(session_path))
        mqttc.on_connect = lambda *_args: connected.set()
        mqttc.on_message = lambda _client, _userdata, message: (
            received_payloads.append(message.payload.decode("utf-8")),
            message_received.set(),
        )
        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()
            fake_broker.expect_packet(
                "connect",
                paho_test.gen_connect("persistent-qos2-client", clean_session=False),
            )
            fake_broker.send_packet(paho_test.gen_connack(rc=0, flags=0))
            assert connected.wait(1)

            fake_broker.send_packet(
                paho_test.gen_publish("persistent/topic", qos=2, payload="payload", mid=7)
            )
            fake_broker.expect_packet("pubrec", paho_test.gen_pubrec(7))

            assert mqttc.disconnect() == client.MQTT_ERR_SUCCESS
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

        fake_broker.finish()

        restored_connected = threading.Event()
        restored_client = client.Client(
            client.CallbackAPIVersion.VERSION2,
            "persistent-qos2-client",
            clean_session=False,
            transport=fake_broker.transport,
        )
        restored_client.enable_session_persistence(str(session_path))
        restored_client.on_connect = lambda *_args: restored_connected.set()
        restored_client.on_message = lambda _client, _userdata, message: (
            received_payloads.append(message.payload.decode("utf-8")),
            message_received.set(),
        )
        restored_client.connect_async("localhost", fake_broker.port)
        restored_client.loop_start()

        try:
            fake_broker.start()
            fake_broker.expect_packet(
                "connect",
                paho_test.gen_connect("persistent-qos2-client", clean_session=False),
            )
            fake_broker.send_packet(paho_test.gen_connack(rc=0, flags=0))
            assert restored_connected.wait(1)

            fake_broker.send_packet(paho_test.gen_pubrel(7))
            fake_broker.expect_packet("pubcomp", paho_test.gen_pubcomp(7))
            assert message_received.wait(1)
            assert received_payloads == ["payload"]

            assert restored_client.disconnect() == client.MQTT_ERR_SUCCESS
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            restored_client.loop_stop()
