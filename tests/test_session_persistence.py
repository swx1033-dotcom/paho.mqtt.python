import os
import threading
import tempfile
import time

import paho.mqtt.client as client
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode, MessageState
from paho.mqtt.persistence import SQLiteSessionPersistence, NullSessionPersistence
from paho.mqtt.client import MQTTMessage, mqtt_ms_publish, mqtt_ms_wait_for_puback

import pytest

import tests.paho_test as paho_test
from tests.testsupport.broker import FakeBroker, fake_broker


class TestSQLiteSessionPersistence:
    """Tests for the SQLite session persistence store."""

    @pytest.fixture
    def tmp_db(self):
        tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        path = tmp.name
        tmp.close()
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_open_and_close(self, tmp_db):
        store = SQLiteSessionPersistence(db_path=tmp_db)
        store.open("test_client")
        store.close()

    def test_save_and_load_session_state(self, tmp_db):
        store = SQLiteSessionPersistence(db_path=tmp_db)
        store.open("test_client")

        out_messages = {
            1: MQTTMessage(mid=1, topic=b"test/topic1"),
        }
        msg = out_messages[1]
        msg.qos = 1
        msg.payload = b"hello"
        msg.state = mqtt_ms_publish

        in_messages = {
            2: MQTTMessage(mid=2, topic=b"test/topic2"),
        }
        msg2 = in_messages[2]
        msg2.qos = 2
        msg2.payload = b"world"
        msg2.state = mqtt_ms_wait_for_puback

        subscriptions = [("test/topic1", 0), ("test/topic2", 1)]

        store.save_session_state(
            last_mid=42,
            out_messages=out_messages,
            in_messages=in_messages,
            subscriptions=subscriptions,
        )

        store.close()

        store2 = SQLiteSessionPersistence(db_path=tmp_db)
        store2.open("test_client")

        last_mid, out_msgs, in_msgs, subs = store2.load_session_state()

        assert last_mid == 42
        assert len(out_msgs) == 1
        assert 1 in out_msgs
        assert out_msgs[1]['topic'] == "test/topic1"
        assert out_msgs[1]['payload'] == b"hello"
        assert out_msgs[1]['qos'] == 1

        assert len(in_msgs) == 1
        assert 2 in in_msgs
        assert in_msgs[2]['topic'] == "test/topic2"
        assert in_msgs[2]['qos'] == 2

        assert subs == [("test/topic1", 0), ("test/topic2", 1)]

        store2.close()

    def test_clear_session_state(self, tmp_db):
        store = SQLiteSessionPersistence(db_path=tmp_db)
        store.open("test_client")

        out_messages = {1: MQTTMessage(mid=1, topic=b"test")}
        out_messages[1].qos = 1
        out_messages[1].payload = b"data"

        store.save_session_state(10, out_messages, {}, [("test", 0)])
        store.clear_session_state()

        last_mid, out_msgs, in_msgs, subs = store.load_session_state()
        assert last_mid == 0
        assert len(out_msgs) == 0
        assert len(in_msgs) == 0
        assert len(subs) == 0

        store.close()

    def test_incremental_message_operations(self, tmp_db):
        store = SQLiteSessionPersistence(db_path=tmp_db)
        store.open("test_client")

        msg = MQTTMessage(mid=10, topic=b"test/out")
        msg.qos = 1
        msg.payload = b"outgoing"
        msg.state = mqtt_ms_publish

        store.save_out_message(10, msg)

        msg2 = MQTTMessage(mid=20, topic=b"test/in")
        msg2.qos = 2
        msg2.payload = b"incoming"
        store.save_in_message(20, msg2)

        _, out_msgs, in_msgs, _ = store.load_session_state()
        assert len(out_msgs) == 1
        assert out_msgs[10]['topic'] == "test/out"
        assert len(in_msgs) == 1
        assert in_msgs[20]['topic'] == "test/in"

        store.delete_out_message(10)
        _, out_msgs, in_msgs, _ = store.load_session_state()
        assert len(out_msgs) == 0
        assert len(in_msgs) == 1

        store.delete_in_message(20)
        _, out_msgs, in_msgs, _ = store.load_session_state()
        assert len(out_msgs) == 0
        assert len(in_msgs) == 0

        store.close()

    def test_subscription_operations(self, tmp_db):
        store = SQLiteSessionPersistence(db_path=tmp_db)
        store.open("test_client")

        store.add_subscription("sensor/temp", 1)
        store.add_subscription("sensor/humidity", 2)

        _, _, _, subs = store.load_session_state()
        assert len(subs) == 2
        assert ("sensor/temp", 1) in subs
        assert ("sensor/humidity", 2) in subs

        store.remove_subscription("sensor/temp")
        _, _, _, subs = store.load_session_state()
        assert len(subs) == 1
        assert ("sensor/humidity", 2) in subs

        store.clear_subscriptions()
        _, _, _, subs = store.load_session_state()
        assert len(subs) == 0

        store.close()

    def test_null_persistence(self):
        store = NullSessionPersistence()
        store.open("test")
        msg = MQTTMessage(mid=1, topic=b"test")
        msg.qos = 1
        msg.payload = b"data"
        store.save_session_state(5, {1: msg}, {}, [("test", 0)])
        last_mid, out_msgs, in_msgs, subs = store.load_session_state()
        assert last_mid == 0
        assert len(out_msgs) == 0
        assert len(in_msgs) == 0
        assert len(subs) == 0
        store.close()

    def test_thread_safety(self, tmp_db):
        store = SQLiteSessionPersistence(db_path=tmp_db)
        store.open("test_client")

        errors = []
        def worker(worker_id):
            try:
                for i in range(10):
                    mid = worker_id * 100 + i
                    msg = MQTTMessage(mid=mid, topic=f"test/worker{worker_id}".encode())
                    msg.qos = 1
                    msg.payload = f"payload_{worker_id}_{i}".encode()
                    msg.state = mqtt_ms_publish
                    store.save_out_message(mid, msg)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(4):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0

        _, out_msgs, _, _ = store.load_session_state()
        assert len(out_msgs) == 40

        store.close()


class TestClientSessionPersistence:
    """Tests for client-level session persistence integration."""

    def test_session_persistence_property(self):
        persist = SQLiteSessionPersistence()
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test-client",
            session_persistence=persist,
        )
        assert mqttc.session_persistence is persist

    def test_session_persistence_defaults_to_null(self):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test-client",
        )
        assert isinstance(mqttc.session_persistence, NullSessionPersistence)

    @pytest.fixture
    def tmp_db(self):
        tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        path = tmp.name
        tmp.close()
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_disconnect_saves_session_state(self, tmp_db):
        persist = SQLiteSessionPersistence(db_path=tmp_db)
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test-persist-client",
            clean_session=False,
            session_persistence=persist,
        )

        msg = MQTTMessage(mid=10, topic=b"test/session")
        msg.qos = 1
        msg.payload = b"persisted_data"
        msg.state = mqtt_ms_publish
        mqttc._out_messages[10] = msg
        mqttc._last_mid = 50

        mqttc.disconnect()

        _, out_msgs, _, _ = persist.load_session_state()
        assert len(out_msgs) >= 1

        persist.close()
        mqttc._reset_sockets()

    def test_connect_restores_session_state(self, tmp_db, fake_broker):
        persist = SQLiteSessionPersistence(db_path=tmp_db)

        msg = MQTTMessage(mid=15, topic=b"test/restore")
        msg.qos = 1
        msg.payload = b"restored_data"
        msg.state = mqtt_ms_publish

        persist.open("test-restore-client")
        persist.save_session_state(
            last_mid=100,
            out_messages={15: msg},
            in_messages={},
            subscriptions=[("test/restore", 0)],
        )
        persist.close()

        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test-restore-client",
            clean_session=False,
            session_persistence=persist,
            transport=fake_broker.transport,
        )
        mqttc.enable_logger()

        assert mqttc._last_mid == 0

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "test-restore-client",
                clean_session=False,
                proto_ver=4,
            )
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count == len(connack_packet)

            time.sleep(0.5)

            assert mqttc._last_mid == 100
            assert 15 in mqttc._out_messages
            assert mqttc._out_messages[15].mid == 15
            assert mqttc._out_messages[15].topic == "test/restore"

        finally:
            mqttc.loop_stop()
            persist.close()
            mqttc._reset_sockets()

    def test_messages_persist_across_disconnect_reconnect(self, tmp_db, fake_broker):
        persist = SQLiteSessionPersistence(db_path=tmp_db)

        msg = MQTTMessage(mid=25, topic=b"test/across")
        msg.qos = 1
        msg.payload = b"survive_data"
        msg.state = mqtt_ms_publish

        persist.open("test-across-client")
        persist.save_session_state(
            last_mid=200,
            out_messages={25: msg},
            in_messages={},
            subscriptions=[("test/across", 0)],
        )
        persist.close()

        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test-across-client",
            clean_session=False,
            session_persistence=persist,
            transport=fake_broker.transport,
        )
        mqttc.enable_logger()

        assert 25 not in mqttc._out_messages

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "test-across-client",
                clean_session=False,
                proto_ver=4,
            )
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count == len(connack_packet)

            time.sleep(0.5)

            assert 25 in mqttc._out_messages
            assert mqttc._out_messages[25].topic == "test/across"
            assert mqttc._out_messages[25].payload == b"survive_data"

        finally:
            mqttc.loop_stop()
            persist.close()
            mqttc._reset_sockets()

    def test_clean_session_clears_persisted_state(self, tmp_db, fake_broker):
        persist = SQLiteSessionPersistence(db_path=tmp_db)

        msg = MQTTMessage(mid=30, topic=b"test/clear")
        msg.qos = 1
        msg.payload = b"clear_data"
        msg.state = mqtt_ms_publish

        persist.open("test-clear-client")
        persist.save_session_state(
            last_mid=300,
            out_messages={30: msg},
            in_messages={},
            subscriptions=[("test/clear", 0)],
        )
        persist.close()

        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test-clear-client",
            clean_session=True,
            session_persistence=persist,
            transport=fake_broker.transport,
        )
        mqttc.enable_logger()

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "test-clear-client",
                clean_session=True,
                proto_ver=4,
            )
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count == len(connack_packet)

            time.sleep(0.5)

            assert 30 not in mqttc._out_messages

        finally:
            mqttc.loop_stop()
            persist.close()
            mqttc._reset_sockets()

    def test_multiple_messages_persistence(self, tmp_db):
        persist = SQLiteSessionPersistence(db_path=tmp_db)

        out_messages = {}
        for i in range(1, 6):
            msg = MQTTMessage(mid=i, topic=f"test/topic{i}".encode())
            msg.qos = 1 if i % 2 == 1 else 2
            msg.payload = f"payload_{i}".encode()
            msg.state = mqtt_ms_publish
            out_messages[i] = msg

        in_messages = {}
        for i in range(10, 13):
            msg = MQTTMessage(mid=i, topic=f"test/in{i}".encode())
            msg.qos = 2
            msg.payload = f"incoming_{i}".encode()
            msg.state = mqtt_ms_wait_for_puback
            in_messages[i] = msg

        subscriptions = [("test/topic1", 0), ("test/topic2", 1), ("test/topic3", 2)]

        persist.open("test-multi-client")
        persist.save_session_state(
            last_mid=500,
            out_messages=out_messages,
            in_messages=in_messages,
            subscriptions=subscriptions,
        )

        last_mid, restored_out, restored_in, restored_subs = persist.load_session_state()

        assert last_mid == 500
        assert len(restored_out) == 5
        assert len(restored_in) == 3
        assert len(restored_subs) == 3

        for i in range(1, 6):
            assert i in restored_out
            assert restored_out[i]['payload'] == f"payload_{i}".encode()
            assert restored_out[i]['qos'] == (1 if i % 2 == 1 else 2)

        for i in range(10, 13):
            assert i in restored_in

        persist.close()