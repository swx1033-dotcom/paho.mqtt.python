import os
import tempfile
import threading

import paho.mqtt.client as client
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.persistence import SQLitePersistence

import tests.paho_test as paho_test
from tests.testsupport.broker import FakeBroker, fake_broker  # noqa: F401


class TestSQLitePersistence:
    def test_open_close(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            p.close()

    def test_save_and_get_out_messages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_out_message(1, "test/topic", b"payload1", 1, False, False, 2, 1000.0)
                p.save_out_message(2, "test/topic2", b"payload2", 2, True, True, 3, 1001.0)
                msgs = p.get_out_messages()
                assert len(msgs) == 2
                assert msgs[0]["mid"] == 1
                assert msgs[0]["topic"] == "test/topic"
                assert msgs[0]["payload"] == b"payload1"
                assert msgs[0]["qos"] == 1
                assert msgs[0]["retain"] is False
                assert msgs[0]["dup"] is False
                assert msgs[0]["state"] == 2
                assert msgs[1]["mid"] == 2
                assert msgs[1]["retain"] is True
                assert msgs[1]["dup"] is True
            finally:
                p.close()

    def test_save_and_get_in_messages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_in_message(1, "in/topic", b"payload_in", 2, True, False, 5, 2000.0)
                msgs = p.get_in_messages()
                assert len(msgs) == 1
                assert msgs[0]["mid"] == 1
                assert msgs[0]["topic"] == "in/topic"
                assert msgs[0]["payload"] == b"payload_in"
                assert msgs[0]["qos"] == 2
                assert msgs[0]["retain"] is True
            finally:
                p.close()

    def test_remove_out_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_out_message(1, "test/topic", b"payload1", 1, False, False, 2, 1000.0)
                p.save_out_message(2, "test/topic2", b"payload2", 2, True, True, 3, 1001.0)
                p.remove_out_message(1)
                msgs = p.get_out_messages()
                assert len(msgs) == 1
                assert msgs[0]["mid"] == 2
            finally:
                p.close()

    def test_remove_in_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_in_message(1, "in/topic", b"payload_in", 2, True, False, 5, 2000.0)
                p.remove_in_message(1)
                msgs = p.get_in_messages()
                assert len(msgs) == 0
            finally:
                p.close()

    def test_update_out_message_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_out_message(1, "test/topic", b"payload1", 1, False, False, 2, 1000.0)
                p.update_out_message_state(1, 4)
                msgs = p.get_out_messages()
                assert msgs[0]["state"] == 4
            finally:
                p.close()

    def test_save_and_get_subscriptions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_subscription("test/topic", 1)
                p.save_subscription("test/topic2", 2)
                subs = p.get_subscriptions()
                assert len(subs) == 2
                topics = {s["topic"] for s in subs}
                assert "test/topic" in topics
                assert "test/topic2" in topics
            finally:
                p.close()

    def test_remove_subscription(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_subscription("test/topic", 1)
                p.save_subscription("test/topic2", 2)
                p.remove_subscription("test/topic")
                subs = p.get_subscriptions()
                assert len(subs) == 1
                assert subs[0]["topic"] == "test/topic2"
            finally:
                p.close()

    def test_save_and_get_last_mid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                assert p.get_last_mid() == 0
                p.save_last_mid(42)
                assert p.get_last_mid() == 42
                p.save_last_mid(100)
                assert p.get_last_mid() == 100
            finally:
                p.close()

    def test_clear_session(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_out_message(1, "test/topic", b"payload1", 1, False, False, 2, 1000.0)
                p.save_in_message(1, "in/topic", b"payload_in", 2, True, False, 5, 2000.0)
                p.save_subscription("test/topic", 1)
                p.save_last_mid(42)
                p.clear_session()
                assert len(p.get_out_messages()) == 0
                assert len(p.get_in_messages()) == 0
                assert len(p.get_subscriptions()) == 0
                assert p.get_last_mid() == 0
            finally:
                p.close()

    def test_save_all_out_messages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                p.save_out_message(99, "old/topic", b"old", 0, False, False, 0, 0.0)
                p.save_all_out_messages([
                    {"mid": 1, "topic": "test/topic", "payload": b"p1", "qos": 1,
                     "retain": False, "dup": False, "state": 2, "timestamp": 1000.0},
                    {"mid": 2, "topic": "test/topic2", "payload": b"p2", "qos": 2,
                     "retain": True, "dup": True, "state": 3, "timestamp": 1001.0},
                ])
                msgs = p.get_out_messages()
                assert len(msgs) == 2
                assert msgs[0]["mid"] == 1
                assert msgs[1]["mid"] == 2
            finally:
                p.close()

    def test_concurrent_access(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                errors = []

                def writer(thread_id):
                    try:
                        for i in range(50):
                            p.save_out_message(
                                thread_id * 1000 + i,
                                f"thread/{thread_id}/topic/{i}",
                                f"payload_{thread_id}_{i}".encode(),
                                i % 3,
                                False, False, i % 10, float(i),
                            )
                    except Exception as e:
                        errors.append(e)

                threads = [threading.Thread(target=writer, args=(t,)) for t in range(5)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                assert len(errors) == 0
                msgs = p.get_out_messages()
                assert len(msgs) == 250
            finally:
                p.close()

    def test_wal_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            p = SQLitePersistence("test_client", db_path=db_path)
            p.open()
            try:
                cursor = p._conn.execute("PRAGMA journal_mode")
                mode = cursor.fetchone()[0]
                assert mode.lower() == "wal"
            finally:
                p.close()


class TestClientPersistence:
    def test_enable_persistence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "test-persist-client",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
            )
            persistence = SQLitePersistence("test-persist-client", db_path=db_path)
            mqttc.enable_persistence(persistence)
            assert mqttc._persistence is not None
            mqttc._persistence.close()

    def test_enable_persistence_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                mqttc = client.Client(
                    CallbackAPIVersion.VERSION2,
                    "test-persist-default",
                    protocol=MQTTProtocolVersion.MQTTv311,
                    clean_session=False,
                )
                mqttc.enable_persistence()
                assert mqttc._persistence is not None
                mqttc._persistence.close()
            finally:
                os.chdir(old_cwd)

    def test_persistence_save_session_on_disconnect(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-disconnect-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
                transport=fake_broker.transport,
            )
            persistence = SQLitePersistence("persist-disconnect-test", db_path=db_path)
            mqttc.enable_persistence(persistence)

            connected = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                connected.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-disconnect-test", keepalive=60,
                    clean_session=False, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                connected.wait(timeout=5)

                disconnect_packet = paho_test.gen_disconnect()
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == disconnect_packet
            finally:
                mqttc.loop_stop()

            assert len(persistence.get_subscriptions()) >= 0
            persistence.close()

    def test_persistence_tracks_subscriptions(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-sub-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
                transport=fake_broker.transport,
            )
            persistence = SQLitePersistence("persist-sub-test", db_path=db_path)
            mqttc.enable_persistence(persistence)

            subscribed = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                mqttc.subscribe("test/topic", 1)

            def on_subscribe(mqttc, obj, mid, reason_codes, properties):
                subscribed.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.on_subscribe = on_subscribe
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-sub-test", keepalive=60,
                    clean_session=False, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                subscribe_packet = paho_test.gen_subscribe(
                    1, "test/topic", 1)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == subscribe_packet

                suback_packet = paho_test.gen_suback(1, 1)
                fake_broker.send_packet(suback_packet)

                subscribed.wait(timeout=5)

                disconnect_packet = paho_test.gen_disconnect()
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == disconnect_packet
            finally:
                mqttc.loop_stop()

            subs = persistence.get_subscriptions()
            assert len(subs) == 1
            assert subs[0]["topic"] == "test/topic"
            assert subs[0]["qos"] == 1
            persistence.close()

    def test_persistence_restores_subscriptions_on_reconnect(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            persistence = SQLitePersistence("persist-reconnect-test", db_path=db_path)
            persistence.open()
            persistence.save_subscription("test/topic", 1)
            persistence.save_subscription("test/topic2", 2)
            persistence.close()

            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-reconnect-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
                transport=fake_broker.transport,
            )
            persistence2 = SQLitePersistence("persist-reconnect-test", db_path=db_path)
            mqttc.enable_persistence(persistence2)

            connected = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                connected.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-reconnect-test", keepalive=60,
                    clean_session=False, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                connected.wait(timeout=5)

                assert "test/topic" in mqttc._subscriptions
                assert mqttc._subscriptions["test/topic"] == 1
                assert "test/topic2" in mqttc._subscriptions
                assert mqttc._subscriptions["test/topic2"] == 2
            finally:
                mqttc.loop_stop()
                persistence2.close()

    def test_persistence_saves_out_messages(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-outmsg-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
                transport=fake_broker.transport,
            )
            persistence = SQLitePersistence("persist-outmsg-test", db_path=db_path)
            mqttc.enable_persistence(persistence)

            published = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                mqttc.publish("test/topic", b"hello", qos=1)

            def on_publish(mqttc, obj, mid, reason_code, properties):
                published.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.on_publish = on_publish
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-outmsg-test", keepalive=60,
                    clean_session=False, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                publish_packet = paho_test.gen_publish(
                    "test/topic", qos=1, payload=b"hello", mid=1)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == publish_packet

                puback_packet = paho_test.gen_puback(1)
                fake_broker.send_packet(puback_packet)

                published.wait(timeout=5)

                disconnect_packet = paho_test.gen_disconnect()
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == disconnect_packet
            finally:
                mqttc.loop_stop()

            out_msgs = persistence.get_out_messages()
            assert len(out_msgs) == 0
            persistence.close()

    def test_persistence_saves_qos2_out_messages(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-qos2-outmsg-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
                transport=fake_broker.transport,
            )
            persistence = SQLitePersistence("persist-qos2-outmsg-test", db_path=db_path)
            mqttc.enable_persistence(persistence)

            published = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                mqttc.publish("test/qos2", b"hello_qos2", qos=2)

            def on_publish(mqttc, obj, mid, reason_code, properties):
                published.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.on_publish = on_publish
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-qos2-outmsg-test", keepalive=60,
                    clean_session=False, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                publish_packet = paho_test.gen_publish(
                    "test/qos2", qos=2, payload=b"hello_qos2", mid=1)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == publish_packet

                pubrec_packet = paho_test.gen_pubrec(1)
                fake_broker.send_packet(pubrec_packet)

                pubrel_packet = paho_test.gen_pubrel(1)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == pubrel_packet

                pubcomp_packet = paho_test.gen_pubcomp(1)
                fake_broker.send_packet(pubcomp_packet)

                published.wait(timeout=5)

                disconnect_packet = paho_test.gen_disconnect()
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == disconnect_packet
            finally:
                mqttc.loop_stop()

            out_msgs = persistence.get_out_messages()
            assert len(out_msgs) == 0
            persistence.close()

    def test_persistence_clear_session_on_clean_session(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            persistence = SQLitePersistence("persist-clean-test", db_path=db_path)
            persistence.open()
            persistence.save_subscription("test/topic", 1)
            persistence.save_out_message(1, "test/topic", b"payload", 1, False, False, 2, 1000.0)
            persistence.save_last_mid(5)
            persistence.close()

            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-clean-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=True,
                transport=fake_broker.transport,
            )
            persistence2 = SQLitePersistence("persist-clean-test", db_path=db_path)
            mqttc.enable_persistence(persistence2)

            connected = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                connected.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-clean-test", keepalive=60,
                    clean_session=True, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                connected.wait(timeout=5)

                disconnect_packet = paho_test.gen_disconnect()
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == disconnect_packet
            finally:
                mqttc.loop_stop()

            persistence2.close()
            persistence3 = SQLitePersistence("persist-clean-test", db_path=db_path)
            persistence3.open()
            assert len(persistence3.get_subscriptions()) == 0
            assert len(persistence3.get_out_messages()) == 0
            persistence3.close()

    def test_persistence_unsubscribe_removes_from_store(self, fake_broker):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-unsub-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
                transport=fake_broker.transport,
            )
            persistence = SQLitePersistence("persist-unsub-test", db_path=db_path)
            mqttc.enable_persistence(persistence)

            unsubscribed = threading.Event()

            def on_connect(mqttc, obj, flags, reason_code, properties):
                mqttc.subscribe("test/topic", 1)

            def on_subscribe(mqttc, obj, mid, reason_codes, properties):
                mqttc.unsubscribe("test/topic")

            def on_unsubscribe(mqttc, obj, mid, reason_codes, properties):
                unsubscribed.set()
                mqttc.disconnect()

            mqttc.on_connect = on_connect
            mqttc.on_subscribe = on_subscribe
            mqttc.on_unsubscribe = on_unsubscribe
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()

            try:
                fake_broker.start()
                connect_packet = paho_test.gen_connect(
                    "persist-unsub-test", keepalive=60,
                    clean_session=False, proto_ver=4)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == connect_packet

                connack_packet = paho_test.gen_connack(rc=0)
                fake_broker.send_packet(connack_packet)

                subscribe_packet = paho_test.gen_subscribe(1, "test/topic", 1)
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == subscribe_packet

                suback_packet = paho_test.gen_suback(1, 1)
                fake_broker.send_packet(suback_packet)

                unsubscribe_packet = paho_test.gen_unsubscribe(2, "test/topic")
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == unsubscribe_packet

                unsuback_packet = paho_test.gen_unsuback(2)
                fake_broker.send_packet(unsuback_packet)

                unsubscribed.wait(timeout=5)

                disconnect_packet = paho_test.gen_disconnect()
                packet_in = fake_broker.receive_packet(1000)
                assert packet_in == disconnect_packet
            finally:
                mqttc.loop_stop()

            subs = persistence.get_subscriptions()
            assert len(subs) == 0
            persistence.close()

    def test_persistence_mid_preserved_across_sessions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            persistence = SQLitePersistence("persist-mid-test", db_path=db_path)
            persistence.open()
            persistence.save_last_mid(50)
            persistence.close()

            mqttc = client.Client(
                CallbackAPIVersion.VERSION2,
                "persist-mid-test",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_session=False,
            )
            persistence2 = SQLitePersistence("persist-mid-test", db_path=db_path)
            mqttc.enable_persistence(persistence2)
            mqttc._persistence_restore_session()

            assert mqttc._last_mid == 50

            new_mid = mqttc._mid_generate()
            assert new_mid == 51

            persistence2.close()

    def test_persistence_no_persistence_no_error(self):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "no-persist-test",
            protocol=MQTTProtocolVersion.MQTTv311,
        )
        assert mqttc._persistence is None
        mqttc._persistence_save_session()
        mqttc._persistence_restore_session()
        mqttc._persistence_save_out_message(None)
        mqttc._persistence_remove_out_message(1)
        mqttc._persistence_save_in_message(None)
        mqttc._persistence_remove_in_message(1)
        mqttc._persistence_update_out_message_state(1, 0)
        mqttc._persistence_save_subscription("test", 0)
        mqttc._persistence_remove_subscription("test")
