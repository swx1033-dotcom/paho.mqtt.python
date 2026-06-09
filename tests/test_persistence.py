import os
import unittest
import paho.mqtt.client as mqtt
from paho.mqtt.persistence import SQLitePersistence

class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_mqtt_session.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_session_persistence(self):
        persistence = SQLitePersistence(self.db_path)
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="test_persistence_client", clean_session=False)
        client.persistence_set(persistence)
        
        # Mock connection and add state
        client._client_id = b"test_persistence_client"
        client.subscribe("test/topic", 1)
        
        # Add out_messages
        msg_out = mqtt.MQTTMessage(1, b"test/topic")
        msg_out.payload = b"payload1"
        msg_out.qos = 1
        msg_out.state = 1
        client._out_messages[1] = msg_out
        
        # Add in_messages
        msg_in = mqtt.MQTTMessage(2, b"test/topic")
        msg_in.payload = b"payload2"
        msg_in.qos = 2
        msg_in.state = 2
        client._in_messages[2] = msg_in

        # Save session
        client._save_session()
        
        # Create a new client instance to simulate reconnect
        new_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="test_persistence_client", clean_session=False)
        new_client.persistence_set(persistence)
        new_client._client_id = b"test_persistence_client"
        
        # Load session
        new_client._load_session()
        
        self.assertEqual(len(new_client._subscriptions), 1)
        self.assertEqual(new_client._subscriptions[0][0], "test/topic")
        self.assertEqual(new_client._subscriptions[0][1], 1)
        
        self.assertEqual(len(new_client._out_messages), 1)
        self.assertEqual(new_client._out_messages[1].payload, b"payload1")
        self.assertEqual(new_client._out_messages[1].qos, 1)
        
        self.assertEqual(len(new_client._in_messages), 1)
        self.assertEqual(new_client._in_messages[2].payload, b"payload2")
        self.assertEqual(new_client._in_messages[2].qos, 2)
        
        # Reset session
        new_client._reset_session()
        self.assertEqual(len(new_client._out_messages), 0)
        
        # Reload to verify it's cleared
        new_client._load_session()
        self.assertEqual(len(new_client._out_messages), 0)
        self.assertEqual(len(new_client._subscriptions), 0)

if __name__ == '__main__':
    unittest.main()
