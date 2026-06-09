import sqlite3
import json
from .client import MQTTMessage, MQTTMessageInfo
from .properties import Properties

class SQLitePersistence:
    def __init__(self, db_path="mqtt_session.db"):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        conn.execute('PRAGMA journal_mode=WAL')
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    client_id TEXT,
                    msg_type TEXT, -- "in" or "out"
                    mid INTEGER,
                    timestamp REAL,
                    state INTEGER,
                    dup INTEGER,
                    topic TEXT,
                    payload BLOB,
                    qos INTEGER,
                    retain INTEGER,
                    properties BLOB,
                    PRIMARY KEY (client_id, msg_type, mid)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS subscriptions (
                    client_id TEXT,
                    topic TEXT,
                    qos INTEGER,
                    PRIMARY KEY (client_id, topic)
                )
            ''')
            conn.commit()

    def clear_session(self, client_id):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN TRANSACTION')
            try:
                cursor.execute('DELETE FROM messages WHERE client_id = ?', (client_id,))
                cursor.execute('DELETE FROM subscriptions WHERE client_id = ?', (client_id,))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def save_session(self, client_id, out_messages, in_messages, subscriptions):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN TRANSACTION')
            try:
                cursor.execute('DELETE FROM messages WHERE client_id = ?', (client_id,))
                cursor.execute('DELETE FROM subscriptions WHERE client_id = ?', (client_id,))
                
                for msg_type, messages in [("out", out_messages), ("in", in_messages)]:
                    for mid, m in messages.items():
                        props = m.properties.pack() if getattr(m, 'properties', None) else None
                        cursor.execute('''
                            INSERT INTO messages (client_id, msg_type, mid, timestamp, state, dup, topic, payload, qos, retain, properties)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            client_id, msg_type, mid, m.timestamp, m.state, int(m.dup), 
                            m.topic, m.payload, m.qos, int(m.retain), props
                        ))

                for topic, qos in subscriptions:
                    cursor.execute('''
                        INSERT INTO subscriptions (client_id, topic, qos)
                        VALUES (?, ?, ?)
                    ''', (client_id, topic, qos))
                
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def load_session(self, client_id):
        import collections
        out_messages = collections.OrderedDict()
        in_messages = collections.OrderedDict()
        subscriptions = []
        
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT msg_type, mid, timestamp, state, dup, topic, payload, qos, retain, properties FROM messages WHERE client_id = ? ORDER BY timestamp ASC', (client_id,))
            for row in cursor.fetchall():
                msg_type, mid, timestamp, state, dup, topic, payload, qos, retain, properties_blob = row
                
                m = MQTTMessage(mid, topic.encode('utf-8'))
                m.timestamp = timestamp
                m.state = state
                m.dup = bool(dup)
                m.payload = payload
                m.qos = qos
                m.retain = bool(retain)
                
                if properties_blob:
                    # Try to unpack properties. Note: we need the packet type to unpack, 
                    # but Properties doesn't strictly need it if we know it's PUBLISH
                    from .packettypes import PacketTypes
                    props = Properties(PacketTypes.PUBLISH)
                    props.unpack(properties_blob)
                    m.properties = props
                
                if msg_type == "out":
                    out_messages[mid] = m
                else:
                    in_messages[mid] = m
                    
            cursor.execute('SELECT topic, qos FROM subscriptions WHERE client_id = ?', (client_id,))
            for row in cursor.fetchall():
                subscriptions.append((row[0], row[1]))
                
        return out_messages, in_messages, subscriptions
