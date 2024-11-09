from kafka import KafkaConsumer

from app.settings.config import TEST_TOPIC, CONSUMER_GROUP_ID, BOOTSTRAP_SERVER
from contextlib import contextmanager


@contextmanager
def create_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        TEST_TOPIC,  # Subscribe to the 'test-topic' topic
        bootstrap_servers=BOOTSTRAP_SERVER,  # Kafka broker address
        group_id=CONSUMER_GROUP_ID,  # Consumer group ID
        auto_offset_reset='earliest',  # Start reading from the earliest message
        key_deserializer=lambda k: k.decode('utf-8') if k else None,  # Deserialize keys
        value_deserializer=lambda v: v.decode('utf-8') if v else None  # Deserialize values
    )
    try:
        yield consumer
    finally:
        consumer.close()
