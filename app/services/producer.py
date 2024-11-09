from kafka import KafkaProducer

from app.settings.config import BOOTSTRAP_SERVER


def create_producer() -> KafkaProducer:
    # Create a Kafka producer instance
    # 'bootstrap_servers' specifies the Kafka broker's address
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,  # Kafka broker address
        key_serializer=lambda k: k.encode('utf-8'),  # Serialize keys to UTF-8
        value_serializer=lambda v: v.encode('utf-8')  # Serialize values to UTF-8
    )
