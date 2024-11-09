from app.services.producer import create_producer
from app.settings.config import TEST_TOPIC

if __name__ == '__main__':
    producer = create_producer()
    # Produce a message to the 'test-topic' topic
    # The key helps in partitioning (optional)
    future = producer.send(topic=TEST_TOPIC, key='hello', value='Hello, Kafka!')

    # Wait for the message to be sent and get the result
    result = future.get(timeout=10)  # Wait up to 10 seconds for delivery
    print(f"Message sent to topic {result.topic}, partition {result.partition}, offset {result.offset}")
    producer.close()
