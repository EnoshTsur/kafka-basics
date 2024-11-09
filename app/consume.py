from app.services.consumer import create_consumer

if __name__ == '__main__':
    with create_consumer() as consumer:
        print("Waiting for messages...")
        try:
            for message in consumer:
                # Print the received message key, value, topic, partition, and offset
                print(f"Received message: key={message.key}, value={message.value}, "
                      f"topic={message.topic}, partition={message.partition}, offset={message.offset}")
        except KeyboardInterrupt:
            pass