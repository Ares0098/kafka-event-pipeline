from confluent_kafka import Consumer, KafkaException, KafkaError
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'event-data-consumers',   # consumer group name
    'auto.offset.reset': 'earliest'       # read from beginning if no committed offset
}

consumer = Consumer(conf)
topic = "event_data"
consumer.subscribe([topic])

print(f"👂 Listening to topic '{topic}'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        # Deserialize JSON
        event = json.loads(msg.value().decode("utf-8"))
        print(f"📩 Received: {event}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()