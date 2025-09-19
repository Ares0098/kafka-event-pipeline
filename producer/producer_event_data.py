from confluent_kafka import Producer
import socket
import json
import time
import random

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

topic = "event_data"

# Some fake events
event_types = ["login", "logout", "purchase", "view"]

for i in range(10):  # send 10 events
    event = {
        "user_id": random.randint(1, 100),
        "event": random.choice(event_types),
        "amount": random.randint(10, 500) if random.choice(event_types) == "purchase" else None
    }
    producer.produce(
        topic=topic,
        key=str(event["user_id"]),
        value=json.dumps(event),
        callback=delivery_report
    )
    time.sleep(1)  # simulate streaming

producer.flush()