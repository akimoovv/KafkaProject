import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5
            )
            logging.info("Connected to Kafka")
            return producer
        except NoBrokersAvailable as e:
            logging.warning(f"Kafka broker not available ({e}), retrying in 5 seconds...")
            time.sleep(5)

producer = create_producer()

cats = [
    {"name": "Whiskers", "age": 3},
    {"name": "Felix", "age": 5},
    {"name": "Garfield", "age": 6},
    {"name": "Tom", "age": 4},
    {"name": "Simba", "age": 2}
]

for cat in cats:
    producer.send('cats', value=cat)
    logging.info(f"Sent cat: {cat}")
    time.sleep(1)

producer.flush()
