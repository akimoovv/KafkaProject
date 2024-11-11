import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                'cats',
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='cat-consumers',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info("Connected to Kafka")
            return consumer
        except NoBrokersAvailable as e:
            logging.warning(f"Kafka broker not available ({e}), retrying in 5 seconds...")
            time.sleep(5)

consumer = create_consumer()

for message in consumer:
    cat = message.value
    logging.info(f"Received cat: {cat}")
    # Simulate processing
    # Commit offset after processing
    consumer.commit()
