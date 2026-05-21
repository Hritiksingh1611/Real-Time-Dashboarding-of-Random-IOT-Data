import json
import logging
import os
import random
import time
import sys

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'iot_data')
MAX_ITERATIONS = os.getenv('MAX_ITERATIONS')

def generate_iot_data():

    producer = None

    try:
        # Kafka Producer
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        devices = ['device_1', 'device_2', 'device_3']
        count = 0
        max_iterations = int(MAX_ITERATIONS) if MAX_ITERATIONS else None

        while max_iterations is None or count < max_iterations:
            try:
                data = {
                    'device_id': random.choice(devices),
                    'timestamp': time.time(),
                    'temperature': round(random.uniform(20.0, 30.0), 2),
                    'humidity': round(random.uniform(40.0,60.0), 2)
                }
                print(f"Producing IoT data to Kafka topic '{KAFKA_TOPIC}' : {data}")

                # Send data to Kafka topic
                future = producer.send(KAFKA_TOPIC, data)
                future.get(timeout=10)

                count += 1
                time.sleep(1)

            except KafkaError as e:
                logging.error(f"Error sending data to Kafka: {e}")
                continue

            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                sys.exit(1)

    except Exception as e:
        logging.error(f"Error in the data simulation process: {e}")
        sys.exit(1)

    finally:
        if producer is not None:
            producer.close()

if __name__ == '__main__':
    generate_iot_data()
