from kafka import KafkaProducer
from utils.utils import get_velib_data
import json
from time import sleep

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

last_record_id = None

while True:
    data = get_velib_data(9999)
    print("Sending data to Kafka...")
    producer.send('velib_data', data)
    print("Waiting for the next data update...")
    sleep(60)