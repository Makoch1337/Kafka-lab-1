import time
import random
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import os

def wait_for_kafka(bootstrap_servers, timeout=60):
    """Ожидает доступности Kafka-брокеров в течение timeout секунд."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            print("Kafka is available (producer)")
            return True
        except NoBrokersAvailable:
            print("Waiting for Kafka brokers...")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️nexpected error: {e}")
            time.sleep(5)
    print("Timeout waiting for Kafka")
    return False

class StockProducer:
    def __init__(self, bootstrap_servers, topic, acks='all'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=acks,
            retries=5
        )
        self.topic = topic
        self.sleep_mode = os.getenv('SLEEP_MODE', 'fixed')
        self.sleep_time = float(os.getenv('SLEEP_TIME', 0.1))

    def send_stock_data(self, data):
        try:
            future = self.producer.send(self.topic, key=data['Date'].encode(), value=data)
            result = future.get(timeout=10)
            print(f"Sent: {data['Date']} at offset {result.offset}")
        except Exception as e:
            print(f"Error sending {data['Date']}: {e}")

    def simulate_stream(self, csv_path):
        df = pd.read_csv(csv_path)
        
        if 'Date' not in df.columns:
            date_col = df.columns[0]
            df.rename(columns={date_col: 'Date'}, inplace=True)
            print(f"Renamed column '{date_col}' to 'Date'")
        
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        for _, row in df.iterrows():
            record = row.to_dict()
            self.send_stock_data(record)
            if self.sleep_mode == 'fixed':
                time.sleep(self.sleep_time)
            else:
                time.sleep(random.uniform(0.05, 0.3))

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092').split(',')
    TOPIC = 'raw-stock'
    DATA_PATH = '/app/data/aapl.csv'
    ACKS = os.getenv('ACKS', 'all')

    if not wait_for_kafka(BOOTSTRAP_SERVERS):
        exit(1)

    producer = StockProducer(BOOTSTRAP_SERVERS, TOPIC, acks=ACKS)
    producer.simulate_stream(DATA_PATH)
