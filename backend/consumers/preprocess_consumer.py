from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import pandas as pd
import ta
from collections import defaultdict, deque
import os
import time

def wait_for_kafka(bootstrap_servers, timeout=60):
    """Ожидает доступности Kafka-брокеров в течение timeout секунд."""
    from kafka import KafkaProducer
    start = time.time()
    while time.time() - start < timeout:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            print("Kafka is available (preprocess)")
            return True
        except NoBrokersAvailable:
            print("Waiting for Kafka brokers...")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️nexpected error: {e}")
            time.sleep(5)
    print("Timeout waiting for Kafka")
    return False

class PreprocessConsumer:
    def __init__(self, bootstrap_servers, input_topic, output_topic, group_id):
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.output_topic = output_topic
        self.history = defaultdict(lambda: deque(maxlen=50))

    def calculate_indicators(self, symbol, record):
        self.history[symbol].append(record)
        df = pd.DataFrame(list(self.history[symbol]))
        if len(df) < 20:
            return None
        df['SMA_5'] = ta.trend.sma_indicator(df['Close'], window=5)
        df['SMA_20'] = ta.trend.sma_indicator(df['Close'], window=20)
        df['RSI'] = ta.momentum.rsi(df['Close'], window=14)
        latest = df.iloc[-1].to_dict()
        for k, v in latest.items():
            if pd.isna(v):
                latest[k] = None
        return latest

    def run(self):
        for msg in self.consumer:
            data = msg.value
            symbol = 'AAPL'
            enriched = self.calculate_indicators(symbol, data)
            if enriched:
                self.producer.send(self.output_topic, value=enriched)
                print(f"Processed and sent: {enriched['Date']} with SMA_5={enriched['SMA_5']:.2f}, RSI={enriched['RSI']:.2f}")

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092').split(',')
    INPUT_TOPIC = 'raw-stock'
    OUTPUT_TOPIC = 'processed-stock'
    GROUP_ID = os.getenv('GROUP_ID', 'preprocess-group')

    if not wait_for_kafka(BOOTSTRAP_SERVERS):
        exit(1)

    consumer = PreprocessConsumer(BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC, GROUP_ID)
    consumer.run()
