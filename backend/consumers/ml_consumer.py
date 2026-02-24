from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import joblib
import numpy as np
import os
import time
from collections import deque

def wait_for_kafka(bootstrap_servers, timeout=60):
    """Ожидает доступности Kafka-брокеров в течение timeout секунд."""
    from kafka import KafkaProducer
    start = time.time()
    while time.time() - start < timeout:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            print("✅ Kafka is available (ml)")
            return True
        except NoBrokersAvailable:
            print("Waiting for Kafka brokers...")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️error: {e}")
            time.sleep(5)
    print("Timeout waiting for Kafka")
    return False

class MLConsumer:
    def __init__(self, bootstrap_servers, input_topic, output_topic, model_path, group_id):
        self.model = joblib.load(model_path)
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
        self.prediction_history = deque(maxlen=100)

    def predict(self, record):
        features = [
            record['Close'],
            record['Volume'],
            record.get('SMA_5', 0),
            record.get('SMA_20', 0),
            record.get('RSI', 50)
        ]
        features = [0 if x is None else x for x in features]
        pred = self.model.predict([features])[0]
        proba = self.model.predict_proba([features])[0].max()
        return int(pred), float(proba)

    def run(self):
        for msg in self.consumer:
            data = msg.value
            pred, prob = self.predict(data)
            result = {
                'Date': data['Date'],
                'prediction': pred,
                'probability': prob,
                'actual_future': None
            }
            self.producer.send(self.output_topic, value=result)
            print(f"Prediction for {data['Date']}: {pred} (prob={prob:.2f})")

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092').split(',')
    INPUT_TOPIC = 'processed-stock'
    OUTPUT_TOPIC = 'predictions'
    MODEL_PATH = '/app/models/stock_model.pkl'
    GROUP_ID = os.getenv('GROUP_ID', 'ml-group')

    if not wait_for_kafka(BOOTSTRAP_SERVERS):
        exit(1)

    consumer = MLConsumer(BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC, MODEL_PATH, GROUP_ID)
    consumer.run()
