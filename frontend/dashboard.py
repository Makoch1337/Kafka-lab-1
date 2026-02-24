import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import os

st.set_page_config(layout='wide')
st.title('Данные из Kafka')

bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092').split(',')

# Функция для чтения последних сообщений из топика
@st.cache_data(ttl=5)  # кэш на 5 секунд, чтобы не дёргать Kafka слишком часто
def get_latest_messages(topic, max_messages=100):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000
        )
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= max_messages:
                break
        consumer.close()
        return messages
    except Exception as e:
        st.error(f"Ошибка подключения к Kafka: {e}")
        return []

if st.button('Обновить'):
    st.cache_data.clear()

# Читаем данные из топиков
st.subheader('Топик: processed-stock')
stock_data = get_latest_messages('processed-stock', 50)
if stock_data:
    df_stock = pd.DataFrame(stock_data)
    st.dataframe(df_stock)
else:
    st.info('Нет данных в топике processed-stock')

st.subheader('Топик: predictions')
pred_data = get_latest_messages('predictions', 50)
if pred_data:
    df_pred = pd.DataFrame(pred_data)
    st.dataframe(df_pred)
else:
    st.info('Нет данных в топике predictions')
