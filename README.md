# Kafka Real-time Stock Analysis

Проект для лабораторной работы: конвейер обработки данных с Apache Kafka, машинным обучением и визуализацией в Streamlit.

## Состав проекта
- **backend/** – продюсеры и консюмеры (Python)
- **frontend/** – дашборд Streamlit
- **docker-compose.yml** – оркестрация всех сервисов
- **data/** – исторические данные (AAPL)

## Запуск
1. Установите Docker и docker-compose.
2. Выполните:
   ```
   docker-compose up --build -d
   ```
3. Откройте http://localhost:8501

