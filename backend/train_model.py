import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib
import os

print("Downloading Apple stock data...")
df = yf.download('AAPL', start='2020-01-01', end='2024-01-01')
df = df[['Close', 'Volume']].copy()

# Создаём признаки: скользящие средние и RSI
df['SMA_5'] = df['Close'].rolling(window=5).mean()
df['SMA_20'] = df['Close'].rolling(window=20).mean()

delta = df['Close'].diff()
gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
rs = gain / loss
df['RSI'] = 100 - (100 / (1 + rs))

df['Target'] = (df['Close'].shift(-5) > df['Close']).astype(int)

df = df.dropna()

# Признаки для модели
features = ['Close', 'Volume', 'SMA_5', 'SMA_20', 'RSI']
X = df[features]
y = df['Target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)

print("Training Random Forest model...")
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

accuracy = model.score(X_test, y_test)
print(f"Test accuracy: {accuracy:.3f}")

os.makedirs('models', exist_ok=True)
model_path = 'models/stock_model.pkl'
joblib.dump(model, model_path)
print(f"Model saved to {model_path}")
