import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM, Dropout
import yfinance as yf
import time

# Fetch initial dataset
def fetch_initial_data(ticker='AAPL'):
    data = yf.download(ticker, start='2010-01-01', end='2023-01-01')
    data = data[['Close']]
    data.reset_index(drop=True, inplace=True)
    return data

# Preprocess data
def preprocess_data(dataset):
    training_set = dataset.iloc[:, 0:1].values
    scaler = MinMaxScaler(feature_range=(0, 1))
    training_set_scaled = scaler.fit_transform(training_set)
    
    X_train = []
    y_train = []
    for i in range(60, len(training_set_scaled)):
        X_train.append(training_set_scaled[i-60:i, 0])
        y_train.append(training_set_scaled[i, 0])
    X_train, y_train = np.array(X_train), np.array(y_train)
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
    return X_train, y_train, scaler

# Build model
def build_model():
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(60, 1)))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

# Train model
def train_model(model, X_train, y_train):
    model.fit(X_train, y_train, epochs=50, batch_size=32)

# Fetch new data dynamically using yfinance
def fetch_new_data(ticker='AAPL'):
    new_data = yf.download(ticker, period='1d', interval='1m')
    latest_price = new_data['Close'].iloc[-1]
    return latest_price

# Update data dynamically
def update_and_plot(model, scaler, ticker='AAPL'):
    plt.ion()  # Enable interactive mode
    fig, ax = plt.subplots()
    predictions = []
    real_stock_prices = []

    for _ in range(100):  # Simulate 100 new updates
        new_price = fetch_new_data(ticker)
        real_stock_prices.append(new_price)
        
        # Scale new data
        scaled_new_price = scaler.transform([[new_price]])
        recent_data = np.array(real_stock_prices[-60:] if len(real_stock_prices) >= 60 else [0] * (60 - len(real_stock_prices)) + real_stock_prices)
        recent_data = scaler.transform(recent_data.reshape(-1, 1)).reshape(1, 60, 1)
        
        # Make prediction
        predicted_price = model.predict(recent_data)[0][0]
        predictions.append(predicted_price)
        
        # Plot updates
        ax.clear()
        ax.plot(real_stock_prices, color='blue', label='Real Stock Price')
        ax.plot(scaler.inverse_transform(np.array(predictions).reshape(-1, 1)), color='red', label='Predicted Price')
        ax.set_title('Stock Price Prediction')
        ax.legend()
        plt.draw()
        plt.pause(1)  # Simulate delay (1 second per update)

# Main execution
if __name__ == '__main__':
    dataset = fetch_initial_data()
    X_train, y_train, scaler = preprocess_data(dataset)
    model = build_model()
    train_model(model, X_train, y_train)
    update_and_plot(model, scaler)