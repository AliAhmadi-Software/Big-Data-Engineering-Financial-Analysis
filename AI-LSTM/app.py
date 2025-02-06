import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
import time
import datetime
import yfinance as yf  # برای دانلود داده‌های به‌روز سهام
import argparse
import os

# تابعی برای ساخت مدل LSTM
def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))  # پیش‌بینی قیمت بسته شدن
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

# دانلود داده‌های جدید
def fetch_stock_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    data.columns = [col[0] for col in data.columns]  # حذف MultiIndex و ساده‌سازی ستون‌ها
    return data[['Open', 'High', 'Low', 'Close']]

# آماده‌سازی داده‌ها
def prepare_data(data, time_steps=60):
    data_min = data.min()
    data_max = data.max()
    scaled_data = (data - data_min) / (data_max - data_min)  # نرمال‌سازی داده‌ها
    x_train, y_train = [], []

    for i in range(time_steps, len(scaled_data) - 30):  # آخرین 30 روز برای پیش‌بینی آینده نگه داشته می‌شود
        x_train.append(scaled_data.iloc[i-time_steps:i].values)
        y_train.append(scaled_data.iloc[i]['Close'])
    
    return np.array(x_train), np.array(y_train), scaled_data, data_min, data_max

# بازگرداندن داده‌ها به مقیاس اصلی
def inverse_transform(predictions, data_min, data_max):
    return predictions * (data_max['Close'] - data_min['Close']) + data_min['Close']

# پیش‌بینی داده‌های آینده
def predict_future(model, recent_data, time_steps, future_days, data_min, data_max):
    future_predictions = []
    input_sequence = recent_data[-time_steps:].values.reshape(1, time_steps, 1)
    for _ in range(future_days):
        pred = model.predict(input_sequence)[0][0]
        future_predictions.append(pred)
        # اضافه کردن پیش‌بینی جدید به دنباله ورودی و حفظ سه بعدی بودن آن
        pred_reshaped = np.array([[[pred]]])  # تبدیل به شکل (1, 1, 1)
        input_sequence = np.append(input_sequence[:, 1:, :], pred_reshaped, axis=1)
    return inverse_transform(np.array(future_predictions), data_min, data_max)

# نمایش نمودار و به‌روزرسانی پویا
def plot_dynamic_chart(actual_prices, future_predictions, dates, future_dates):
    plt.figure(figsize=(10, 6))
    plt.plot(dates, actual_prices, label='Historical Prices', color='blue')
    plt.plot(future_dates, future_predictions, label='30-Day Forecast', linestyle='--', color='orange')
    plt.xlabel('Date')
    plt.ylabel('Stock Price')
    plt.title('LSTM Stock Price Prediction')
    plt.legend()
    plt.grid(True)
    plt.show()

# گرفتن نماد سهام از آرگومان خط فرمان یا متغیر محیطی
def get_stock_symbol():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', default=os.getenv('STOCK_SYMBOL', 'AAPL'))
    args = parser.parse_args()
    return args.symbol

# حلقه اصلی اجرای روزانه
if __name__ == "__main__":
    ticker = get_stock_symbol()
    print(f"Running LSTM model for stock symbol: {ticker}")
    time_steps = 60  # تعداد روزهای ورودی به مدل
    future_days = 30  # تعداد روزهای آینده برای پیش‌بینی

    while True:
        # تنظیم تاریخ‌های شروع و پایان
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

        # گرفتن داده‌های جدید و آماده‌سازی آنها
        stock_data = fetch_stock_data(ticker, start_date, end_date)
        x_train, y_train, scaled_data, data_min, data_max = prepare_data(stock_data[['Close']].reset_index(drop=True), time_steps)

        # ساخت مدل و آموزش آن
        model = build_lstm_model((x_train.shape[1], 1))
        model.fit(x_train, y_train, epochs=50, batch_size=32, verbose=1)  # افزایش تعداد epochs

        # پیش‌بینی آینده
        future_dates = pd.date_range(stock_data.index[-1], periods=future_days + 1, freq='B')[1:]
        future_predictions = predict_future(model, scaled_data, time_steps, future_days, data_min, data_max)

        # نمایش نمودار
        plot_dynamic_chart(stock_data['Close'].values, future_predictions, stock_data.index, future_dates)

        # اجرای کد هر 24 ساعت
        print("Waiting for the next update...")
        time.sleep(86400)  # معادل یک روز