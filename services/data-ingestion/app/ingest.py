import requests
import time
import json
import logging
from datetime import datetime
import pytz
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
LOCAL_TIMEZONE = pytz.timezone('Asia/Tehran')  # Example: 'Asia/Tehran' for Iran

# Initialize Kafka producer
producer = KafkaProducer(
    api_version=(0, 10, 2),
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# API URL
API_URL = "https://api.nobitex.ir/market/udf/history"

# List of stock symbols to fetch data for
symbols = ["BTCIRT", "USDTIRT", "ETHIRT", "ETCIRT", "SHIBIRT"]

# Parameters for the API request
resolution = "1"  # 1-minute resolution



def format_timestamp(timestamp):
    """Convert Unix timestamp to local datetime string."""
    utc_time = datetime.fromtimestamp(timestamp, pytz.utc)  # Convert to UTC
    local_time = utc_time.astimezone(LOCAL_TIMEZONE)  # Convert to local timezone
    return local_time.strftime('%Y-%m-%d %H:%M:%S')

def send_to_kafka(data, topic, key=None):
    """
    Sends 'data' to the specified 'topic'.

    :param data: The message payload (dict or any Python object).
    :param topic: The Kafka topic string.
    :param key: The key string (e.g. stock symbol).
    """
    try:
        future = producer.send(topic, key=key, value=data)
        record_metadata = future.get(timeout=30)
        logging.info(
            f"Data sent to topic={topic}, partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}, data={data}"
        )
        return True
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        return False

try:
    while True:  # Infinite loop for real-time data
        current_time = int(time.time())
        from_time = current_time - 120  # Fetch the last 2 minutes of data

        for symbol in symbols:
            # API request parameters for each stock
            params = {
                "symbol": symbol,
                "resolution": resolution,
                "from": from_time,
                "to": current_time
            }

            # Send GET request to Nobitex API
            response = requests.get(API_URL, params=params)

            if response.status_code == 200:
                data = response.json()
                if data.get("s") == "ok" and data.get("t"):
                    # Extract the latest candle data
                    timestamps = data["t"]
                    open_prices = data["o"]
                    high_prices = data["h"]
                    low_prices = data["l"]
                    close_prices = data["c"]
                    volumes = data['v']
                    # Prepare the latest data point for ingestion
                    latest_index = -1  # Last element
                    payload = {
                        "stock_symbol": symbol,
                        "local_time": format_timestamp(timestamps[latest_index]),
                        "open": open_prices[latest_index],
                        "high": high_prices[latest_index],
                        "low": low_prices[latest_index],
                        "close": close_prices[latest_index],
                        "volume": volumes[latest_index]
                    }

                    # Determine the topic based on the stock symbol
                    stock_symbol = payload.get("stock_symbol", "")
                    topic = f"{stock_symbol.lower()}_topic" if stock_symbol else "default_topic"
                    payload["topic"] = topic

                    # Send the data to Kafka
                    kafka_result = send_to_kafka(payload, topic, key=stock_symbol)
                    if kafka_result:
                        logging.info(f"payload :{payload} processed and forwarded to Kafka topic: {topic}")
                    else:
                        logging.error(f"Failed to send data to Kafka topic: {topic}")

                elif data.get("s") == "error":
                    logging.error(f"API Error: {data.get('errmsg')}")
                elif data.get("s") == "no_data":
                    logging.info("No new data in the period between from and to.")
                else:
                    logging.info("No new data available or API returned an error.")
            else:
                logging.error(f"HTTP Error: {response.status_code}")

        # Wait for the next minute
        time.sleep(60)

except KeyboardInterrupt:
    logging.info("Real-time data fetching stopped by the user.")
except Exception as e:
    logging.error(f"An error occurred: {e}")