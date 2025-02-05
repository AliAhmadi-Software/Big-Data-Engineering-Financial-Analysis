from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import socket
import pandas as pd
import logging
import time
import json
import math
import os
# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
OUTPUT_TOPIC = os.getenv("KAFKA_TOPIC_OUTPUT", "output_topic")
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_ILP_PORT = os.getenv("QUESTDB_ILP_PORT",9009)
logging.basicConfig(level=logging.INFO)

# Function to send data to QuestDB via ILP
def send_to_questdb(data):
    try:
        with socket.create_connection((QUESTDB_HOST, QUESTDB_ILP_PORT)) as sock:
            sock.sendall(data.encode("utf-8"))
            logging.info(f"Data sent to QuestDB: {data}")
    except Exception as e:
        logging.error(f"Error sending data to QuestDB: {e}")

# Retry mechanism to connect to Kafka broker
consumer = None
while not consumer:
    try:
        logging.info("Attempting to connect to Kafka broker...")
        consumer = KafkaConsumer(
            OUTPUT_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode("utf-8"),
            api_version=(0, 10, 2)
        )
        logging.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
    except NoBrokersAvailable:
        logging.warning("Kafka broker not available, retrying in 5 seconds...")
        time.sleep(5)

# Main loop to listen for messages
logging.info(f"Listening for messages on topic: {OUTPUT_TOPIC}")
for message in consumer:
    value = message.value
    logging.info(f"Received message: {value}")
    
    try:
        # Deserialize JSON safely
        data = json.loads(value)
        
        # Replace NaN string values with float('nan')
        for key, val in data.items():
            if val == "NaN":
                data[key] = float('nan')
        
        # Ensure `volume` and other numeric fields are converted to integers if necessary
        data['volume'] = int(data['volume']) if not math.isnan(data['volume']) else 0
        data['SMA_5'] = int(data['SMA_5']) if not math.isnan(data['SMA_5']) else 0
        data['delta'] = int(data['delta']) if not math.isnan(data['delta']) else 0
        data['gain'] = int(data['gain']) if not math.isnan(data['gain']) else 0
        data['loss'] = int(data['loss']) if not math.isnan(data['loss']) else 0

        # Convert message to Influx Line Protocol format
        line_protocol = (
            f"stock_data,stock_symbol={data['stock_symbol']} "
            f"open={data['open']},high={data['high']},low={data['low']},"
            f"close={data['close']},volume={data['volume']},"
            f"SMA_5={data['SMA_5']},EMA_10={data['EMA_10'] if not math.isnan(data['EMA_10']) else 'NaN'},"
            f"delta={data['delta']},gain={data['gain']},loss={data['loss']},"
            f"avg_gain_10={data['avg_gain_10'] if not math.isnan(data['avg_gain_10']) else 'NaN'},"
            f"avg_loss_10={data['avg_loss_10'] if not math.isnan(data['avg_loss_10']) else 'NaN'},"
            f"rs={data['rs'] if not math.isnan(data['rs']) else 'NaN'},"
            f"RSI_10={data['RSI_10'] if not math.isnan(data['RSI_10']) else 'NaN'},"
            f"signal=\"{data['signal']}\" "
            f"{int(pd.to_datetime(data['local_time']).timestamp() * 1e9)}\n"
        )
        send_to_questdb(line_protocol)
    except Exception as e:
        logging.error(f"Error processing message: {e}")
