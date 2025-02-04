import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

import pandas as pd
import json
import math
from kafka import KafkaProducer
import psycopg2  # <-- for inserting into QuestDB

# Configure logging
# logging.basicConfig(level=logging.INFO)

# Set log level for Kafka consumer to WARN to reduce verbosity
spark = SparkSession.builder \
    .appName("OneRowPerSymbolMinute") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.properties") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties") \
    .getOrCreate()

# Set Kafka consumer log level to WARN
spark.sparkContext.setLogLevel("WARN")


# Keep track of which (symbol, local_time) we already sent
already_sent = set()

# (A) Global DataFrame to accumulate data
global_data = pd.DataFrame(columns=[
    "stock_symbol", "local_time",
    "open", "high", "low", "close", "volume"
])

# -----------------------------------------------------------------------------
# QuestDB Configuration
# -----------------------------------------------------------------------------
QUESTDB_HOST = "questdb"
QUESTDB_PORT = 8812
QUESTDB_DATABASE = "qdb"
QUESTDB_USER = "admin"
QUESTDB_PASSWORD = "quest"

def insert_batch_to_questdb(records):
    """
    Insert the given list of record dicts into QuestDB's 'stock_data' table.
    The table schema includes a 'signal' column, so each record should have it.
    
    Each dict in 'records' is expected to have these keys:
      stock_symbol, local_time, open, high, low, close, volume,
      SMA_5, EMA_10, delta, gain, loss, avg_gain_10, avg_loss_10,
      rs, RSI_10, signal
    """
    if not records:
        return  # nothing to insert

    # Open connection to QuestDB
    try:
        conn = psycopg2.connect(
            dbname=QUESTDB_DATABASE,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            host=QUESTDB_HOST,
            port=QUESTDB_PORT
        )
        logging.warning("Successfully connected to QuestDB.")
    except Exception as e:
        logging.error(f"Error connecting to QuestDB: {e}")
        return

    try:
        query = """
        INSERT INTO stock_data (
            stock_symbol, local_time, open, high, low, close, volume,
            SMA_5, EMA_10, delta, gain, loss, avg_gain_10, avg_loss_10,
            rs, RSI_10, signal
        )
        VALUES (
            %(stock_symbol)s, %(local_time)s, %(open)s, %(high)s, %(low)s,
            %(close)s, %(volume)s, %(SMA_5)s, %(EMA_10)s, %(delta)s,
            %(gain)s, %(loss)s, %(avg_gain_10)s, %(avg_loss_10)s,
            %(rs)s, %(RSI_10)s, %(signal)s
        );
        """
        with conn.cursor() as cur:
            for record in records:
                # Convert any float('nan') or string "NaN" to None to avoid insert errors
                for k, v in record.items():
                    if isinstance(v, float) and math.isnan(v):
                        record[k] = None
                    elif isinstance(v, str) and v.lower() == "nan":
                        record[k] = None

                cur.execute(query, record)
        conn.commit()

        logging.warning(f"Inserted {len(records)} new records into QuestDB.")
    except Exception as e:
        logging.error(f"Error inserting into QuestDB: {e}")
    finally:
        conn.close()

# -------------------------------------------------------------------
# 1) Compute Indicators
# -------------------------------------------------------------------
def compute_indicators_for_group(group_df: pd.DataFrame):
    gdf = group_df.copy()

    # -- SMA(5) --
    gdf["SMA_5"] = gdf["close"].rolling(window=5).mean()

    # -- EMA(10) --
    gdf["EMA_10"] = gdf["close"].ewm(span=10, adjust=False, min_periods=10).mean()

    # -- RSI(10) --
    gdf["delta"] = gdf["close"].diff()
    gdf["gain"] = gdf["delta"].clip(lower=0)
    gdf["loss"] = -gdf["delta"].clip(upper=0)
    gdf["avg_gain_10"] = gdf["gain"].rolling(window=10).mean()
    gdf["avg_loss_10"] = gdf["loss"].rolling(window=10).mean()
    # Avoid division by zero in RSI calculation
    gdf["rs"] = gdf["avg_gain_10"] / gdf["avg_loss_10"].replace({0: None})
    gdf["RSI_10"] = 100 - (100 / (1 + gdf["rs"]))

    return gdf

# -------------------------------------------------------------------
# 2) Generate Signals (Scenario B)
# -------------------------------------------------------------------
def generate_signals_scenario_b(df: pd.DataFrame) -> pd.DataFrame:
    """
    Scenario B (Crossover-Driven Priority):
      1) BUY if SMA(5) > EMA(10) and RSI_10 < 70
      2) SELL if SMA(5) < EMA(10) and RSI_10 > 30
      3) Otherwise, HOLD
    """
    def get_signal(row):
        rsi = row["RSI_10"]
        sma_5 = row["SMA_5"]
        ema_10 = row["EMA_10"]

        # If any indicator is NaN, default to HOLD
        if pd.isnull(rsi) or pd.isnull(sma_5) or pd.isnull(ema_10):
            return "HOLD"

        if (sma_5 > ema_10) and (rsi < 70):
            return "BUY"
        elif (sma_5 < ema_10) and (rsi > 30):
            return "SELL"
        else:
            return "HOLD"

    df["signal"] = df.apply(get_signal, axis=1)
    return df

# -------------------------------------------------------------------
# 3) foreachBatch Processing
# -------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    print(f"\n=== Processing Micro-Batch: {batch_id} ===")
    # Disable Arrow conversions to avoid issues with row-by-row Pandas ops
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    # 1) Convert local_time => string
    batch_df = batch_df.selectExpr(
        "stock_symbol",
        "CAST(local_time AS STRING) AS local_time_str",
        "open", "high", "low", "close", "volume"
    )

    # 2) Convert Spark DF to Pandas
    pdf = batch_df.toPandas()
    if pdf.empty:
        print("No new data in this batch.")
        return

    # 3) Convert local_time_str -> datetime
    pdf["local_time"] = pd.to_datetime(pdf["local_time_str"])
    pdf.drop(columns=["local_time_str"], inplace=True)

    # 4) Append new rows to global data
    global global_data
    global_data = pd.concat([global_data, pdf], ignore_index=True)

    # 5) Sort + compute rolling indicators
    global_data.sort_values(["stock_symbol", "local_time"], inplace=True)
    updated_pdf = global_data.groupby("stock_symbol", group_keys=False).apply(compute_indicators_for_group)
    updated_pdf.reset_index(drop=True, inplace=True)

    # 6) Generate signals (Scenario B)
    updated_pdf = generate_signals_scenario_b(updated_pdf)

    # 7) We only want the final row for each symbol/time
    latest_symbol_time = updated_pdf.groupby(
        ["stock_symbol", "local_time"], group_keys=False
    ).tail(1)

    # 8) Filter out (symbol, local_time) combos we've already sent
    new_records = []
    for row_dict in latest_symbol_time.to_dict(orient="records"):
        combo = (row_dict["stock_symbol"], row_dict["local_time"])
        if combo not in already_sent:
            new_records.append(row_dict)
            already_sent.add(combo)

    if not new_records:
        print("No newly updated rows to send this batch.")
        return

    # 9) Send new records (including 'signal') to Kafka
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )
    for row in new_records:
        producer.send(output_topic, row)
    producer.flush()
    producer.close()
    print(f"Sent {len(new_records)} new (symbol, local_time) combos to Kafka topic: {output_topic}")

    # 10) Insert new records to QuestDB
    insert_batch_to_questdb(new_records)

# -------------------------------------------------------------------
# 4) Spark Setup
# -------------------------------------------------------------------

# Kafka config
kafka_broker = "kafka:9092"
input_topics = "btcirt_topic,usdtirt_topic,ethirt_topic,etcirt_topic,shibirt_topic"
output_topic = "output_topic"

# Define schema for incoming JSON
schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("local_time", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# (A) Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", input_topics) \
    .option("startingOffsets", "earliest") \
    .load()

# (B) Extract columns from 'value' JSON
value_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# (C) foreachBatch => process_batch
query = value_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
