FROM openjdk:8-jdk-slim

# Install Python, wget, and other dependencies
RUN apt-get update && apt-get install -y python3 python3-pip wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir --timeout=300 -r requirements.txt

# Copy application code
COPY app /app

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Download and install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz | tar xvz -C /opt && \
    mv /opt/spark-3.1.2-bin-hadoop3.2 /opt/spark

# Run the consumer
CMD ["spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "/app/consumer.py"]