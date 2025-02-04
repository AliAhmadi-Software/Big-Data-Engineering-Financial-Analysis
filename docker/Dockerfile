# استفاده از تصویر پایه OpenJDK 11
FROM openjdk:11

# تعریف نسخه‌های Spark و Hadoop
ENV SPARK_VERSION=3.2.0
ENV HADOOP_VERSION=3.3.1

# نصب ابزارهای لازم و dos2unix
RUN apt-get update && apt-get install -y curl dos2unix python3 python3-pip

# دانلود و نصب Hadoop
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
        tar -zx hadoop-${HADOOP_VERSION}/lib/native && \
    ln -s hadoop-${HADOOP_VERSION} hadoop && \
    echo "Hadoop ${HADOOP_VERSION} native libraries installed in /opt/hadoop/lib/native"

# دانلود و نصب Spark
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz | \
        tar -zx && \
    ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 spark && \
    echo "Spark ${SPARK_VERSION} installed in /opt"

# کپی اسکریپت‌ها و تبدیل آن‌ها به فرمت لینوکس
ADD common.sh spark-master spark-worker /
RUN dos2unix /common.sh /spark-master /spark-worker && \
    chmod +x /spark-master /spark-worker

# کپی فایل پیکربندی Spark و تبدیل آن به فرمت لینوکس
ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN dos2unix /opt/spark/conf/spark-defaults.conf

# تنظیم متغیر محیطی PATH
ENV PATH $PATH:/opt/spark/bin

# نصب وابستگی‌های پایتون
RUN pip3 install pandas kafka-python psycopg2-binary

# کپی فایل consumer.py به داخل Image
COPY consumer.py /opt/spark/work-dir/consumer.py

# تنظیم مسیر کاری
WORKDIR /opt/spark/work-dir

# دستور پیش‌فرض برای اجرای برنامه
CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1", \
     "consumer.py"]