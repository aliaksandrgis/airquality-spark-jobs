FROM apache/spark:3.5.0

USER root
WORKDIR /opt/airquality

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY jobs ./jobs

ENV PYTHONPATH=/opt/airquality \
    PYSPARK_PYTHON=python3

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4"]
CMD ["jobs/live_measurements.py"]
