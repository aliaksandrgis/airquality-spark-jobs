FROM bitnami/spark:3.5.0

USER root
WORKDIR /opt/airquality

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs ./jobs

ENV PYTHONPATH=/opt/airquality

ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit", "--packages", "org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4"]
CMD ["jobs/live_measurements.py"]
