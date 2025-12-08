Obzor
=====
Eto repohranilishe opisivaet Spark streaming sloi platformy AirQuality. Produser (repo `airquality-data-pipeline`) poluchaet izmereniya iz API stran NL/DE/PL i otpravlyaet v Kafka Confluent (`airquality.raw`). Dannye cherez Spark Structured Streaming popadayut v bronzovyi sloi (Parquet na lokanom diske Pi) i v Supabase tabelicu `public.measurements_curated`. Produser generiruet 10-15k soobshenii za cikl (obnovlenie API ~1ch), poetomu Spark nuzhno derzhat zapushennym 24/7.

Struktura proekta
-----------------
```
airquality-data-pipeline -> Kafka topic airquality.raw -> Spark job (repo) -> Supabase Postgres
                                                   \-> file:///home/pc/bronze (bronze)
```

Zapuskaetsya na Raspberry Pi 4 (Debian 13) s ustanovlennymi openjdk-21, python3.13, pipenv/venv. Spark stavitsya globalno (apt `spark`) ili raspakovyvaetsya v `/home/pc/spark`. Git repo kloniruetsya v `/home/pc/airquality-spark-jobs`.

Ustanovka na Pi
---------------
1. Ustanovit paket:
   ```
   sudo apt update
   sudo apt install -y openjdk-21-jre-headless python3 python3-venv python3-pip git
   ```
2. Klon:
   ```
   git clone https://github.com/<org>/airquality-spark-jobs.git
   cd airquality-spark-jobs
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -U pip
   pip install -r requirements.txt
   ```
3. Sozdat katalogi:
   ```
   mkdir -p /home/pc/bronze /home/pc/tmp
   ```

Peremennye okruzheniya (`.env`)
-------------------------------
Fail `airquality-spark-jobs/.env` (bez realnyh znachenii):
```
KAFKA_BOOTSTRAP=pkc-<cluster>.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<CONFLUENT_KEY>
KAFKA_SASL_PASSWORD=<CONFLUENT_SECRET>

SUPABASE_JDBC_URL=jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require
SUPABASE_USER=<SUPABASE_USER>
SUPABASE_PASSWORD=<SUPABASE_PASSWORD>

BRONZE_PATH=file:///home/pc/bronze
BRONZE_CHECKPOINT_PATH=file:///home/pc/bronze/_checkpoints/live
SUPABASE_CHECKPOINT_PATH=file:///home/pc/bronze/_checkpoints/supabase
```
Zagruzka v PowerShell (`scripts/with-env.ps1`) ili v bash:
```
set -a; source .env; set +a
```

JAR zavisimosti
---------------
Skachat i polozhit v `airquality-spark-jobs/jars`:
```
aws-java-sdk-core-1.12.648.jar
aws-java-sdk-s3-1.12.648.jar
aws-java-sdk-sts-1.12.648.jar
commons-pool2-2.11.1.jar
hadoop-aws-3.3.4.jar
kafka-clients-3.5.2.jar
postgresql-42.7.1.jar
spark-sql-kafka-0-10_2.12-3.5.4.jar
spark-token-provider-kafka-0-10_2.12-3.5.4.jar
```

Primer rucnogo zapuska
----------------------
```
export TMPDIR=/home/pc/tmp
spark-submit \
  --conf spark.local.dir=/home/pc/tmp \
  --conf spark.driver.host=192.168.0.200 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.blockManager.port=40493 \
  --conf spark.ui.port=4040 \
  --jars jars/postgresql-42.7.1.jar,jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-core-1.12.648.jar,jars/aws-java-sdk-s3-1.12.648.jar,jars/aws-java-sdk-sts-1.12.648.jar,jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar,jars/kafka-clients-3.5.2.jar,jars/commons-pool2-2.11.1.jar \
  jobs/live_measurements.py
```

Script i systemd
----------------
- `scripts/run_live_measurements.sh` (bash) avtomaticheski aktiviruet `.venv`, zagruzhaet `.env`, nastraivaet `TMPDIR` i vyzyvaet `spark-submit` s nuzhnymi JAR.
- Systemd unit primer lezhit v `systemd/airquality-live.service`. Skopeeruy ego v `/etc/systemd/system/`, otredaktiruy `User`/`WorkingDirectory`, zatem:
  ```bash
  sudo systemctl daemon-reload
  sudo systemctl enable airquality-live
  sudo systemctl start airquality-live
  journalctl -u airquality-live -f
  ```
  Logi skripta takzhe pisutsya v `logs/live_measurements.log`.

Kafka topic
-----------
`airquality.raw`: 1 partition, retention time 1 day, retention size 1 GB, cleanup policy Delete, max message 2 MB. Pri peresozdanii nuzhno ostanovit spark, udalit topic, sozdat s parametrami, zapustit produser i Spark.

Supabase
--------
Tablica `public.measurements_curated` s PK `id` i unikom `(station_id, pollutant, observed_at)`. Pri oshibkah `Tenant or user not found` proverit, chto ispolzuetsya pooler host `aws-1-eu-west-1.pooler.supabase.com` i user `postgres.<project_id>`. Test:
```
PGPASSWORD="<PASS>" psql -h aws-1-eu-west-1.pooler.supabase.com \
  -U "<USER>" -d postgres -c "select 1;" sslmode=require
```

Diagnostika
-----------
- Loglevel: `tail -f /home/pc/airquality-spark-jobs/logs/live_measurements.log`
- Systemd: `journalctl -u airquality-live -f`
- Disk: `df -h`, `du -sh /home/pc/airquality-spark-jobs /home/pc/bronze /home/pc/tmp`
- Kafka offset reset: peresozdat topic ili ispolzovat Confluent UI.
- Supabase: `select count(*) from measurements_curated;`

Svyaz s produserom
------------------
Produser (`python -m app.main` iz repo `airquality-data-pipeline`) dolzhen byt zapushen pered Spark. On zapisivaet tolko v Kafka (Postgres catalog optional). Spark job chitayet tolko novye offsety (latest). Esli Kafka pusta, Spark budet v sostoyanii idle (`Streaming query has been idle...`). Pri zalive bolshogo istoricheskogo obema nuzhno vremenno ostanovit systemd, zapustit `spark-submit` rucno, dozhdatsya okonchaniya, zatem vernutsya k servisu.
