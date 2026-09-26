# Real-Time Streaming Pipeline with Schema Evolution

A local, reproducible data-engineering project that ingests live weather data, publishes it to a Kafka-compatible broker, evolves the source schema mid-stream, validates records, routes malformed records to a dead-letter queue (DLQ), and processes the stream with Spark Structured Streaming.

## Architecture

Open-Meteo API
-> Python Producer
-> Redpanda (Kafka-compatible)
-> Spark Structured Streaming
-> Delta Lake

Invalid records -> weather-dlq

Schema Registry stores and checks schema versions.

## Tech stack

- Python
- Redpanda / Kafka
- Confluent Schema Registry
- Avro schemas
- Apache Spark Structured Streaming
- Delta Lake
- Docker Compose

## What the project demonstrates

- Streaming ingestion
- Schema versioning and compatibility
- Backward-compatible schema evolution
- Data validation
- Dead-letter queue handling
- Spark Structured Streaming
- Lakehouse storage with Delta Lake

## Run

### 1. Start infrastructure

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

### 2. Install producer dependencies

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
# source .venv/bin/activate

pip install -r producer/requirements.txt
```

### 3. Start the producer

```bash
python producer/producer.py
```

The producer starts with schema v1:

```text
location, temperature, timestamp
```

After `EVOLVE_AFTER` records it switches to schema v2:

```text
location, temperature, timestamp, humidity
```

The v2 schema adds `humidity` with a default value, making the evolution backward-compatible.

### 4. Run Spark

Install Spark/PySpark locally, or use an existing Spark installation with the Delta Lake and Kafka packages.

Example:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 \
  spark/streaming_job.py
```

For Spark versions other than 3.5.x, change the package versions to match your Spark/Scala installation.

### 5. Inspect output

The streaming job writes:

```text
output/delta/weather
output/delta/dlq
```

## Schema evolution

The compatibility policy is `BACKWARD`.

v1:

```json
{
  "type": "record",
  "name": "Weather",
  "fields": [
    {"name": "location", "type": "string"},
    {"name": "temperature", "type": "double"},
    {"name": "timestamp", "type": "string"}
  ]
}
```

v2:

```json
{
  "type": "record",
  "name": "Weather",
  "fields": [
    {"name": "location", "type": "string"},
    {"name": "temperature", "type": "double"},
    {"name": "timestamp", "type": "string"},
    {"name": "humidity", "type": ["null", "double"], "default": null}
  ]
}
```

Adding a nullable/defaulted field allows old consumers to continue reading old data.

## Failure scenarios

The producer can deliberately emit malformed records:

- missing `temperature`
- non-numeric temperature
- missing `location`

These records are sent to `weather-dlq` and are not allowed to terminate the stream.

## GitHub screenshots to add

1. `docker compose ps`
2. Producer showing schema v1 -> v2
3. Schema Registry subjects/versions
4. Spark streaming console
5. Delta output
6. DLQ records

## Disclaimer

This project uses synthetic/public weather data. It is a learning project and not a production trading or operational system.
