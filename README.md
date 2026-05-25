# Real-time Server Monitoring Distributed Pipeline with Apache Kafka and Spark

Technologies: Apache Kafka · Apache Spark · ZeroTier

---

## 1. Introduction

In large-scale distributed systems, real-time server monitoring is essential to ensure that system resources such as CPU, memory, disk, and network I/O are performing within expected thresholds. This assignment demonstrates a real-time data pipeline that ingests simulated server metrics, streams them through Apache Kafka, and analyzes them using Apache Spark (batch jobs on CSVs produced by Kafka consumers). ZeroTier is used to interconnect machines across networks.

Each team will simulate a multi-node environment: a Producer, a Kafka Broker, and two Consumer/Spark nodes. The producer streams dataset records into four Kafka topics. Consumers write raw CSVs locally and Spark jobs perform window-based aggregation and anomaly detection.

---

## 2. Objective

Build a distributed monitoring pipeline that:
- Streams server metrics into Kafka topics.
- Stores incoming data to local CSVs on consumer machines.
- Runs PySpark jobs on those CSVs (not Structured Streaming) to compute sliding-window aggregates and raise alerts.
- Uses ZeroTier for secure connectivity between nodes.

Key components:
- Kafka for real-time messaging
- Spark (PySpark) for window-based aggregation and alerting
- ZeroTier for networking across machines

Two independent consumers process different metric groups in parallel:
- Consumer 1: CPU & Memory (runs `Team_157_spark_job_1.py`)
- Consumer 2: Network & Disk (runs `Team_157_spark_job_2.py`)

---

## 3. System Roles

Each team configures four machines with the following roles:

- Producer Machine — publishes server metric messages to Kafka topics.
- Broker Machine — runs Kafka broker (and ZooKeeper if needed) and exposes port `9092`.
- Consumer 1 Machine — subscribes to `topic-cpu` and `topic-mem`, writes `cpu_data.csv` and `mem_data.csv`, runs `Team_157_spark_job_1.py` to produce `team_NO_CPU_MEM.csv`.
- Consumer 2 Machine — subscribes to `topic-net` and `topic-disk`, writes `net_data.csv` and `disk_data.csv`, runs `Team_157_spark_job_2.py` to produce `team_NO_NET_DISK.csv`.

Important: ensure all machines can reach the Kafka broker at `broker_ip:9092` over the ZeroTier network.

---

## 4. Data Flow Overview

1. The producer reads the provided dataset CSV and publishes rows to four topics: `topic-cpu`, `topic-mem`, `topic-net`, `topic-disk`.
2. Consumers subscribe to their topics and append incoming records to local CSV files.
3. After ingestion (or continuously on schedule), each consumer runs the corresponding PySpark job that reads the CSV(s) and computes windowed aggregates.

Window parameters used by the Spark jobs:
- Window size = 30 seconds
- Slide interval = 10 seconds
- Aggregations are computed per `server_id`

Note: Use PySpark batch jobs reading CSV files — Spark Structured Streaming is prohibited for this assignment.

---

## 5. Dataset & Metrics

Dataset schema (CSV columns):

`ts, server_id, cpu_pct, mem_pct, net_in, net_out, disk_io`

- `ts`: Timestamp of the record (expected format: `HH:MM:SS`).
- `server_id`: Unique server identifier.
- `cpu_pct`: CPU usage percentage.
- `mem_pct`: Memory usage percentage.
- `net_in`: Incoming network traffic (numeric).
- `net_out`: Outgoing network traffic (numeric).
- `disk_io`: Disk read/write activity (numeric).

Each team receives a dataset with team-specific values and thresholds. Stream the dataset from the producer to demonstrate real-time ingestion.

---

## 6. Alerting Logic

Apply alert thresholds provided with your team dataset. The example rules below show the logic; adapt threshold values as supplied to your team.

Consumer 1 (CPU + Memory):

- If avg(cpu_pct) > threshold AND avg(mem_pct) > threshold → "High CPU + Memory stress"
- If avg(cpu_pct) > threshold AND avg(mem_pct) ≤ threshold → "CPU spike suspected"
- If avg(mem_pct) > threshold AND avg(cpu_pct) ≤ threshold → "Memory saturation suspected"

Consumer 2 (Network + Disk):

- If max(net_in) > threshold AND max(disk_io) > threshold → "Network flood + Disk thrash suspected"
- If max(net_in) > threshold AND max(disk_io) ≤ threshold → "Possible DDoS"
- If max(disk_io) > threshold AND max(net_in) ≤ threshold → "Disk thrash suspected"

All numeric output values (averages, maxima) must be formatted to two decimal places in final CSV outputs.

---

## 7. Expected Outputs

After running the pipeline and Spark jobs, the following files should be produced per team:

- `cpu_data.csv`, `mem_data.csv` — raw CPU & memory data produced by Consumer 1
- `net_data.csv`, `disk_data.csv` — raw network & disk data produced by Consumer 2
- `team_NO_CPU_MEM.csv` — Spark Job 1 alerts (schema below)
- `team_NO_NET_DISK.csv` — Spark Job 2 alerts (schema below)

Output schema for CPU_MEM (Spark Job 1):

`server_id,window_start,window_end,avg_cpu,avg_mem,alert`

Output schema for NET_DISK (Spark Job 2):

`server_id,window_start,window_end,max_net_in,max_disk_io,alert`

Timestamps in `window_start` and `window_end` must use `HH:MM:SS` format (local time), not UTC literals.

