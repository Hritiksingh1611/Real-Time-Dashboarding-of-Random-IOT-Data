# Streaming Data with Kafka KRaft

## Overview

This project simulates IoT sensor data, streams it through Apache Kafka in KRaft mode, and stores it in Cassandra. It now also includes two browser-based visual tools:

- Kafka UI for inspecting the Kafka topic and broker state
- Streamlit dashboard for charting temperature and humidity from Cassandra

## Components

- `iot_data.py`: generates fake IoT events and sends them to Kafka
- `stream_iot_data.py`: consumes Kafka events and inserts them into Cassandra
- `docker-compose.yml`: starts Kafka, Cassandra, Kafka UI, and the dashboard
- `dashboard.py`: Streamlit app for live charts and recent readings

## Visual URLs

- Kafka UI: `http://localhost:18080`
- Streamlit Dashboard: `http://localhost:18501`

## Runtime Ports

- Kafka external listener: `19092`
- Kafka controller: `19093`
- Cassandra: `19042`
- Kafka UI: `18080`
- Streamlit dashboard: `18501`

## How To Run

Start the infrastructure:

```bash
docker compose up -d
```

Start the producer and consumer if they are not already running:

```bash
docker logs -f rt-py-producer
docker logs -f rt-py-consumer
```

## What You Can See

In Kafka UI:

- the `iot_data` topic
- broker information
- live messages moving through Kafka

In the Streamlit dashboard:

- line charts for temperature by device
- line charts for humidity by device
- recent readings in a table
- quick metrics for row count, devices, latest temperature, and latest humidity

## Directory Structure

```text
README.md
dashboard.py
dashboard-requirements.txt
docker-compose.yml
iot_data.py
stream_iot_data.py
image.jpg
```
