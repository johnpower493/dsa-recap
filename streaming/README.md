# Real-time Streaming Data Engineering

This module covers essential patterns and technologies for building real-time data pipelines.

## Technologies Covered
- **Kafka**: Distributed event streaming platform
- **Kinesis**: AWS streaming service
- **Flink**: Stateful stream processing
- **Spark Streaming**: Micro-batch processing
- **Pub/Sub**: Google Cloud messaging

## Core Concepts

### 1. Producer Patterns
- At-least-once delivery
- Partitioning strategies
- Batching and compression
- Backpressure handling

### 2. Consumer Patterns
- Consumer groups and rebalancing
- Exactly-once semantics
- Offsets management
- Dead letter queues

### 3. Stream Processing
- Windowing (tumbling, sliding, session)
- State management
- Time semantics (event time vs processing time)
- Late arrival handling
- Watermarks

### 4. Real-time Aggregations
- Count-based windows
- Time-based windows
- Session windows
- Dynamic aggregations

## Exercises

1. **Basic Kafka Producer** - `01_kafka_producer_exercise.py`
2. **Kafka Consumer with Rebalancing** - `02_kafka_consumer_exercise.py`
3. **Exactly-Once Semantics** - `03_exactly_once_exercise.py`
4. **Windowed Aggregations** - `04_windowing_exercise.py`
5. **Late Arrival Handling** - `05_late_arrival_exercise.py`
6. **Backpressure Management** - `06_backpressure_exercise.py`
7. **Kinesis Integration** - `07_kinesis_exercise.py`
8. **Pub/Sub Patterns** - `08_pubsub_exercise.py`

## Learning Path

1. Start with basic producer/consumer patterns
2. Master exactly-once semantics
3. Learn windowing and aggregations
4. Handle edge cases (late data, backpressure)
5. Integrate with cloud services

## Prerequisites

- Docker (for local Kafka/Kinesis)
- Python 3.9+
- Familiarity with basic data engineering concepts

## Running Exercises

```bash
# Start local Kafka
docker-compose up -d

# Run exercises
python streaming/01_kafka_producer_exercise.py