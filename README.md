# Real-Time Transaction Analytics Suite

This project is a comprehensive suite of real-time streaming applications built using Apache Spark Structured Streaming and Kafka, designed to perform advanced analytics on financial transaction data.

## Components

### 1. **Fraud Detection System**
Detects fraudulent transactions in real-time using:
- **Velocity Check:** Flags excessive transactions from a user in a short period.
- **Geographic Anomalies:** Identifies location-based impossibilities.
- **Amount Outliers:** Detects transactions significantly higher than the user’s average.

### 2. **Commission Analytics Dashboard**
Calculates real-time metrics on commission data:
- Aggregated commission per type and merchant category.
- Commission ratios.
- Top commission-generating merchants.

### 3. **Streaming Insights Application**
Performs time-windowed aggregations:
- 1-minute windows with 20-second slides.
- Tracks transaction volume and total amount per merchant category.

## Technologies
- Apache Spark 3.5.0
- Kafka 2.12
- Python (PySpark)
- Real-time JSON processing
- Structured Streaming with watermarks and sliding windows

## Usage
Each module reads from a Kafka topic (`darooghe.transactions`), performs transformations and analytics, and outputs to:
- Console (for debugging)
- Dedicated Kafka topics (e.g., `darooghe.fraud_alerts`, `darooghe.commission.ratio`)

## Fault Tolerance
Checkpoints are configured to ensure recovery and fault tolerance in the event of failures.

## Note
For local testing, ensure Kafka is running on `localhost:9092` and the required topics are pre-created.