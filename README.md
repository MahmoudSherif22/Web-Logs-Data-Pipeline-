# Web-Scraping-Data-Pipeline-
Data Pipeline for webscraping and transforming raw data into table format 
![Example Image](Pipline-design/Pipline.jpeg)
## 📥 Data Ingestion with Apache Flume
This configuration sets up Apache Flume to ingest log data from a local file and send it to a Kafka topic (web2) in real-time. It demonstrates a typical file-to-Kafka pipeline using Flume's TailDir source, an in-memory channel, and a Kafka sink.
### ⚙️ Configuration Overview
Source:
Uses the TAILDIR source to monitor a log file:
/home/bigdata/flume-logs/input/web_data.txt
It continuously reads new lines appended to the file (like tail -f).

Channel:
An in-memory channel (memoryChannel) is used to buffer events between source and sink.

Sink:
Uses KafkaSink to push the events to a Kafka topic named web2 on localhost:9092.
## 🧩 Kafka Configuration (via Apache Flume)
This section defines the Kafka sink configuration in an Apache Flume pipeline. The goal is to stream real-time data from a local log file to a Kafka topic (web2) running on localhost:9092.

### 🔽 Kafka Sink Configuration Breakdown
Kafka Sink Type:

plaintext
Copy
Edit
org.apache.flume.sink.kafka.KafkaSink
The Flume sink used to send events to Kafka.

Kafka Bootstrap Servers:

plaintext
Copy
Edit
localhost:9092
Address of the Kafka broker the sink connects to.

Kafka Topic:

plaintext
Copy
Edit
web2
Destination Kafka topic where the data is published.

Acknowledgments:

plaintext
Copy
Edit
kafka.producer.acks = 1
Ensures messages are acknowledged by the Kafka leader only (balance of durability and performance).

Batch Settings:

kafka.flumeBatchSize = 20: Number of Flume events per Kafka producer batch.

kafka.producer.linger.ms = 1: Waits 1 ms before sending batches, improving throughput slightly.

Compression:

kafka.producer.compression.type = snappy: Compresses Kafka messages using Snappy for better performance and reduced network usage.

### 🧠 Purpose
This configuration enables seamless ingestion of real-time data from local logs into Kafka, making it suitable for:

Log aggregation

Real-time analytics

Event-driven pipelines using Kafka consumers (e.g., Spark, Flink, or custom apps)

## ⚡ Real-Time Data Processing with PySpark and Kafka
This Spark application leverages Structured Streaming in PySpark to consume, parse, and display real-time data from a Kafka topic (web2). The application is designed to work with data ingested via Apache Flume, where logs are forwarded to Kafka in JSON array format.
### 🔧 Key Features
Spark Streaming + Kafka Integration:
Reads streaming data directly from Kafka using the spark-sql-kafka-0-10 connector.

JSON Array Parsing:
Incoming Kafka messages are expected to be JSON arrays of objects (e.g., multiple log entries). These are parsed using a user-defined schema.

Data Explosion:
Each object in the JSON array is extracted using explode(), producing one row per record for structured processing.

Console Output:
Parsed records are printed in real-time to the console using writeStream with append mode.



