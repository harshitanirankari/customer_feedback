# Customer Feedback Insights System

This project implements a data pipeline for collecting and analyzing customer feedback from various sources.

## Project Architecture

- **Data Ingestion**: Kafka (Reddit, potentially other sources)
- **Data Warehouse**: Snowflake
- **ETL/Transform**: Python
- **Task Scheduling**: Snowflake TaskScheduler
- **CI/CD**: GitHub Actions / GitLab CI / Jenkins
- **Chatbot Interface**: For insights
- **Deployment**: Docker + CI/CD pipeline

## Setup Instructions

### Prerequisites

- Python 3.x
- Apache Kafka
- Reddit API credentials

### Installation

1. Install required packages:
   ```
   pip install -r requirements.txt
   ```

### Kafka Setup (Windows)

1. Download Kafka from: https://kafka.apache.org/downloads

2. Extract the downloaded file to a location like `C:\kafka`

3. Start Zookeeper (in one command prompt):
   ```
   cd C:\kafka
   .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
   ```

4. Start Kafka server (in another command prompt):
   ```
   cd C:\kafka
   .\bin\windows\kafka-server-start.bat .\config\server.properties
   ```

5. Create the Kafka topic (in a third command prompt):
   ```
   cd C:\kafka
   .\bin\windows\kafka-topics.bat --create --topic reddit_feedback --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

### Running the Data Ingestion

1. Start the Reddit to Kafka script:
   ```
   python data_ingestion/reddit_to_kafka.py
   ```

2. To verify data is being ingested properly, run the consumer test script in another terminal:
   ```
   python data_ingestion/kafka_consumer_test.py
   ```

## Project Components

- `data_ingestion/`: Scripts for collecting data from various sources
  - `reddit_to_kafka.py`: Streams Reddit comments to Kafka
  - `kafka_consumer_test.py`: Test consumer to verify Kafka messages

## Next Steps

- Implement ETL process to move data from Kafka to Snowflake
- Set up Snowflake tables and schema
- Create transformation scripts
- Configure Snowflake TaskScheduler
- Develop chatbot interface for insights
- Set up CI/CD pipeline with Docker
