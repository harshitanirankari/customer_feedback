# Customer Feedback Analysis System - Docker Setup

This document provides instructions for running the Customer Feedback Analysis System using Docker.

## Prerequisites

- Docker and Docker Compose installed on your system
- Reddit API credentials (client ID, client secret, and user agent)
- Snowflake account credentials

## Environment Variables

Before running the application, you need to set up your environment variables. The application uses the following environment variables:

- `REDDIT_CLIENT_ID`: Your Reddit API client ID
- `REDDIT_CLIENT_SECRET`: Your Reddit API client secret
- `REDDIT_USER_AGENT`: Your Reddit API user agent
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (automatically set in Docker environment)
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_PASSWORD`: Your Snowflake password
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_WAREHOUSE`: Your Snowflake warehouse name
- `SNOWFLAKE_DATABASE`: Your Snowflake database name
- `SNOWFLAKE_SCHEMA`: Your Snowflake schema name

These variables can be set in a `.env` file at the root of the project.

## Running with Docker Compose

1. Build and start the containers:
   ```bash
   docker-compose up -d
   ```

2. Check the status of the containers:
   ```bash
   docker-compose ps
   ```

3. View logs from the containers:
   ```bash
   docker-compose logs -f
   ```

4. Stop the containers:
   ```bash
   docker-compose down
   ```

## Components

The Docker setup includes the following components:

- **Zookeeper**: Required for Kafka operation
- **Kafka**: Message broker for streaming data
- **Kafka Setup**: Creates the necessary Kafka topics
- **Feedback Analyzer**: The main application that processes Reddit data and loads it into Snowflake

## Running Individual Components

You can run specific components of the pipeline using the `run_pipeline.py` script:

- **Full Pipeline**: `docker-compose run feedback-analyzer python run_pipeline.py full`
- **Data Ingestion Only**: `docker-compose run feedback-analyzer python run_pipeline.py ingest`
- **Data Processing Only**: `docker-compose run feedback-analyzer python run_pipeline.py process`
- **Run Tests**: `docker-compose run feedback-analyzer python run_pipeline.py test`

## Building for CI/CD

The Docker setup is designed to be integrated with CI/CD pipelines. The Dockerfile and docker-compose.yml files provide the foundation for setting up automated build, test, and deployment processes.

For CI/CD integration, you'll need to:

1. Store sensitive environment variables securely in your CI/CD platform
2. Configure build and test stages using the provided Docker configuration
3. Set up deployment stages for your target environment

## Troubleshooting

- **Kafka Connection Issues**: Ensure that Kafka and Zookeeper are running properly. Check the logs with `docker-compose logs kafka`.
- **Reddit API Issues**: Verify your Reddit API credentials in the `.env` file.
- **Snowflake Connection Issues**: Confirm your Snowflake credentials and ensure the warehouse, database, and schema exist.
