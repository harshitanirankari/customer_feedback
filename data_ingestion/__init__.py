# data_ingestion/__init__.py
"""
Data Ingestion Package
----------------------
Components for ingesting data from various sources and moving it to storage.
"""

# Import key modules to make them available directly from the package
from . import utils
from . import reddit_to_kafka_refactored
from . import kafka_to_snowflake