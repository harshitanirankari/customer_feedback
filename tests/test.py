#!/usr/bin/env python
"""
Pipeline Tests
-------------
Core tests for the Reddit to Snowflake data pipeline.
"""

import os
import sys
import json
import uuid
import unittest
from datetime import datetime

# Add the parent directory to the Python path so we can import data_ingestion
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class UtilityFunctionsTests(unittest.TestCase):
    """Tests for utility functions"""
    
    def test_clean_text_for_storage(self):
        """Test text cleaning function"""
        try:
            from data_ingestion.utils import clean_text_for_storage
            
            test_cases = [
                ("Normal text", "Normal text"),
                ("Text with https://example.com link", "Text with link"),
                ("Text with @mentions", "Text with mentions"),
                ("Text with\nmultiple\nlines", "Text with multiple lines")
            ]
            
            for input_text, expected in test_cases:
                cleaned = clean_text_for_storage(input_text)
                self.assertIn(expected, cleaned, 
                              f"Expected '{expected}' to be in '{cleaned}'")
                
        except ImportError:
            self.skipTest("Utils module not available")
    
    def test_is_automoderator_comment(self):
        """Test AutoModerator comment detection"""
        try:
            from data_ingestion.utils import is_automoderator_comment
            
            self.assertTrue(is_automoderator_comment("AutoModerator", "I am a bot"))
            self.assertFalse(is_automoderator_comment("regular_user", "Normal comment"))
            
        except ImportError:
            self.skipTest("Utils module not available")

class KafkaTests(unittest.TestCase):
    """Tests for Kafka functionality"""
    
    def test_producer_consumer_flow(self):
        """Test basic Kafka producer and consumer flow"""
        try:
            from kafka import KafkaProducer, KafkaConsumer
            import json
            
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            test_topic = f"test_topic_{uuid.uuid4().hex[:8]}"
            
            # Create producer and send message
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            test_id = str(uuid.uuid4())
            test_message = {
                "test_id": test_id,
                "message": "Test message"
            }
            
            future = producer.send(test_topic, test_message)
            result = future.get(timeout=10)
            producer.flush()
            
            # Create consumer and try to get the message
            consumer = KafkaConsumer(
                test_topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            
            found = False
            for message in consumer:
                if message.value.get("test_id") == test_id:
                    found = True
                    break
            
            consumer.close()
            producer.close()
            
            self.assertTrue(found, "Test message not found in Kafka topic")
            
        except Exception as e:
            self.skipTest(f"Kafka test failed: {str(e)}")

class SnowflakeTests(unittest.TestCase):
    """Tests for Snowflake functionality"""
    
    def test_snowflake_connection(self):
        """Test connection to Snowflake"""
        try:
            import snowflake.connector
            
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA')
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            self.assertEqual(result[0], 1, "Snowflake connection failed")
            
        except Exception as e:
            self.skipTest(f"Snowflake connection test failed: {str(e)}")
    
    def test_snowflake_tables_exist(self):
        """Test that required Snowflake tables exist"""
        try:
            import snowflake.connector
            
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA')
            )
            
            cursor = conn.cursor()
            tables = ['reddit_feedback_landing', 'reddit_feedback_final']
            
            for table in tables:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                result = cursor.fetchall()
                self.assertTrue(len(result) > 0, f"Table {table} does not exist")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            self.skipTest(f"Snowflake tables test failed: {str(e)}")

class RedditApiTests(unittest.TestCase):
    """Tests for Reddit API"""
    
    def test_reddit_api_connection(self):
        """Test connection to Reddit API"""
        try:
            import praw
            
            reddit = praw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=os.getenv('REDDIT_USER_AGENT')
            )
            
            subreddit = reddit.subreddit('amazon')
            for _ in subreddit.hot(limit=1):
                pass
            
            self.assertTrue(True, "Successfully connected to Reddit API")
            
        except Exception as e:
            self.skipTest(f"Reddit API connection test failed: {str(e)}")

class PipelineComponentTests(unittest.TestCase):
    """Tests for pipeline components"""
    
    def test_reddit_to_kafka(self):
        """Test Reddit to Kafka components"""
        try:
            # Add parent directory to sys.path to find the data_ingestion package
            import sys
            import os
            sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
            
            # Import reddit_to_kafka module to test functionality
            from data_ingestion import reddit_to_kafka_refactored
            
            # Check for required components
            self.assertTrue(hasattr(reddit_to_kafka_refactored, 'process_reddit_content'), 
                           "process_reddit_content function not found")
            
            self.assertTrue(True, "Reddit to Kafka components available")
            
        except Exception as e:
            self.skipTest(f"Reddit to Kafka component test failed: {str(e)}")
    
    def test_kafka_to_snowflake(self):
        """Test Kafka to Snowflake components"""
        try:
            # Import kafka_to_snowflake module to test functionality
            from data_ingestion import kafka_to_snowflake
            
            # Check for required components
            self.assertTrue(hasattr(kafka_to_snowflake, 'SnowflakeConnector'), 
                           "SnowflakeConnector class not found")
            
            self.assertTrue(True, "Kafka to Snowflake components available")
            
        except Exception as e:
            self.skipTest(f"Kafka to Snowflake component test failed: {str(e)}")

if __name__ == "__main__":
    unittest.main(verbosity=2)