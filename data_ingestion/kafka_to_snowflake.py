import os
import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
import snowflake.connector

# Import sentiment analyzer and utility functions
from sentiment_analyzer import SentimentAnalyzer
from utils import clean_text_for_storage, is_automoderator_comment

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """Class to handle Snowflake connection and data insertion"""

    def __init__(self, batch_size=10, flush_interval=60):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.batch = []
        self.last_flush_time = time.time()
        
        # Set to track processed comment IDs for deduplication
        self.processed_ids = set()
        logger.info("Initialized deduplication tracking for comments")

        try:
            self.conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'reddit_wh'),
                database=os.getenv('SNOWFLAKE_DATABASE', 'reddit_feedback_db'),
                schema=os.getenv('SNOWFLAKE_SCHEMA', 'reddit_schema')
            )
            logger.info("Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {str(e)}")
            logger.warning("Will proceed in simulation mode without actual Snowflake connection")
            self.conn = None

    def is_automoderator_comment_record(self, record):
        """
        Secondary check to filter out any AutoModerator comments that might have slipped through
        
        Args:
            record (dict): The Kafka record to check
            
        Returns:
            bool: True if the record is from an AutoModerator and should be filtered out
        """
        author = record.get('AUTHOR', '').lower()
        text = record.get('TEXT', '').lower()
        
        result = is_automoderator_comment(author, text)
        if result:
            logger.info(f"Filtered out AutoModerator comment at Snowflake stage")
        
        return result

    def add_record(self, record):
        # Additional filter for AutoModerator comments
        if self.is_automoderator_comment_record(record):
            return
            
        # Check for duplicate comment IDs
        comment_id = record.get('COMMENT_ID', '')
        if comment_id and comment_id in self.processed_ids:
            logger.info(f"Skipping duplicate comment ID: {comment_id}")
            return
        
        # Track this comment ID to prevent future duplicates
        if comment_id:
            self.processed_ids.add(comment_id)
            # Log every 100th processed ID to show progress
            if len(self.processed_ids) % 100 == 0:
                logger.info(f"Now tracking {len(self.processed_ids)} unique comment IDs")
            
        self.batch.append(record)
        current_time = time.time()
        if len(self.batch) >= self.batch_size or (current_time - self.last_flush_time) >= self.flush_interval:
            self.flush_batch()

    def flush_batch(self):
        if not self.batch:
            logger.debug("No records to flush")
            return True

        logger.info(f"Flushing batch of {len(self.batch)} records to Snowflake")

        try:
            snowflake_records = []
            for record in self.batch:
                try:
                    # Convert lists to JSON strings for Snowflake storage
                    topics_json = json.dumps(record.get('TOPICS', []))
                    keywords_json = json.dumps(record.get('KEYWORDS', []))

                    sentiment_score = record.get('SENTIMENT_SCORE', 0.0)
                    sentiment_category = record.get('SENTIMENT_CATEGORY', 'neutral')

                    # Clean the text to remove extremely long content or unnecessary formatting
                    cleaned_text = self.prepare_text_for_snowflake(record.get('TEXT', ''))
                    cleaned_title = self.prepare_text_for_snowflake(record.get('POST_TITLE', ''))

                    snowflake_record = {
                        'COMMENT_ID': record.get('COMMENT_ID', ''),
                        'AUTHOR': record.get('AUTHOR', 'unknown'),
                        'TEXT': cleaned_text,
                        'SUBREDDIT': record.get('SUBREDDIT', ''),
                        'CREATED_DATE': record.get('CREATED_DATE', ''),
                        'PERMALINK': record.get('PERMALINK', ''),
                        'POST_TITLE': cleaned_title,
                        'POST_ID': record.get('POST_ID', ''),
                        'SCORE': record.get('SCORE', 0),
                        'INGESTION_TIME': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'PROCESSED_AT': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'PLATFORM': record.get('PLATFORM', 'unknown'),
                        'TOPICS': topics_json,
                        'KEYWORDS': keywords_json,  # Store RAKE keywords
                        'SENTIMENT_SCORE': sentiment_score,
                        'SENTIMENT_CATEGORY': sentiment_category,
                        'FEEDBACK_TYPE': record.get('FEEDBACK_TYPE', 'general')
                    }
                    snowflake_records.append(snowflake_record)
                except KeyError as e:
                    logger.error(f"Missing key in record: {e}")
                    logger.debug(f"Record structure: {json.dumps(record, indent=2)}")
                    continue

            if snowflake_records:
                logger.info(f"Sample record: {json.dumps(snowflake_records[0], indent=2)}")
            else:
                logger.warning("No valid records to send to Snowflake")
                return False

            if self.conn:
                cursor = self.conn.cursor()
                insert_count = 0
                for record in snowflake_records:
                    try:
                        insert_sql = """
                        INSERT INTO reddit_feedback_landing (
                            COMMENT_ID, AUTHOR, TEXT, SUBREDDIT, CREATED_DATE, 
                            PERMALINK, POST_TITLE, POST_ID, SCORE, INGESTION_TIME,
                            PROCESSED_AT, PLATFORM, TOPICS, KEYWORDS, SENTIMENT_SCORE, 
                            SENTIMENT_CATEGORY, FEEDBACK_TYPE
                        ) 
                        SELECT
                            %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s,
                            %s, %s, TRY_PARSE_JSON(%s), TRY_PARSE_JSON(%s), %s, 
                            %s, %s
                        """

                        cursor.execute(
                            insert_sql,
                            (
                                record['COMMENT_ID'],
                                record['AUTHOR'],
                                record['TEXT'],
                                record['SUBREDDIT'],
                                record['CREATED_DATE'],
                                record['PERMALINK'],
                                record['POST_TITLE'],
                                record['POST_ID'],
                                record['SCORE'],
                                record['INGESTION_TIME'],
                                record['PROCESSED_AT'],
                                record['PLATFORM'],
                                record['TOPICS'],
                                record['KEYWORDS'],  # Add RAKE keywords
                                record['SENTIMENT_SCORE'],
                                record['SENTIMENT_CATEGORY'],
                                record['FEEDBACK_TYPE']
                            )
                        )
                        insert_count += 1
                    except Exception as e:
                        logger.error(f"Error inserting record: {str(e)}")

                self.conn.commit()
                logger.info(f"Successfully inserted {insert_count} records into Snowflake")
                cursor.close()
            else:
                logger.info("Simulating Snowflake insertion (no connection available)")
                time.sleep(1)
                logger.info(f"Simulated processing of {len(snowflake_records)} records for Snowflake")

            self.last_flush_time = time.time()
            self.batch = []
            return True

        except Exception as e:
            logger.error(f"Error flushing batch to Snowflake: {str(e)}")
            return False
    
    def prepare_text_for_snowflake(self, text):
        """
        Prepare text for storage in Snowflake by applying cleaning and truncation
        """
        if not text:
            return ""
            
        # First clean the text using our utility function
        cleaned_text = clean_text_for_storage(text)
        
        # Then truncate to fit Snowflake limits
        MAX_TEXT_LENGTH = 8000
        if len(cleaned_text) > MAX_TEXT_LENGTH:
            cleaned_text = cleaned_text[:MAX_TEXT_LENGTH] + "... [truncated]"
        
        return cleaned_text

    def close(self):
        if self.batch:
            self.flush_batch()
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")
        else:
            logger.info("No Snowflake connection to close")
            
        # Log final deduplication statistics
        logger.info(f"Deduplication summary: tracked {len(self.processed_ids)} unique comment IDs")

def process_kafka_messages():
    """Process messages from Kafka and send to Snowflake"""
    consumer = KafkaConsumer(
        'customer_feedback_v2',  # Updated to use new topic with consistent uppercase schema
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='snowflake_loader_v2'  # New consumer group for the new format
    )

    logger.info("Kafka consumer initialized, waiting for messages...")
    logger.info("Consumer-side deduplication is enabled to prevent duplicate records in Snowflake")

    try:
        # Initialize SnowflakeConnector with deduplication
        snowflake = SnowflakeConnector()
        message_counter = 0
        
        for message in consumer:
            try:
                message_counter += 1
                data = message.value
                logger.info(f"Received message #{message_counter} from platform: {data.get('PLATFORM', 'unknown')}")
                
                # Enhanced logging for debugging
                author = data.get('AUTHOR', 'unknown')
                subreddit = data.get('SUBREDDIT', 'unknown')
                comment_id = data.get('COMMENT_ID', 'unknown')
                logger.info(f"Comment ID: {comment_id}, Author: {author}, Subreddit: {subreddit}")
                
                # Add the record to the batch (with deduplication)
                snowflake.add_record(data)

            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

    finally:
        snowflake.close()
        logger.info("Kafka consumer closed")

if __name__ == "__main__":
    process_kafka_messages()
