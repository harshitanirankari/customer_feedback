from kafka import KafkaConsumer
import json
import logging
import os
from dotenv import load_dotenv
from datetime import datetime
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def consume_messages():
    """Test consumer to verify Kafka messages in the updated Snowflake format"""
    consumer = KafkaConsumer(
        'customer_feedback_v2',  # Using the new topic with updated schema
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        auto_offset_reset='earliest',  # Only read messages that arrive after starting the consumer
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='test_group_v2'  # New consumer group for the new topic
    )
    
    logger.info("Starting Kafka consumer for updated data format. Press CTRL+C to exit.")
    
    # Create a directory for storing JSON files if it doesn't exist
    os.makedirs('data_output', exist_ok=True)
    
    # Create platform-specific directories
    platforms = ['amazon', 'flipkart', 'walmart', 'target', 'bestbuy', 'general']
    for platform in platforms:
        os.makedirs(f'data_output/{platform}', exist_ok=True)
    
    message_count = 0
    platform_counts = {platform: 0 for platform in platforms}
    all_messages = []
    
    try:
        for message in consumer:
            message_count += 1
            data = message.value
            
            # Check if this is the old or new format (backward compatibility)
            if 'PLATFORM' in data:
                # New format (uppercase keys)
                platform = data.get('PLATFORM', 'general').lower()
                subreddit = data.get('SUBREDDIT', 'unknown')
                author = data.get('AUTHOR', 'unknown')
                title = data.get('POST_TITLE', '')
                text = data.get('TEXT', '')
                permalink = data.get('PERMALINK', '')
                topics = data.get('TOPICS', [])
                sentiment_category = data.get('SENTIMENT_CATEGORY', 'neutral')
                sentiment_score = data.get('SENTIMENT_SCORE', 0.0)
            else:
                # Old format (lowercase keys with nested structure)
                platform = data.get('platform', 'general').lower()
                subreddit = data.get('source_details', {}).get('subreddit', 'unknown')
                author = data.get('source_details', {}).get('author', 'unknown')
                title = data.get('feedback_title', '')
                text = data.get('feedback_text', '')
                permalink = data.get('source_link', '')
                topics = data.get('topics', [])
                
                # Handle different sentiment formats
                if isinstance(data.get('sentiment'), dict):
                    sentiment_category = data.get('sentiment', {}).get('category', 'neutral')
                    sentiment_score = data.get('sentiment', {}).get('score', 0.0)
                else:
                    sentiment_category = data.get('sentiment', 'neutral')
                    sentiment_score = 0.0
            
            # Ensure platform is in our expected list
            if platform not in platform_counts:
                platform = 'general'
            
            platform_counts[platform] += 1
            
            # Add to our collection of all messages
            all_messages.append(data)
            
            # Save all messages to a JSON file after each new message
            with open('data_output/all_feedback.json', 'w', encoding='utf-8') as f:
                json.dump(all_messages, f, indent=2, ensure_ascii=False)
            
            # Save to platform-specific directory
            platform_dir = f'data_output/{platform}'
            with open(f'{platform_dir}/message_{platform_counts[platform]}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Print message details with platform highlighting
            logger.info(f"{'='*20} NEW MESSAGE {'='*20}")
            logger.info(f"PLATFORM: {platform.upper()}")
            logger.info(f"Message #{message_count} (Platform #{platform_counts[platform]}) from r/{subreddit}")
            logger.info(f"Author: {author}")
            
            # Show title and text, truncating if too long
            logger.info(f"Title: {title[:50]}..." if len(title) > 50 else f"Title: {title}")
            logger.info(f"Text: {text[:100]}..." if len(text) > 100 else f"Text: {text}")
            
            # Display topics and sentiment
            if topics:
                logger.info(f"Topics: {', '.join(topics)}")
            
            logger.info(f"Sentiment: {sentiment_category} (Score: {sentiment_score:.2f})")
            logger.info(f"Link: {permalink}")
            logger.info(f"Saved to: {platform_dir}/message_{platform_counts[platform]}.json")
            
            # Print platform statistics
            logger.info("Platform Statistics:")
            for p, count in platform_counts.items():
                if count > 0:
                    logger.info(f"  - {p.upper()}: {count} messages")
            
            # Print the full JSON structure with nice formatting
            print("\nFULL JSON DATA:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            print("-" * 80)
            
            # Small delay to make output more readable
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Save summary statistics
        summary = {
            "total_messages": message_count,
            "platform_counts": platform_counts,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open('data_output/consumer_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
            
        logger.info(f"Test complete. Processed {message_count} messages.")
        logger.info(f"Platform breakdown: {json.dumps(platform_counts)}")

if __name__ == "__main__":
    consume_messages()
