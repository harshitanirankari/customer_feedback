
import praw
from kafka import KafkaProducer
import json
import time
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import nltk
import threading

# Import sentiment analyzer
try:
    from sentiment_analyzer import SentimentAnalyzer
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from sentiment_analyzer import SentimentAnalyzer

# Import utility functions
try:
    from utils import (
        clean_text_for_storage,
        is_automoderator_comment,
        identify_platform,
        extract_keywords_rake,
        is_relevant_feedback,
        extract_feedback_topics,
        get_combined_subreddits
    )
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from utils import (
        clean_text_for_storage,
        is_automoderator_comment,
        identify_platform,
        extract_keywords_rake,
        is_relevant_feedback,
        extract_feedback_topics,
        get_combined_subreddits
    )

# Import keywords and subreddits 
try:
    from keywords_config import (
        platform_subreddits, 
        platform_keywords, 
        feedback_categories, 
        feedback_related_keywords
    )
except ImportError:
    logging.error("Could not import keywords_config.py - make sure it exists in the same directory")
    sys.exit(1)

# Ensure NLTK data is downloaded (required for RAKE)
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reddit_stream.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
MIN_CONTENT_LENGTH = 50  # Minimum characters for meaningful feedback
KAFKA_TOPIC = "customer_feedback_v2"  # New topic name for updated schema

# Create sentiment analyzer instance
sentiment_analyzer = SentimentAnalyzer()

def init_reddit():
    """Initialize and return Reddit API client"""
    try:
        client_id = os.getenv('REDDIT_CLIENT_ID')
        client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        user_agent = os.getenv('REDDIT_USER_AGENT')
        
        logger.info(f"Connecting to Reddit API with client_id: {client_id}")
        logger.info(f"Using user agent: {user_agent}")
        
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            read_only=True
        )
        
        # Verify connection
        subreddit_name = reddit.subreddit('amazon').display_name
        logger.info(f"Successfully connected to Reddit API and verified access to r/{subreddit_name}")
        return reddit
        
    except Exception as e:
        logger.error(f"Error initializing Reddit API: {str(e)}")
        logger.error("Please check your Reddit API credentials in the .env file")
        raise

def init_kafka():
    """Initialize and return Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )



def analyze_sentiment(text):
    """Use SentimentAnalyzer to analyze text sentiment"""
    try:
        score, magnitude = sentiment_analyzer.analyze(text)
        category = sentiment_analyzer.get_sentiment_category(score)
        return {
            "score": score,
            "magnitude": magnitude,
            "category": category
        }
    except Exception as e:
        logger.error(f"Error in sentiment analysis: {str(e)}")
        return {
            "score": 0.0,
            "magnitude": 0.0,
            "category": "neutral"
        }

def process_reddit_content(content, content_type=None, producer=None):
    """
    Process a Reddit comment or post and send to Kafka if relevant
    
    Args:
        content: Reddit Comment or Submission object
        content_type: Either "comment" or "post". If None, will be determined automatically.
        producer: Kafka producer instance
        
    Returns:
        boolean: True if processed and sent to Kafka, False otherwise
    """
    # Determine content type if not specified
    if content_type is None:
        # Check if it's a comment or submission based on object attributes
        if hasattr(content, 'body'):
            content_type = "comment"
        elif hasattr(content, 'selftext'):
            content_type = "post"
        else:
            logger.error("Unknown content type - neither comment nor post")
            return False
    try:
        # Skip if deleted or removed content
        if content_type == "comment":
            if not content.author or not content.body:
                return False
            text = content.body
            try:
                title = content.submission.title
                post_id = content.submission.id
            except Exception as e:
                logger.warning(f"Couldn't get post details: {str(e)}")
                title = "[Unknown]"
                post_id = ""
        else:  # post
            if not content.author or not content.selftext:
                return False
            text = content.selftext
            title = content.title
            post_id = content.id
        
        # Skip if too short to be meaningful
        if len(text.strip()) < MIN_CONTENT_LENGTH:
            logger.debug(f"Skipping short content ({len(text.strip())} chars): {text[:30]}...")
            return False
        
        # Skip AutoModerator comments
        if content_type == "comment" and is_automoderator_comment(content.author.name, text):
            logger.debug(f"Skipping AutoModerator comment in r/{content.subreddit.display_name}")
            return False
        
        # Check if relevant customer feedback
        if not is_relevant_feedback(text, title, platform_keywords, feedback_categories, feedback_related_keywords):
            return False
        
        # Identify platform
        platform = identify_platform(text, title, platform_keywords)
        
        # Extract topics
        topics = extract_feedback_topics(text, title, feedback_categories)
        
        # Extract keywords using RAKE
        keywords = extract_keywords_rake(text, title)
        
        # Analyze sentiment
        sentiment = analyze_sentiment(text)
        
        # Feedback type
        feedback_type = "platform_specific" if platform != "general" else "general"
        
        # Clean text for storage
        cleaned_text = clean_text_for_storage(text)
        cleaned_title = clean_text_for_storage(title)
        
        # Common message structure
        message = {
            "COMMENT_ID": content.id,
            "AUTHOR": str(content.author),
            "TEXT": cleaned_text,
            "SUBREDDIT": content.subreddit.display_name,
            "CREATED_DATE": datetime.utcfromtimestamp(content.created_utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "POST_ID": post_id,
            "POST_TITLE": cleaned_title,
            "SCORE": content.score,
            "PLATFORM": platform,
            "TOPICS": topics,
            "KEYWORDS": keywords,
            "SENTIMENT_SCORE": sentiment["score"],
            "SENTIMENT_MAGNITUDE": sentiment["magnitude"],
            "SENTIMENT_CATEGORY": sentiment["category"],
            "FEEDBACK_TYPE": feedback_type
        }
        
        # Add permalink
        if content_type == "comment":
            message["PERMALINK"] = f"https://reddit.com{content.permalink}"
        else:
            message["PERMALINK"] = f"https://reddit.com{content.permalink}"
            message["COMMENT_COUNT"] = content.num_comments
        
        # Send to Kafka
        if producer:
            producer.send(KAFKA_TOPIC, message)
            logger.info(f"Sent {content_type} to Kafka - Platform: {platform}, Topics: {topics}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {content_type}: {str(e)}")
        return False

def stream_comments(reddit, producer):
    """Stream comments from specified subreddits to Kafka"""
    try:
        # Get combined subreddit string using the helper function
        subreddits_string, unique_subreddits = get_combined_subreddits(platform_subreddits)
        
        # Create the subreddit object
        subreddit = reddit.subreddit(subreddits_string)
        
        logger.info(f"Starting to stream comments from {len(unique_subreddits)} subreddits...")
        
        # Stream comments
        for comment in subreddit.stream.comments(skip_existing=True):
            process_reddit_content(comment, "comment", producer)
                
    except Exception as e:
        logger.error(f"Error in comment stream: {str(e)}")

def stream_posts(reddit, producer):
    """Stream posts from Reddit and send to Kafka"""
    try:
        # Get combined subreddit string using the helper function
        subreddits_string, unique_subreddits = get_combined_subreddits(platform_subreddits)
        
        # Create the subreddit object
        subreddit = reddit.subreddit(subreddits_string)
        
        logger.info(f"Starting to stream posts from {len(unique_subreddits)} subreddits...")
        
        # Stream posts
        for submission in subreddit.stream.submissions(skip_existing=True):
            process_reddit_content(submission, "post", producer)
                
    except Exception as e:
        logger.error(f"Error in post stream: {str(e)}")

def search_platform_feedback(reddit, producer, platform, limit=50):
    """Search for feedback related to a specific platform"""
    logger.info(f"Searching for {platform} feedback across relevant subreddits")
    
    platform_specific_keywords = platform_keywords.get(platform, [])
    if not platform_specific_keywords:
        logger.error(f"No keywords defined for platform: {platform}")
        return
    
    try:
        # Get platform-specific subreddits plus general using the helper function
        _, search_subreddits = get_combined_subreddits(platform_subreddits, platform)
        
        # Search in selected subreddits
        for subreddit_name in search_subreddits:
            subreddit = reddit.subreddit(subreddit_name)
            logger.info(f"Searching in r/{subreddit_name}")
            
            # Get the newest comments
            for comment in subreddit.comments(limit=limit):
                # Use the shared processing function
                process_reddit_content(comment, "comment", producer)
            
            # Short pause between subreddits to avoid API rate limits
            time.sleep(1)
                    
    except Exception as e:
        logger.error(f"Error searching for {platform} feedback: {str(e)}")

def run():
    """Main function to run the Reddit to Kafka pipeline"""
    try:
        # Initialize Reddit client
        reddit = init_reddit()
        
        # Initialize Kafka producer
        producer = init_kafka()
        
        # First search for platform-specific feedback
        for platform in platform_keywords.keys():
            try:
                search_platform_feedback(reddit, producer, platform, limit=25)
            except Exception as e:
                logger.error(f"Error searching for {platform} feedback: {str(e)}")
                continue
        
        # Start streaming in separate threads
        comments_thread = threading.Thread(target=stream_comments, args=(reddit, producer))
        posts_thread = threading.Thread(target=stream_posts, args=(reddit, producer))
        
        # Start threads
        comments_thread.daemon = True
        posts_thread.daemon = True
        comments_thread.start()
        posts_thread.start()
        
        logger.info("Started streaming threads. Press Ctrl+C to exit.")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled error in main process: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    run()
