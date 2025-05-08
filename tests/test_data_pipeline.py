import unittest
from unittest.mock import patch, MagicMock
import os
import sys
from datetime import datetime

# Add parent directory to path to import the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_ingestion.setup_kafka_topic import setup_kafka_topic
from data_ingestion.reddit_to_kafka_refactored import process_reddit_content
from data_ingestion.kafka_to_snowflake import SnowflakeConnector
from data_ingestion.sentiment_analyzer import SentimentAnalyzer

class TestDataPipeline(unittest.TestCase):
    
    @patch('data_ingestion.setup_kafka_topic.subprocess.run')
    @patch('data_ingestion.setup_kafka_topic.platform.system')
    @patch('data_ingestion.setup_kafka_topic.os.path.exists')
    @patch('data_ingestion.setup_kafka_topic.input')
    def test_core_pipeline_components(self, mock_input, mock_path_exists, mock_platform_system, mock_subprocess_run):
        # Mock platform and path checks to avoid Kafka installation path checks
        mock_platform_system.return_value = 'Linux'  # Use Linux to avoid Windows path input
        mock_path_exists.return_value = True
        mock_input.return_value = '/tmp/kafka'  # Mock any input request
        
        # Test Kafka topic setup
        mock_process = MagicMock()
        mock_process.stdout = 'Created topic customer_feedback'
        mock_subprocess_run.return_value = mock_process
        self.assertTrue(setup_kafka_topic())
        
        # Test sentiment analysis
        analyzer = SentimentAnalyzer()
        score, _ = analyzer.analyze("This is a great product!")
        self.assertGreater(score, 0)
    
    @patch('data_ingestion.reddit_to_kafka_refactored.analyze_sentiment')
    @patch('data_ingestion.reddit_to_kafka_refactored.identify_platform')
    @patch('data_ingestion.reddit_to_kafka_refactored.is_relevant_feedback')
    @patch('data_ingestion.reddit_to_kafka_refactored.extract_keywords_rake')
    @patch('data_ingestion.reddit_to_kafka_refactored.is_automoderator_comment')
    def test_data_flow(self, mock_is_automoderator, mock_extract_keywords, mock_is_relevant, mock_identify_platform, mock_analyze_sentiment):
        # Mock necessary functions
        mock_is_automoderator.return_value = False
        mock_is_relevant.return_value = True
        mock_identify_platform.return_value = 'amazon'
        mock_extract_keywords.return_value = ['amazon', 'feedback', 'quality']
        mock_analyze_sentiment.return_value = {'score': 0.5, 'magnitude': 0.7, 'category': 'positive'}
        
        # Create a proper mock comment with all required attributes
        comment_text = 'This is a detailed Amazon feedback about their product quality and delivery service. Very satisfied.'
        
        mock_comment = MagicMock()
        mock_comment.body = comment_text
        mock_comment.id = 'comment123'
        mock_comment.created_utc = datetime.now().timestamp()
        mock_comment.permalink = '/r/amazon/comments/123/title/comment123/'
        mock_comment.score = 10
        
        # Mock author and subreddit as they are nested objects
        mock_author = MagicMock()
        mock_author.name = 'user123'
        mock_comment.author = mock_author
        
        mock_subreddit = MagicMock()
        mock_subreddit.display_name = 'amazon'
        mock_comment.subreddit = mock_subreddit
        
        # Mock submission (parent post)
        mock_submission = MagicMock()
        mock_submission.title = 'Amazon Product Review'
        mock_submission.id = 'post123'
        mock_comment.submission = mock_submission
        
        # Mock producer
        mock_producer = MagicMock()
        
        # Test Reddit to Kafka
        result = process_reddit_content(mock_comment, 'comment', mock_producer)
        self.assertTrue(result)
        mock_producer.send.assert_called_once()
        
        # Test Kafka to Snowflake
        with patch('data_ingestion.kafka_to_snowflake.snowflake.connector.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            connector = SnowflakeConnector(batch_size=1)
            connector.add_record({
                'COMMENT_ID': 'test123',
                'TEXT': 'Test feedback',
                'PLATFORM': 'amazon',
                'SENTIMENT_SCORE': 0.5
            })
            
            mock_cursor.execute.assert_called_once()

if __name__ == '__main__':
    unittest.main()
