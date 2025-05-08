import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_ingestion.kafka_to_snowflake import SnowflakeConnector

class TestKafkaToSnowflake(unittest.TestCase):
    
    @patch('data_ingestion.kafka_to_snowflake.snowflake.connector.connect')
    def test_snowflake_connector(self, mock_connect):
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Initialize connector and add a record
        connector = SnowflakeConnector(batch_size=1)  # Small batch size to trigger immediate flush
        connector.add_record({
            'COMMENT_ID': 'test123',
            'TEXT': 'Test feedback',
            'PLATFORM': 'amazon',
            'SENTIMENT_SCORE': 0.5,
            'SENTIMENT_CATEGORY': 'positive'
        })
        
        # Verify core functionality
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()
