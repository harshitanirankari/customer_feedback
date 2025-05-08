import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_ingestion.utils import (
    clean_text_for_storage,
    is_automoderator_comment,
    identify_platform,
    extract_keywords_rake
)

class TestUtils(unittest.TestCase):
    
    def test_core_text_processing(self):
        # Test text cleaning
        text = "Check out https://example.com/product"
        cleaned = clean_text_for_storage(text)
        self.assertNotIn("https://", cleaned)
        
        # Test AutoModerator detection
        self.assertTrue(is_automoderator_comment("AutoModerator", "I am a bot"))
        
        # Test platform identification
        platform_keywords = {"amazon": ["amazon"]}
        self.assertEqual("amazon", identify_platform("Amazon order", platform_keywords=platform_keywords))
    
    @patch('data_ingestion.utils.Rake')
    def test_keyword_extraction(self, mock_rake_class):
        # Setup mock
        mock_rake = MagicMock()
        mock_rake_class.return_value = mock_rake
        mock_rake.get_ranked_phrases_with_scores.return_value = [(5.0, "customer service")]
        
        # Test keyword extraction
        keywords = extract_keywords_rake("The customer service was excellent")
        self.assertEqual(["customer service"], keywords)

if __name__ == '__main__':
    unittest.main()
