import unittest
import os
import sys

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_ingestion.sentiment_analyzer import SentimentAnalyzer

class TestSentimentAnalyzer(unittest.TestCase):
    
    def setUp(self):
        self.analyzer = SentimentAnalyzer()
    
    def test_core_sentiment_functionality(self):
        # Test sentiment analysis with positive text
        score, _ = self.analyzer.analyze("This product is excellent!")
        self.assertGreater(score, 0)
        self.assertEqual("positive", self.analyzer.get_sentiment_category(score))
        
        # Test sentiment analysis with negative text
        score, _ = self.analyzer.analyze("This product is terrible!")
        self.assertLess(score, 0)
        self.assertEqual("negative", self.analyzer.get_sentiment_category(score))
        
        # Test feedback analysis
        data = {"feedback_text": "Great product!"}
        result = self.analyzer.analyze_feedback(data)
        self.assertIn("sentiment_score", result)

if __name__ == '__main__':
    unittest.main()
