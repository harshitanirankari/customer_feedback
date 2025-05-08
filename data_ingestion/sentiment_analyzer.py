import logging
from textblob import TextBlob
import re
from utils import clean_text_for_storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """Class to analyze sentiment of text using TextBlob"""
    
    def __init__(self):
        logger.info("Initializing SentimentAnalyzer")
    
    def clean_text(self, text):
        """Clean text for sentiment analysis"""
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove special characters and numbers
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        # Convert to lowercase
        text = text.lower()
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def analyze(self, text):
        """Analyze sentiment of text and return score and magnitude"""
        try:
            # Clean the text
            cleaned_text = self.clean_text(text)
            
            # Skip empty text
            if not cleaned_text:
                return 0.0, 0.0
                
            # Analyze sentiment
            analysis = TextBlob(cleaned_text)
            
            # TextBlob returns polarity between -1 (negative) and 1 (positive)
            sentiment_score = analysis.sentiment.polarity
            
            # TextBlob returns subjectivity between 0 (objective) and 1 (subjective)
            # We'll use this as our "magnitude" or confidence
            sentiment_magnitude = analysis.sentiment.subjectivity
            
            logger.debug(f"Sentiment analysis: score={sentiment_score}, magnitude={sentiment_magnitude}")
            
            return sentiment_score, sentiment_magnitude
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return 0.0, 0.0
    
    def get_sentiment_category(self, score):
        """Get sentiment category based on score"""
        if score > 0.5:
            return "positive"
        elif score < -0.5:
            return "negative"
        else:
            return "neutral"
    
    def analyze_feedback(self, data):
        """Analyze sentiment of feedback data"""
        try:
            # Extract text from data - use feedback_text field from our structure
            text = data.get('feedback_text', '')
            
            # Analyze sentiment
            score, magnitude = self.analyze(text)
            
            # Add sentiment data to the original data
            data['sentiment_score'] = score
            data['sentiment_magnitude'] = magnitude
            data['sentiment_category'] = self.get_sentiment_category(score)
            
            return data
            
        except Exception as e:
            logger.error(f"Error analyzing feedback: {str(e)}")
            # Return original data if analysis fails
            return data


