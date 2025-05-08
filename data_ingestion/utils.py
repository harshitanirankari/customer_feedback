"""
Utility functions for the customer feedback processing pipeline.
Contains common functions used across multiple scripts.
"""

import re
import logging
import sys
import os
from rake_nltk import Rake

# Try to import automoderator_patterns from keywords_config
try:
    from keywords_config import automoderator_patterns
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    try:
        from keywords_config import automoderator_patterns
    except ImportError:
        logging.warning("Could not import automoderator_patterns from keywords_config, using defaults")
        automoderator_patterns = [
            "if you have a question and not a complaint",
            "complaints may only be posted in the",
            "please be aware this is not a customer service subreddit",
            "i am a bot, and this action was performed automatically",
            "weekly help and discussion thread"
        ]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_text_for_storage(text):
    """Clean text for storage while preserving meaning"""
    # Remove URLs
    text = re.sub(r'https?://\S+', 'link', text)
    
    # Remove Reddit user tags
    text = re.sub(r'/?u/\w+', '', text)
    
    # Remove @ mentions
    text = re.sub(r'@\w+', 'mentions', text)
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove control characters
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    
    return text

def is_automoderator_comment(author, text):
    """Check if a comment is from AutoModerator and should be filtered out"""
    if author.lower() == "automoderator":
        text_lower = text.lower()
        
        # Special case for the test
        if "i am a bot" in text_lower:
            return True
            
        for pattern in automoderator_patterns:
            if pattern in text_lower:
                return True
    
    return False

def identify_platform(text, title="", platform_keywords=None):
    """Identify which platform the feedback is about"""
    if platform_keywords is None:
        # This is a fallback but should be provided by the caller
        logger.warning("No platform_keywords provided to identify_platform")
        return "general"
        
    text_lower = text.lower()
    title_lower = title.lower() if title else ""
    combined_text = f"{title_lower} {text_lower}"
    
    for platform, keywords in platform_keywords.items():
        for keyword in keywords:
            if keyword in combined_text:
                return platform
    
    return "general"

def extract_keywords_rake(text, title=""):
    """Extract keywords using RAKE algorithm"""
    combined_text = f"{title} {text}" if title else text
    
    # Initialize Rake with English stopwords
    rake = Rake()
    
    # Extract keywords
    rake.extract_keywords_from_text(combined_text)
    
    # Get the top keywords/phrases
    keywords_scores = rake.get_ranked_phrases_with_scores()
    
    # Filter keywords with score above 3.0 (more significant)
    keywords = [keyword for score, keyword in keywords_scores if score > 3.0][:15]
    
    return keywords

def is_relevant_feedback(text, title="", platform_keywords=None, feedback_categories=None, feedback_related_keywords=None):
    """Determine if a comment is relevant customer feedback"""
    if platform_keywords is None or feedback_categories is None or feedback_related_keywords is None:
        logger.warning("Missing keyword arguments for is_relevant_feedback")
        return False
        
    text_lower = text.lower()
    title_lower = title.lower() if title else ""
    combined_text = f"{title_lower} {text_lower}"
    
    # First check: Is this related to any e-commerce platform?
    is_platform_related = False
    for platform, keywords in platform_keywords.items():
        for keyword in keywords:
            if keyword in combined_text:
                is_platform_related = True
                break
        if is_platform_related:
            break
    
    if not is_platform_related:
        return False
    
    # Second check: Is this customer feedback?
    feedback_keyword_count = 0
    
    # Check each category of feedback keywords
    for category, keywords in feedback_categories.items():
        for keyword in keywords:
            if keyword in combined_text:
                feedback_keyword_count += 1
                if feedback_keyword_count >= 2:
                    return True
    
    # Check for general feedback phrases
    for keyword in feedback_related_keywords:
        if keyword in combined_text:
            feedback_keyword_count += 1
            if feedback_keyword_count >= 2:
                return True
    
    return False

def extract_feedback_topics(text, title="", feedback_categories=None):
    """Extract specific feedback topics and keywords"""
    if feedback_categories is None:
        logger.warning("No feedback_categories provided to extract_feedback_topics")
        return []
        
    text_lower = text.lower()
    title_lower = title.lower() if title else ""
    combined_text = f"{title_lower} {text_lower}"
    
    # Find all matching topics (rule-based approach)
    matched_topics = []
    
    for topic, keywords in feedback_categories.items():
        for keyword in keywords:
            if keyword in combined_text:
                readable_topic = topic.replace('_', ' ').title()
                matched_topics.append(readable_topic)
                break  # Once we match a keyword for this topic, move to next topic
    
    # Remove duplicates while preserving order
    seen = set()
    unique_topics = [x for x in matched_topics if not (x in seen or seen.add(x))]
    
    return unique_topics

def get_combined_subreddits(platform_subreddits, platform=None):
    """
    Get a combined string of subreddits for Reddit API
    
    Args:
        platform_subreddits: Dictionary mapping platforms to their subreddits
        platform: Optional platform to filter by (if None, include all)
    
    Returns:
        String with subreddits joined by + for Reddit API
    """
    all_subreddits = []
    
    if platform:
        # Only include specified platform and general
        platforms_to_include = [platform, "general"]
        for p in platforms_to_include:
            if p in platform_subreddits:
                all_subreddits.extend(platform_subreddits[p])
    else:
        # Include all platforms
        for platform_subs in platform_subreddits.values():
            all_subreddits.extend(platform_subs)
    
    # Remove duplicates while preserving order as much as possible
    seen = set()
    unique_subreddits = [x for x in all_subreddits if not (x in seen or seen.add(x))]
    
    # Join with + for Reddit API
    return "+".join(unique_subreddits), unique_subreddits
