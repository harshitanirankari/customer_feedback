"""
Configuration file for Reddit feedback collection keywords and subreddits.
Contains platform-specific subreddits, keywords, and feedback topic categories.
"""

# Define subreddits by platform
platform_subreddits = {
    "amazon": [
        "amazon",               # Main Amazon subreddit
        "AmazonReviews",        # Dedicated to Amazon product reviews
        "amazonreviews",        # Alternative Amazon review community
        "amazonprime",          # Amazon Prime specific discussions
        "AmazonSellers",        # Amazon seller experiences
        "AmazonHelp",           # Amazon customer service experiences
        "AmazonFC",             # Amazon fulfillment center experiences
        "amazonecho",           # Amazon Echo/Alexa feedback
        "AmazonFBAOnlineRetail" # Amazon FBA seller experiences
    ],
    "flipkart": [
        "flipkart",             # Main Flipkart subreddit
        "FlipkartSellers",      # Flipkart seller experiences
        "indianecommerce"       # Indian e-commerce discussions (includes Flipkart)
    ],
    "walmart": [
        "walmart",              # Main Walmart subreddit
        "WalmartSellers",       # Walmart seller experiences
        "WalmartEmployees"      # Walmart employee experiences
    ],
    "target": [
        "target",               # Main Target subreddit
        "TargetEmployees"       # Target employee experiences
    ],
    "bestbuy": [
        "bestbuy",              # Main Best Buy subreddit
        "BestBuyEmployees"      # Best Buy employee experiences
    ],
    "general": [
        "ecommerce",            # General e-commerce discussions
        "CustomerService",      # Customer service experiences
        "TalesFromRetail",      # Retail experiences
        "TalesFromTheCustomer", # Customer experiences
        "OnlineShopping",       # Online shopping experiences
        "ProductReviews"        # Product review discussions
    ]
}

# Platform-specific keywords for filtering
platform_keywords = {
    "amazon": [
        "amazon", "prime", "alexa", "echo", "kindle", "aws", 
        "amazon music", "amazon video", "amazon fresh", "whole foods",
        "amazon delivery", "amazon order", "amazon refund", "amazon return"
    ],
    "flipkart": [
        "flipkart", "flipkart plus", "flipkart delivery", "flipkart order",
        "flipkart customer service", "flipkart refund", "flipkart return"
    ],
    "walmart": [
        "walmart", "wal-mart", "walmart+", "walmart plus", "walmart delivery",
        "walmart order", "walmart refund", "walmart return", "walmart pickup"
    ],
    "target": [
        "target", "target circle", "target redcard", "target delivery",
        "target order", "target refund", "target return", "target pickup"
    ],
    "bestbuy": [
        "best buy", "bestbuy", "geek squad", "best buy delivery",
        "best buy order", "best buy refund", "best buy return"
    ]
}

# Define feedback categories and their keywords
feedback_categories = {
    "delivery": [
        "shipping", "delivery", "package", "arrived", "late", "delay", 
        "tracking", "carrier", "driver", "doorstep", "missing"
    ],
    "customer_service": [
        "customer service", "support", "representative", "agent", "chat", 
        "call", "phone", "email", "contact", "response", "wait time"
    ],
    "product_quality": [
        "quality", "defect", "broken", "damaged", "not working", "faulty",
        "failure", "malfunction", "durability", "build quality"
    ],
    "pricing": [
        "price", "cost", "expensive", "cheap", "overpriced", "affordable",
        "discount", "sale", "promotion", "coupon", "deal", "offer"
    ],
    "returns": [
        "return", "refund", "exchange", "money back", "return policy", 
        "return window", "return label", "restocking fee"
    ],
    "website_app": [
        "website", "app", "mobile", "interface", "user experience", "ux", 
        "login", "account", "checkout", "cart", "search", "filter", "glitch", "bug"
    ]
}

# Feedback-related keywords to determine if text contains customer feedback
feedback_related_keywords = [
    # General feedback terms
    "review", "feedback", "experience", "rating", "star", "recommend",
    
    # Product-related
    "product", "item", "purchase", "bought", "ordered",
    
    # Service-related
    "service", "customer service", "support", "help", "assistance",
    
    # Problem-related
    "issue", "problem", "error", "complaint", "disappointed",
    
    # Resolution-related
    "resolved", "solution", "fixed", "replacement", "refund",
    
    # Quality-related
    "quality", "performance", "works", "doesn't work", "broken",
    
    # Delivery-related
    "shipping", "delivery", "arrived", "package", "tracking",
    
    # Sentiment-related
    "happy", "satisfied", "dissatisfied", "upset", "love", "hate",
    
    # Comparison-related
    "better than", "worse than", "compared to", "similar to",
    
    # Value-related
    "worth", "value", "waste", "money", "expensive", "cheap"
]

# AutoModerator patterns to filter out
automoderator_patterns = [
    "if you have a question and not a complaint",
    "complaints may only be posted in the",
    "please be aware this is not a customer service subreddit",
    "i am a bot, and this action was performed automatically",
    "weekly help and discussion thread"
]
