
"""
Snowflake Sync Scheduler

This script runs as a background process to periodically sync new records
from Snowflake to ChromaDB. It calls the API endpoint for syncing.
"""

import requests
import time
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("snowflake_sync.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("snowflake_sync")

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", 300))  # 5 minutes default


def trigger_sync():
    """Trigger a sync by calling the API endpoint"""
    try:
        logger.info(f"Triggering Snowflake sync at {datetime.now().isoformat()}")
        response = requests.post(f"{API_URL}/sync/snowflake")
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Sync triggered successfully: {result}")
            return True
        else:
            logger.error(f"Failed to trigger sync. Status code: {response.status_code}, Response: {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Error triggering sync: {e}")
        return False


def main():
    """Main function to run the scheduler"""
    logger.info(f"Starting Snowflake sync scheduler. Interval: {SYNC_INTERVAL_SECONDS} seconds")
    
    while True:
        try:
            # Trigger sync
            success = trigger_sync()
            
            # Sleep for the configured interval
            logger.info(f"Sleeping for {SYNC_INTERVAL_SECONDS} seconds until next sync")
            time.sleep(SYNC_INTERVAL_SECONDS)
        
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        
        except Exception as e:
            logger.error(f"Unexpected error in scheduler loop: {e}")
            # Sleep for a shorter time if there was an error
            time.sleep(60)


if __name__ == "__main__":
    main()