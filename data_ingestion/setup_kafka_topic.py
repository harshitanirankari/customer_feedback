import os
import subprocess
import logging
import platform
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_kafka_topic():
    """Set up Kafka topic for all customer feedback"""
    
    # Get Kafka bootstrap servers from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic_name = 'customer_feedback'  # Single topic for all platforms
    
    # Determine OS and set appropriate command
    is_windows = platform.system() == 'Windows'
    
    if is_windows:
        # For Windows
        kafka_dir = input("Enter the path to your Kafka installation (e.g., C:\\kafka): ")
        
        if not os.path.exists(kafka_dir):
            logger.error(f"Kafka directory not found at: {kafka_dir}")
            return False
        
        cmd = [
            os.path.join(kafka_dir, "bin", "windows", "kafka-topics.bat"),
            "--create",
            "--topic", topic_name,
            "--bootstrap-server", bootstrap_servers,
            "--partitions", "3",  # Increased partitions for better parallelism
            "--replication-factor", "1"
        ]
    else:
        # For Linux/Mac
        cmd = [
            "kafka-topics.sh",
            "--create",
            "--topic", topic_name,
            "--bootstrap-server", bootstrap_servers,
            "--partitions", "3",  # Increased partitions for better parallelism
            "--replication-factor", "1"
        ]
    
    try:
        logger.info(f"Creating Kafka topic: {topic_name}")
        logger.info(f"Command: {' '.join(cmd)}")
        
        # Run the command
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        if "Created topic" in result.stdout or "already exists" in result.stderr:
            logger.info(f"Topic '{topic_name}' created successfully or already exists")
            return True
        else:
            logger.error(f"Failed to create topic. Output: {result.stdout}")
            logger.error(f"Error: {result.stderr}")
            return False
            
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr:
            logger.info(f"Topic '{topic_name}' already exists")
            return True
        else:
            logger.error(f"Error creating Kafka topic: {e}")
            logger.error(f"Command output: {e.stdout}")
            logger.error(f"Command error: {e.stderr}")
            return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting Kafka topic setup")
    if setup_kafka_topic():
        logger.info("Kafka topic setup completed successfully")
    else:
        logger.error("Kafka topic setup failed")
