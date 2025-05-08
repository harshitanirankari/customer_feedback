#!/usr/bin/env python
"""
Minimal Pipeline Runner
----------------------
Runs the Reddit to Snowflake data pipeline components.

Usage: python run_pipeline.py [component]
Where component is: full, ingest, process, or test
"""

import os
import sys
import time
import subprocess
import threading
import signal

# Global variables for process management
processes = {
    "reddit_to_kafka": None,
    "kafka_to_snowflake": None
}

def check_environment():
    """Check if all required environment variables are set"""
    required_vars = [
        'REDDIT_CLIENT_ID', 'REDDIT_CLIENT_SECRET', 'REDDIT_USER_AGENT',
        'KAFKA_BOOTSTRAP_SERVERS', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    return True

def start_reddit_to_kafka():
    """Start the Reddit to Kafka ingestion process"""
    print("Starting Reddit to Kafka process")
    
    try:
        process = subprocess.Popen(
            [sys.executable, "data_ingestion/reddit_to_kafka_refactored.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        processes["reddit_to_kafka"] = process
        
        # Simple output monitoring
        def monitor_output():
            for line in iter(process.stdout.readline, ''):
                print(f"[reddit_to_kafka] {line.strip()}")
                
        threading.Thread(target=monitor_output, daemon=True).start()
        return True
        
    except Exception as e:
        print(f"Failed to start Reddit to Kafka: {str(e)}")
        return False

def start_kafka_to_snowflake():
    """Start the Kafka to Snowflake processing"""
    print("Starting Kafka to Snowflake process")
    
    try:
        process = subprocess.Popen(
            [sys.executable, "data_ingestion/kafka_to_snowflake.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        processes["kafka_to_snowflake"] = process
        
        # Simple output monitoring
        def monitor_output():
            for line in iter(process.stdout.readline, ''):
                print(f"[kafka_to_snowflake] {line.strip()}")
                
        threading.Thread(target=monitor_output, daemon=True).start()
        return True
        
    except Exception as e:
        print(f"Failed to start Kafka to Snowflake: {str(e)}")
        return False

def stop_processes():
    """Stop all running processes"""
    for name, process in processes.items():
        if process and process.poll() is None:
            print(f"Stopping {name} process")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

def signal_handler(sig, frame):
    """Handle interrupt signals"""
    print("Shutting down pipeline...")
    stop_processes()
    sys.exit(0)

def run_tests():
    """Run pipeline tests"""
    print("Running tests...")
    
    try:
        process = subprocess.Popen(
            [sys.executable, "tests/test.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        for line in iter(process.stdout.readline, ''):
            print(line.strip())
        
        process.wait()
        return process.returncode == 0
        
    except Exception as e:
        print(f"Error running tests: {str(e)}")
        return False

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get component to run
    component = sys.argv[1] if len(sys.argv) > 1 else "full"
    
    # Check environment
    if not check_environment():
        print("Environment check failed")
        sys.exit(1)
    
    # Run requested component
    if component == "test":
        success = run_tests()
        sys.exit(0 if success else 1)
    
    # Start components
    if component in ["full", "ingest"]:
        if not start_reddit_to_kafka():
            sys.exit(1)
    
    if component in ["full", "process"]:
        if not start_kafka_to_snowflake():
            sys.exit(1)
    
    # Wait for processes to finish or user interrupt
    try:
        while any(p and p.poll() is None for p in processes.values()):
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        stop_processes()