#!/bin/bash

# Create and activate virtual environment
echo "Creating virtual environment..."
python -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Run the FastAPI application
echo "Starting the application..."
uvicorn app:app --host 0.0.0.0 --port 8000 --reload