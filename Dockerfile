FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    cargo \
    rustc \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies and test dependencies
RUN pip install --no-cache-dir -r requirements.txt pytest pytest-mock

# Download NLTK data
RUN python -m nltk.downloader punkt stopwords wordnet

# Copy project files
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a non-root user to run the application
RUN groupadd --system appgroup && useradd --system --gid appgroup appuser
RUN chown -R appuser:appgroup /app
USER appuser

# Command to run the application (can be overridden in docker-compose)
CMD ["python", "run_pipeline.py", "full"]
