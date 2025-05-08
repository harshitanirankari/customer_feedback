FROM python:alpine

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apk update && apk add --no-cache \
    gcc \
    g++ \
    python3-dev \
    musl-dev

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
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
RUN chown -R appuser:appgroup /app
USER appuser

# Command to run the application (can be overridden in docker-compose)
CMD ["python", "run_pipeline.py", "full"]
