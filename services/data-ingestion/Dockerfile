FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose application port (if needed, not necessary for this script)
EXPOSE 5000

# Run the Python script directly
CMD ["python", "/app/app/ingest.py"]
