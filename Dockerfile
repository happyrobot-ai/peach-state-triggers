FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

# Install system deps (if needed)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy application from new unified structure
COPY integrations/meiborg_brothers/ /app/

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

# Start the unified server
CMD ["python", "server.py"]
