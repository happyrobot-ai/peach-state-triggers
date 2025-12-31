#!/bin/sh
set -eu

# Navigate to the service directory
cd integrations/meiborg_brothers/find_load

# Install dependencies (if not already installed by the platform build step)
pip install --no-cache-dir -r requirements.txt >/dev/null 2>&1 || pip install -r requirements.txt

# Start FastAPI app
exec python server.py


