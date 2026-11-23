FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "numpy==1.26.4" \
    "tensorflow==2.15.0" \
    "pandas<3.0" \
    "opencv-python-headless<4.11" \
    kafka-python-ng

RUN pip install --no-cache-dir "tf-keras==2.15.0"

COPY src/ ./src/
COPY models/ ./models/
COPY data/ ./data/

ENV PYTHONUNBUFFERED=1