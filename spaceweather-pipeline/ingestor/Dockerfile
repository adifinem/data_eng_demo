FROM python:3.12-slim
WORKDIR /app

# System basics
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code + config
COPY app.py /app/app.py
COPY service.py /app/service.py
COPY datasets.yml /app/datasets.yml

# Folders for schema cache + state
RUN mkdir -p /app/schemas /app/state

ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]


