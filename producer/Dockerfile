# Small official Python image.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies first, so Docker can reuse this layer when only the code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as an unprivileged user: the producer only needs the network.
USER nobody

# -u = unbuffered output, so log lines appear immediately in `docker compose logs`.
CMD ["python", "-u", "producer.py"]
