# Python 3.13: the Cassandra driver ships a ready-built package for this version.
FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rules.py processor.py ./

USER nobody

CMD ["python", "-u", "processor.py"]
