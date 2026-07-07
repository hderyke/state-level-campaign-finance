FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY db.py .
COPY routers/ routers/
COPY start.sh .
RUN chmod +x start.sh

ENV DB_PATH=/data/state-level-cf.db

EXPOSE 8000

CMD ["./start.sh"]
