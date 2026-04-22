FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV REDIS_HOST=redis  \
    REDIS_PORT=6379   \
    GATEWAY_PORT=5000 \
    FLASK_DEBUG=false

EXPOSE 5000

# Use Gunicorn in production (4 workers, sync)
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "4", \
     "--timeout", "60", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "app:app"]
