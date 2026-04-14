FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /var/lib/talos/work /mnt/archive
EXPOSE 8081
CMD ["uvicorn", "worker_main:app", "--host", "0.0.0.0", "--port", "8081"]
