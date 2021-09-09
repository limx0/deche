FROM python:3-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get purge -y --auto-remove && \
    rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
COPY test-requirements.txt /app/test-requirements.txt
RUN pip install -r /app/requirements.txt -r /app/test-requirements.txt
COPY . /app/
WORKDIR /app
RUN pip install -e ".[s3]"
