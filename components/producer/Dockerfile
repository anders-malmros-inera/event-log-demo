FROM python:3.9-slim
WORKDIR /app

# Install Docker CLI for container control
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com -o get-docker.sh && \
    sh get-docker.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* get-docker.sh

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app.py"]