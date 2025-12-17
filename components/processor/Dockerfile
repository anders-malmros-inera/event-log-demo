FROM python:3.9-slim
WORKDIR /app
RUN pip install kafka-python cassandra-driver
COPY app.py .
CMD ["python", "-u", "app.py"]
