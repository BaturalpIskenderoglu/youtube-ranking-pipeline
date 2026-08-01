FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir pyspark==3.5.0

COPY spark_calculator.py .

CMD ["python", "spark_calculator.py"]
