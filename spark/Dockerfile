FROM apache/spark:3.5.0

USER root

WORKDIR /app

COPY spark_calculator.py /app/spark_calculator.py

USER spark

CMD ["python3", "app/spark_calculator.py"]
