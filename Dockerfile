FROM apache/spark:latest

USER root

RUN apt-get update && apt-get install -y python3-pip
RUN pip install --no-cache-dir pyspark kaggle pandas

WORKDIR /app

COPY . .

EXPOSE 8080

CMD ["tail", "-f", "/dev/null"]