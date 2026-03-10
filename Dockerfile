FROM apache/spark:3.5.0

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

# Create data lake structure and allow spark user to write
RUN mkdir -p /app/data/raw \
    /app/data/bronze \
    /app/data/silver \
    /app/data/gold \
    && chmod -R 777 /app/data

USER spark