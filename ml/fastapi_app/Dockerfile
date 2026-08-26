FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY ml/fastapi_app/requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt

COPY ml/ml_code/ /app/src/
COPY ml/fastapi_app/main.py ml/fastapi_app/utils.py /app/


RUN mkdir -p /app/models /app/output

EXPOSE 8100

CMD ["python", "-c", "print('Pricing Engine Image Ready')"]