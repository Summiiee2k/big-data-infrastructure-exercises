# TBD
FROM python:3.11-slim

WORKDIR /opt/app

COPY . .

RUN pip install --no-cache-dir \
    fastapi==0.103.2 \
    uvicorn==0.23.2 \
    boto3==1.28.57 \
    psycopg2-binary==2.9.9 \
    pytest==7.4.2

EXPOSE 8000

CMD ["uvicorn", "bdi_api.s7.exercise:s7", "--host", "0.0.0.0", "--port", "8000"]