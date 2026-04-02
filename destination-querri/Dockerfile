FROM python:3.11-slim

WORKDIR /airbyte/integration_code

# Install dependencies
COPY setup.py ./
RUN pip install --no-cache-dir .

# Copy source code
COPY main.py ./
COPY destination_querri ./destination_querri

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/destination-querri
