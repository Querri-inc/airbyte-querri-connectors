# Querri destination for Airbyte. Built and pushed to GHCR by
# .github/workflows/publish-destination-querri.yml on a destination-querri-v* tag.
#
# Base: airbyte/python-connector-base 4.1.0 (Python 3.13), pinned by its
# multi-arch index digest so amd64 and arm64 builds start from the same image.
FROM docker.io/airbyte/python-connector-base:4.1.0@sha256:1d1aa21d34e851df4e8a87b391c27724c06e2597608e7161f4d167be853bd7b6

WORKDIR /airbyte/integration_code
COPY pyproject.toml main.py ./
COPY destination_querri ./destination_querri
RUN pip install --no-cache-dir . && rm -rf /pip_cache/*

# Unbuffered stdout: a STATE must reach the platform when it is emitted.
ENV PYTHONUNBUFFERED=1
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.name=ghcr.io/querri-inc/destination-querri
LABEL io.airbyte.version=0.2.0
LABEL org.opencontainers.image.source=https://github.com/Querri-inc/airbyte-querri-connectors

USER airbyte
