# Build from the repository root: docker build -f integration_layer/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv
COPY integration_layer/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY common/ilcommon ./ilcommon
COPY db ./db
COPY integration_layer ./integration_layer
RUN useradd --system --uid 10001 app
USER app

EXPOSE 8000
CMD ["uvicorn", "integration_layer.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
