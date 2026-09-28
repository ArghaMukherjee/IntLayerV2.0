# Build from the repository root: docker build -f consumer_application/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv
COPY consumer_application/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY common/ilcommon ./ilcommon
COPY consumer_application ./consumer_application
RUN useradd --system --uid 10001 app
USER app

EXPOSE 8001
CMD ["uvicorn", "consumer_application.main:app", "--host", "0.0.0.0", "--port", "8001", "--proxy-headers"]
