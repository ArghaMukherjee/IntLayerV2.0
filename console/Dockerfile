# Build from the repository root: docker build -f console/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv
COPY console/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY common/ilcommon ./ilcommon
COPY console ./console
RUN useradd --system --uid 10001 app
USER app

EXPOSE 8080
CMD ["uvicorn", "console.main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers"]
