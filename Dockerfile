FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/pyproject.toml
COPY blackbox /app/blackbox
COPY blackbox_pro /app/blackbox_pro
COPY bb /app/bb
COPY examples /app/examples
COPY README.md /app/README.md

RUN pip install --upgrade pip \
 && pip install -e ".[pro]"

EXPOSE 8088

CMD ["blackbox-pro", "serve", "--host", "0.0.0.0", "--port", "8088", "--root", "/data/.blackbox_store"]
