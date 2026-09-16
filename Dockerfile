# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    APU_LAB_ROOT=/app \
    HOME=/app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --no-dev --no-install-project

COPY src ./src
COPY dashboard_notebook.py ./
COPY scripts ./scripts
RUN uv sync --no-dev

# Cache the Java expansion-service JAR so starting the streaming job needs no
# additional Maven download after the image has been built.
RUN /app/.venv/bin/python -c "from apache_beam.io.kafka import default_io_expansion_service; service=default_io_expansion_service(); address=service.__enter__(); print(address); service.__exit__(None, None, None)"

RUN useradd --create-home --uid 10001 student \
    && mkdir -p /app/data/cache /app/data/processed /app/tmp \
    && chown -R student:student /app
USER student

EXPOSE 2718
CMD ["/app/.venv/bin/marimo", "run", "dashboard_notebook.py", "--host", "0.0.0.0", "--port", "2718"]
