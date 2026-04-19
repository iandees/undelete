FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install Python dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY daemon/ daemon/
COPY pipeline/ pipeline/
COPY main.py .

ENV DATA_DIR=/data

VOLUME /data

CMD ["uv", "run", "python", "main.py"]
