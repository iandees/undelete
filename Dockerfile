FROM python:3.12-slim AS builder

# Build tippecanoe from source
RUN apt-get update && apt-get install -y \
    git build-essential libsqlite3-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && cd /tmp/tippecanoe \
    && make -j$(nproc) \
    && make install

FROM python:3.12-slim

# Copy tippecanoe binaries
RUN apt-get update && apt-get install -y libsqlite3-0 zlib1g && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/tippecanoe /usr/local/bin/tippecanoe
COPY --from=builder /usr/local/bin/tile-join /usr/local/bin/tile-join

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
