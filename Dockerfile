# syntax=docker/dockerfile:1
# Gaussia Brains MCP/REST server — serves Boltzmann Brains over MCP Streamable
# HTTP and a REST management API. Built for local development and CI; review
# auth/secret handling before production use.

FROM python:3.12-slim-bookworm AS builder

WORKDIR /build

# Install tools needed to fetch ORAS and to build any source-only wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast, reproducible Python packaging.
COPY --from=ghcr.io/astral-sh/uv:0.6.17 /uv /uvx /bin/

# Install the ORAS CLI — required for pulling brain snapshots from OCI registries.
# TARGETARCH is supplied by BuildKit ("amd64" or "arm64").
ARG TARGETARCH
ARG ORAS_VERSION=1.3.0
RUN case "${TARGETARCH}" in \
      amd64) ORAS_SHA256="6cdc692f929100feb08aa8de584d02f7bcc30ec7d88bc2adc2054d782db57c64" ;; \
      arm64) ORAS_SHA256="7649738b48fde10542bcc8b0e9b460ba83936c75fb5be01ee6d4443764a14352" ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_${TARGETARCH}.tar.gz" \
      -o /tmp/oras.tar.gz && \
    echo "${ORAS_SHA256}  /tmp/oras.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/oras.tar.gz -C /usr/local/bin oras && \
    chmod +x /usr/local/bin/oras && \
    rm -f /tmp/oras.tar.gz

COPY pyproject.toml ./
COPY src ./src

# Install the package and its dependencies into the system interpreter.
RUN uv pip install --system --no-cache .

# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runner

WORKDIR /app

# curl is used by the docker-compose healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy ORAS and the installed Python environment from the builder.
COPY --from=builder /usr/local/bin/oras /usr/local/bin/oras
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/gaussia-brains-mcp-server /usr/local/bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

USER nobody

ENTRYPOINT ["gaussia-brains-mcp-server"]
