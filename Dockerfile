# syntax=docker/dockerfile:1
# Gaussia Brains MCP/REST server — serves Boltzmann Brains over MCP Streamable
# HTTP and a REST management API. Built for local development and CI; review
# auth/secret handling before production use.

FROM python:3.13-slim-bookworm AS builder

WORKDIR /build

# Install tools needed to fetch ORAS and to build any source-only wheels.
# Also refresh PAM packages to pull in the latest bookworm security update.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential \
    libpam-modules libpam-modules-bin libpam-runtime libpam0g \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast, reproducible Python packaging.
COPY --from=ghcr.io/astral-sh/uv:0.6.17 /uv /uvx /bin/

# Upgrade the system pip to a secure version before installing the project.
RUN pip install --no-cache-dir --upgrade pip==26.3

# Install the ORAS CLI — required for pulling brain snapshots from OCI registries.
# TARGETARCH is supplied by BuildKit ("amd64" or "arm64").
ARG TARGETARCH
ARG ORAS_VERSION=1.4.0
RUN case "${TARGETARCH}" in \
      amd64) ORAS_SHA256="d8e1dce2fd8ce996f1d901845b688eb0cf505a6082897606f053825bb47935d1" ;; \
      arm64) ORAS_SHA256="0791fc200831563e498638d30e8ee8aac20ad1da2610760120fe3f770e12a8a4" ;; \
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
FROM python:3.13-slim-bookworm AS runner

WORKDIR /app

# curl is used by the docker-compose healthcheck.
# Refresh PAM packages to pull in the latest bookworm security update.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    libpam-modules libpam-modules-bin libpam-runtime libpam0g \
    && rm -rf /var/lib/apt/lists/*

# Upgrade the bundled pip to a secure version.
RUN pip install --no-cache-dir --upgrade pip==26.3

# Copy ORAS and the installed Python environment from the builder.
COPY --from=builder /usr/local/bin/oras /usr/local/bin/oras
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/gaussia-brains-mcp-server /usr/local/bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

USER nobody

ENTRYPOINT ["gaussia-brains-mcp-server"]
