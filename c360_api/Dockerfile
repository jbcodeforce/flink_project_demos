# Customer Analytics C360 API - Docker Image
# Multi-stage build using uv for fast, reliable dependency management

# Build stage
FROM python:3.11-slim as builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install dependencies into a virtual environment
RUN uv sync --frozen --no-dev

# Production stage
FROM python:3.11-slim

# Install system dependencies for Spark (if needed)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv in production image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy virtual environment from builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Copy application source code
COPY main.py models.py database.py config.py ./
COPY .env* ./

# Create non-root user for security
RUN groupadd -r c360api && useradd -r -g c360api c360api
RUN chown -R c360api:c360api /app
USER c360api

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/app/.venv/bin:$PATH"
ENV ENVIRONMENT=production

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["uv", "run", "--frozen", "python", "main.py"]
