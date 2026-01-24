# Use Python 3.12 slim image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Create non-root user and set permissions
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app

USER app

# Env vars
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files
COPY --chown=app:app pyproject.toml uv.lock ./

# Install dependencies as 'app'
RUN uv sync --frozen --no-dev

# Copy application code
COPY --chown=app:app common/ ./common/
COPY --chown=app:app core/ ./core/
COPY --chown=app:app setting/ ./setting/
COPY --chown=app:app transport/ ./transport/
COPY --chown=app:app main.py ./

# Run the application
CMD ["uv", "run", "python", "main.py"]
