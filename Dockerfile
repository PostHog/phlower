FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm i -g pnpm \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Python deps
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
RUN uv sync --frozen --no-dev

# React frontend
COPY frontend/ frontend/
RUN cd frontend && CI=true pnpm install --frozen-lockfile && pnpm build

RUN adduser --system --no-create-home phlower \
    && mkdir -p /data && chown phlower /data
USER phlower

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8100

CMD ["python", "-m", "phlower"]
