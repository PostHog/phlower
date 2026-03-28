FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends nodejs npm \
    && npm i -g pnpm \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
RUN uv sync --frozen --no-dev

RUN cd src/phlower/static && CI=true pnpm install --frozen-lockfile

RUN adduser --system --no-create-home phlower
USER phlower

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8100

CMD ["python", "-m", "phlower"]
