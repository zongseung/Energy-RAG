FROM python:3.11-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY db_init/db_config.py .

CMD ["uv", "run", "python", "db_config.py"]
