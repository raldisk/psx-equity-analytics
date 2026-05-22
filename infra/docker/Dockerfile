FROM python:3.14-slim AS builder
WORKDIR /build
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir --prefix=/install -e ".[stats]"

FROM python:3.14-slim AS final
LABEL maintainer="raldisk@heraldcollamar.com"

RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --no-create-home appuser

COPY --from=builder /install /usr/local
COPY serving/ /opt/psx/serving/
COPY scripts/ /opt/psx/scripts/
COPY dbt/ /opt/psx/dbt/

ENV PSX_DATA_ROOT=/data \
    PSX_API_PORT=8000 \
    PYTHONUNBUFFERED=1

USER appuser
WORKDIR /opt/psx

HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

EXPOSE 8000
CMD ["uvicorn", "serving.psx_analytics_api:app", "--host", "0.0.0.0", "--port", "8000"]
