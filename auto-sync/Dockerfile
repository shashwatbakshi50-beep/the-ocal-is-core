FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gosu \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py ./
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && chmod -R a+rX /app \
    && useradd --create-home --uid 10001 autosync \
    && mkdir -p /data/pmtiles /data/libheadway \
    && chown -R autosync:autosync /data

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
    CMD curl -fsS http://127.0.0.1:8000/health || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8000"]

# Adds the headway-cli binary (built from libheadway-main) so /api/pmtiles/extract
# works inside the container. Needs an extra build context:
#   docker build --build-context headway=<libheadway image> --target with-cli .
FROM headway AS headway-cli

FROM runtime AS with-cli
COPY --from=headway-cli /usr/local/bin/headway-cli /usr/local/bin/headway-cli

FROM runtime AS final
