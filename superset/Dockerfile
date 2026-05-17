FROM apache/superset:4.0.2

# Koneksi Trino di Superset memakai paket `trino` (trino-python-client).
# Jangan pasang sqlalchemy-trino>=0.328 — paket itu max v0.5.0 dan sudah deprecated.
USER root
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir "trino>=0.328,<0.340"
USER superset

COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
