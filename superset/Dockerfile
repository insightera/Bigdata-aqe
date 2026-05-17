FROM apache/superset:4.0.2

# Trino: paket `trino` (bukan sqlalchemy-trino — max v0.5.0).
# Jangan upgrade pip/setuptools di image base: Superset 4.0.2 masih butuh pkg_resources (setuptools<81).
USER root
RUN pip install --no-cache-dir \
    "trino>=0.328,<0.340" \
    "setuptools>=65.0.0,<81.0.0"
USER superset

COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
