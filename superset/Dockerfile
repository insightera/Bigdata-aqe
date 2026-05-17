FROM apache/superset:4.0.2

USER root
RUN pip install --no-cache-dir \
    "sqlalchemy-trino>=0.328.0" \
    "trino>=0.328.0"
USER superset

COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
