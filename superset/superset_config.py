import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "lakehouse_superset_secret_change_me")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin123@postgres:5432/superset_db",
)
WTF_CSRF_ENABLED = True
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
