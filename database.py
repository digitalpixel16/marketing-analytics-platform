from sqlalchemy import create_engine
from urllib.parse import quote_plus

DB_USER = "postgres"
DB_PASSWORD = quote_plus("Pixel@16")
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "analytics_db"

engine = create_engine(f"postgresql://postgres:{DB_PASSWORD}@localhost:5432/{DB_NAME}")
