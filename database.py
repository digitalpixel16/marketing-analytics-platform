from sqlalchemy import create_engine
from urllib.parse import quote_plus

DB_USER = "postgres"
DB_PASSWORD = quote_plus("!zWQT9jb%&P!d8B")
DB_HOST = "db.qvbripbaosvdypxideyl.supabase.co"
DB_PORT = "5432"
DB_NAME = "analytics_db"

engine = create_engine(
    f"postgresql://postgres:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
)
