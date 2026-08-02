from sqlalchemy import create_engine
from urllib.parse import quote_plus

DB_USER = "postgres.qvbripbaosvdypxideyl"
DB_PASSWORD = quote_plus("!zWQT9jb%&P!d8B")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"
)
