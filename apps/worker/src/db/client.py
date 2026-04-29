from contextlib import contextmanager
from psycopg import connect
from psycopg.rows import dict_row
from src.config import get_settings

@contextmanager
def get_connection():
    settings = get_settings()
    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        yield conn
