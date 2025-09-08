import psycopg
from contextlib import contextmanager
from db_config import USER_DSN, ADMIN_DSN

_CURRENT_DSN = USER_DSN  # default to least privilege

def set_dsn(role: str):
    """Switch the connection DSN by role string: 'super' => admin DSN, else user DSN."""
    global _CURRENT_DSN
    _CURRENT_DSN = ADMIN_DSN if role == "super" else USER_DSN

@contextmanager
def get_conn():
    """
    Usage:
        with get_conn() as (conn, cur):
            cur.execute("SELECT 1")
            rows = cur.fetchall()
            conn.commit()
    """
    with psycopg.connect(_CURRENT_DSN, autocommit=False) as conn:
        with conn.cursor() as cur:
            yield conn, cur
   
