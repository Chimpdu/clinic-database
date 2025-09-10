import psycopg
from contextlib import contextmanager
from db_config import USER_DSN, ADMIN_DSN

_CURRENT_DSN = USER_DSN  # when set_dsn is not called we use normal user by default

def set_dsn(role: str):
    """Switch the connection DSN by role string: 'super' => admin DSN, else user DSN."""
    global _CURRENT_DSN
    _CURRENT_DSN = ADMIN_DSN if role == "super" else USER_DSN

@contextmanager
# here we use contextmanager so it ensures that the connection is always close and it rollsback if this is exception
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
   
