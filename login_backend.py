import psycopg
from db_config import USER_DSN  
def check_admin(name, password) -> bool:
    with psycopg.connect(USER_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM admins WHERE name=%s AND password=%s", (name, password))
            return cur.fetchone() is not None

def check_user(name, password) -> bool:
    with psycopg.connect(USER_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE name=%s AND password=%s", (name, password))
            return cur.fetchone() is not None

def insert_user(name: str, password: str):
    """Self-register a normal user. Users.name is UNIQUE."""
    with psycopg.connect(USER_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (name, password) VALUES (%s, %s)",
                (name, password)
            )
