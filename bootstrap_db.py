import os
import psycopg
import getpass
from urllib.parse import quote as urlquote, urlparse

def build_superuser_dsn():
    env = os.getenv("SUPERUSER_URL")
    if env and env.strip():
        return env.strip()

    print("SUPERUSER_URL not set. Enter PostgreSQL superuser connection info.")
    host = input("Host [localhost]: ").strip() or "localhost"
    port = input("Port [5432]: ").strip() or "5432"
    user = input("User [postgres]: ").strip() or "postgres"
    pw   = getpass.getpass(f"Password for {user}@{host}:{port}/postgres: ")
    return f"postgresql://{user}:{urlquote(pw)}@{host}:{port}/postgres"

def admin_dsn_from_superuser(super_dsn: str) -> str:
    """Build an admin DSN for clinic_db using same host/port as superuser DSN."""
    p = urlparse(super_dsn)
    host = p.hostname or "localhost"
    port = p.port or 5432
    return f"postgresql://admin:admin@{host}:{port}/clinic_db"

SUPERUSER_URL    = build_superuser_dsn()
ADMIN_CLINIC_DSN = admin_dsn_from_superuser(SUPERUSER_URL)

DDL_ROLES_DB = r"""
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
      CREATE ROLE admin LOGIN PASSWORD 'admin';
   ELSE
      ALTER ROLE admin WITH LOGIN PASSWORD 'admin';
   END IF;

   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'user') THEN
      CREATE ROLE "user" LOGIN PASSWORD 'user';
   ELSE
      ALTER ROLE "user" WITH LOGIN PASSWORD 'user';
   END IF;
END$$;
"""

DDL_CREATE_DB = "CREATE DATABASE clinic_db OWNER admin;"

DDL_PRIVS = r"""
GRANT CONNECT ON DATABASE clinic_db TO "user";
"""

SCHEMA_SQL = r"""
-- main tables here (lowercase names to avoid quoting)
CREATE TABLE IF NOT EXISTS person(
    personnumer   TEXT PRIMARY KEY,
    full_name     TEXT
);

CREATE TABLE IF NOT EXISTS clinic(
    cli_id        TEXT PRIMARY KEY,
    cli_name      TEXT,
    address       TEXT
);

CREATE TABLE IF NOT EXISTS department(
    dept_id       TEXT PRIMARY KEY,
    dept_name     TEXT,
    cli_id        TEXT REFERENCES clinic(cli_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS doctor(
    personnumer           TEXT PRIMARY KEY REFERENCES person(personnumer) ON DELETE CASCADE,
    doctor_id             TEXT NOT NULL UNIQUE,
    dept_id               TEXT REFERENCES department(dept_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS patient(
    personnumer           TEXT PRIMARY KEY REFERENCES person(personnumer) ON DELETE CASCADE,
    patient_id            TEXT NOT NULL UNIQUE,
    doctor_personnumer    TEXT REFERENCES doctor(personnumer)
);

CREATE TABLE IF NOT EXISTS appointment(
    appoint_id            VARCHAR(20) PRIMARY KEY,
    appoint_year          INT,
    appoint_month         INT,
    appoint_day           INT,
    appoint_location      TEXT,
    patient_personnumer   TEXT REFERENCES patient(personnumer),
    doctor_personnumer    TEXT REFERENCES doctor(personnumer)
);

CREATE TABLE IF NOT EXISTS observation(
    obser_id              TEXT PRIMARY KEY,
    obs_year              INT,
    obs_month             INT,
    obs_day               INT,
    obs_comment           BYTEA,
    appoint_id            TEXT REFERENCES appointment(appoint_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS diagnosis(
    diagn_id              TEXT PRIMARY KEY,
    diagn_year            INT,
    diagn_month           INT,
    diagn_day             INT,
    diagn_comment         BYTEA,
    obser_id              TEXT REFERENCES observation(obser_id) ON DELETE SET NULL
);

-- App login tables (name must be unique)
CREATE TABLE IF NOT EXISTS admins(
    id        BIGSERIAL PRIMARY KEY,
    name      TEXT UNIQUE NOT NULL,
    password  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users(
    id        BIGSERIAL PRIMARY KEY,
    name      TEXT UNIQUE NOT NULL,
    password  TEXT NOT NULL
);

-- Privileges for read-only "user" role (tables)
REVOKE ALL ON SCHEMA public FROM "user";
GRANT  USAGE ON SCHEMA public TO   "user";

REVOKE ALL   ON ALL TABLES IN SCHEMA public FROM "user";
GRANT  SELECT ON ALL TABLES IN SCHEMA public TO   "user";

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT  SELECT ON TABLES TO   "user";

-- Sequences: allow "user" to use nextval() for identity/serial columns
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM "user";
GRANT  USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "user";

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT  USAGE, SELECT ON SEQUENCES TO "user";

-- Allow self-registration: INSERT into users
GRANT INSERT ON TABLE users TO "user";
"""


SEED_SQL = r"""
INSERT INTO admins (name, password) VALUES ('admin', 'admin')
ON CONFLICT (name) DO NOTHING;

INSERT INTO users (name, password) VALUES ('user1', 'user123')
ON CONFLICT (name) DO NOTHING;
"""

def main():
    # 1) Create roles & DB using superuser (connect to 'postgres')
    with psycopg.connect(SUPERUSER_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_ROLES_DB)
            cur.execute("SELECT 1 FROM pg_database WHERE datname='clinic_db';")
            if cur.fetchone() is None:
                cur.execute(DDL_CREATE_DB)
            cur.execute(DDL_PRIVS)

    # 2) Create schema & grants & seed accounts inside clinic_db as admin
    with psycopg.connect(ADMIN_CLINIC_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            cur.execute(SEED_SQL)

    print("   clinic_db ready.")
    print("   Default admin: admin / admin")
    print("   Default normal user: user1 / user123")

if __name__ == "__main__":
    main()
