from os import environ

import psycopg

SCHEMA = """
CREATE TABLE Rates (
    effective_at TIMESTAMP WITH TIME ZONE UNIQUE,
    value NUMERIC(13, 4)
)
"""


def get_db_settings() -> str:
    conn_str = environ["BCVBOT_PGCONN"]
    if conn_str is not None:
        return conn_str
    else:
        raise RuntimeError("Required BCVBOT_PGCONN environment variable is undefined!")


def create_database():
    with psycopg.connect(get_db_settings()) as conn:
        conn.execute(SCHEMA)
