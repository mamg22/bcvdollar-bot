import datetime
import decimal
from os import environ
from pathlib import Path
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS Rates (
    effective_at DATETIME UNIQUE,
    value NUMERIC(13, 4)
)
"""


def get_datadir() -> str:
    datadir = environ.get("BCVBOT_DATADIR")
    if datadir is not None:
        datadir = Path(datadir).expanduser()
        datadir.mkdir(exist_ok=True)
        return datadir

    else:
        raise RuntimeError("Required BCVBOT_DATADIR environment variable is undefined!")


def get_db():
    return get_datadir() / "rates.db"


def adapt_decimal(val):
    return str(val)


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def convert_decimal(val):
    return decimal.Decimal(val.decode())


def convert_datetime(val):
    return datetime.datetime.fromisoformat(val.decode())


sqlite3.register_adapter(decimal.Decimal, adapt_decimal)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)

sqlite3.register_converter("numeric", convert_decimal)
sqlite3.register_converter("datetime", convert_datetime)


def create_database():
    with sqlite3.connect(get_db()) as conn:
        conn.execute(SCHEMA)
