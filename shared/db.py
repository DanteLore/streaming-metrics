import sqlite3
from contextlib import contextmanager

from shared.schema import DB_PATH


@contextmanager
def get_conn(path=DB_PATH):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(path=DB_PATH):
    with get_conn(path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS latest_readings (
                device_id  TEXT PRIMARY KEY,
                value      REAL NOT NULL,
                event_time TEXT NOT NULL,
                received_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS temp_metrics (
                window_start TEXT NOT NULL,
                window_end   TEXT NOT NULL,
                temp_sum     REAL NOT NULL,
                temp_count   INTEGER NOT NULL,
                computed_at  TEXT NOT NULL,
                PRIMARY KEY (window_start, window_end)
            );

            CREATE TABLE IF NOT EXISTS production_hourly (
                hour            TEXT PRIMARY KEY,
                total_sausages  REAL NOT NULL,
                computed_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS compliance_violations (
                window_start TEXT PRIMARY KEY,
                window_end   TEXT NOT NULL,
                mean_temp    REAL NOT NULL,
                computed_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS vibration_metrics (
                window_start   TEXT NOT NULL,
                window_end     TEXT NOT NULL,
                mean_vibration REAL NOT NULL,
                computed_at    TEXT NOT NULL,
                PRIMARY KEY (window_start, window_end)
            );
        """)
