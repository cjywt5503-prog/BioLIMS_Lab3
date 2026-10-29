import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


@contextmanager
def database(path, *, write=False):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('PRAGMA foreign_keys=ON')
        if write:
            # Серіалізує локальні зміни між потоками й репліками одного сервісу.
            conn.execute('BEGIN IMMEDIATE')
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_messaging_tables(db):
    db.execute('''CREATE TABLE IF NOT EXISTS outbox_messages (
        message_id TEXT PRIMARY KEY,
        assay_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        target_queue TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        created_at TEXT NOT NULL,
        published_at TEXT,
        claim_token TEXT,
        lease_until REAL NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT
    )''')
    db.execute('CREATE INDEX IF NOT EXISTS outbox_pending ON outbox_messages(status, lease_until)')
    db.execute('''CREATE TABLE IF NOT EXISTS inbox_messages (
        message_id TEXT PRIMARY KEY,
        assay_id TEXT NOT NULL,
        barcode TEXT NOT NULL,
        status TEXT NOT NULL,
        processed_at TEXT NOT NULL
    )''')


def add_outbox(db, event, target_queue):
    # db — та сама транзакція, що записує бізнес-дані.
    db.execute('''INSERT INTO outbox_messages
        (message_id, assay_id, event_type, target_queue, payload_json, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'PENDING', ?)''', (
        event.message_id, event.assay_id, event.event_type, target_queue,
        event.model_dump_json(), now_iso(),
    ))


def add_inbox(db, message_id, assay_id, barcode, status='PROCESSED'):
    db.execute('INSERT INTO inbox_messages VALUES (?, ?, ?, ?, ?)',
               (message_id, assay_id, barcode, status, now_iso()))
