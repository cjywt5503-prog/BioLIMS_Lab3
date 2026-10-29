"""Консьюмер: звіт + Inbox + подія успіху в одній локальній транзакції."""
import json
import logging
import os
import socket
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI

from common.consul import ServiceRegistration
from common.contracts import AssayCompletedEvent, AssayValidatedEvent
from common.messaging import ASSAY_QUEUE, SUCCESS_QUEUE, consume_forever, relay_forever
from common.storage import add_inbox, add_outbox, database, init_messaging_tables, now_iso

logger = logging.getLogger(__name__)
INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv('SERVICE_NAME', 'validation-report-service')
SERVICE_PORT = int(os.getenv('SERVICE_PORT', '8003'))
DB_PATH = os.getenv('REPORT_DB_PATH', '/data/report_v2.sqlite3')
REFERENCE_RANGES = {
    'GLU': {'min': 3.9, 'max': 6.1, 'unit': 'mmol/L'},
    'ALT': {'min': 7.0, 'max': 40.0, 'unit': 'U/L'},
    'CREA': {'min': 62.0, 'max': 106.0, 'unit': 'umol/L'},
}


class SimulatedWriteError(Exception):
    """Навмисний збій усередині транзакції запису звіту — вимога ЛР3."""


def init_db():
    with database(DB_PATH, write=True) as db:
        db.execute('''CREATE TABLE IF NOT EXISTS reports (
            assay_id TEXT PRIMARY KEY, message_id TEXT NOT NULL UNIQUE,
            barcode TEXT NOT NULL, created_at TEXT NOT NULL, payload_json TEXT NOT NULL
        )''')
        db.execute('CREATE INDEX IF NOT EXISTS reports_barcode ON reports(barcode, created_at)')
        init_messaging_tables(db)


def build_report(event):
    validated = []
    for item in event.measurements:
        ref = REFERENCE_RANGES[item.biomarker]
        if item.unit != ref['unit']:
            raise ValueError('Некоректна одиниця вимірювання')
        validated.append({
            'biomarker': item.biomarker,
            'concentration': f'{item.value} {item.unit}',
            'reference_range': f"{ref['min']} - {ref['max']} {ref['unit']}",
            'clinical_status': 'NORMAL' if ref['min'] <= item.value <= ref['max'] else 'OUTSIDE_REFERENCE_RANGE',
        })
    return {
        'report_id': 'REP-' + event.assay_id, 'message_id': event.message_id,
        'assay_id': event.assay_id, 'order_id': event.order_id, 'barcode': event.barcode,
        'saga_status': 'COMPLETED_SUCCESSFULLY', 'simulation': True,
        'delta_check': 'NOT_IMPLEMENTED', 'digital_signature': 'NOT_SIGNED',
        'validated_results': validated,
        'full_processing_trace': event.trace_chain + f' -> [3. ValidationReportService: перевірено демонстраційні інтервали; instance={INSTANCE_ID}]',
        'consumed_by_instance': INSTANCE_ID,
    }


def process_assay(event):
    """True -> ACK; False -> reject/DLQ. Технічні збої піднімаються для повтору."""
    try:
        with database(DB_PATH, write=True) as db:
            prior = db.execute('SELECT * FROM inbox_messages WHERE message_id=?', (event.message_id,)).fetchone()
            if prior is not None:
                if prior['assay_id'] != event.assay_id or prior['barcode'] != event.barcode:
                    raise ValueError('Повторний message_id з іншими даними')
                return prior['status'] == 'PROCESSED'
            report = build_report(event)
            db.execute('INSERT INTO reports VALUES (?, ?, ?, ?, ?)', (
                event.assay_id, event.message_id, event.barcode,
                event.created_at.astimezone(timezone.utc).isoformat(timespec='microseconds'),
                json.dumps(report, ensure_ascii=False),
            ))
            if event.simulate_failure:
                # Запис уже зроблено всередині транзакції, але COMMIT ще не було.
                # Виняток змусить database() виконати ROLLBACK.
                raise SimulatedWriteError('Штучна помилка запису звіту')
            add_inbox(db, event.message_id, event.assay_id, event.barcode)
            success = AssayValidatedEvent(
                message_id='OK-' + event.message_id, original_message_id=event.message_id,
                assay_id=event.assay_id, barcode=event.barcode, report_id=report['report_id'],
                created_at=datetime.now(timezone.utc), trace_chain=report['full_processing_trace'],
            )
            add_outbox(db, success, SUCCESS_QUEUE)
        return True
    except SimulatedWriteError:
        # Окремо фіксуємо саме відмову, а не успішну обробку. Повтор цієї події
        # теж відправляється в DLQ. Звіту та події успіху після rollback немає.
        with database(DB_PATH, write=True) as db:
            if not db.execute('SELECT 1 FROM inbox_messages WHERE message_id=?', (event.message_id,)).fetchone():
                add_inbox(db, event.message_id, event.assay_id, event.barcode, status='REJECTED')
        logger.info('Simulated write rolled back for %s; message will enter DLQ', event.assay_id)
        return False


def on_message(channel, method, properties, body):
    try:
        event = AssayCompletedEvent.model_validate_json(body)
        processed = process_assay(event)
    except ValueError:
        logger.exception('Invalid assay event rejected')
        processed = False
    if processed:
        # Усі три записи вже закомічені; після аварії повтор буде ідемпотентним.
        channel.basic_ack(delivery_tag=method.delivery_tag)
    else:
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    # sqlite3.Error тут не перетворюється на успіх: consume_forever закриє
    # з'єднання й повторить непідтверджене повідомлення після перепідключення.


@asynccontextmanager
async def lifespan(app):
    init_db()
    stopped = threading.Event()
    registration = ServiceRegistration(SERVICE_NAME, SERVICE_PORT, INSTANCE_ID)
    workers = [
        threading.Thread(target=consume_forever, args=(ASSAY_QUEUE, on_message, stopped), daemon=True),
        threading.Thread(target=relay_forever, args=(DB_PATH, stopped), daemon=True),
    ]
    registration.start()
    for worker in workers:
        worker.start()
    try:
        yield
    finally:
        stopped.set()
        for worker in workers:
            worker.join(timeout=12)
        registration.stop()


app = FastAPI(title='Report Service — Transactional Inbox and Outbox', lifespan=lifespan)


@app.get('/health')
def health():
    with database(DB_PATH) as db:
        db.execute('SELECT 1 FROM reports LIMIT 1')
    return {'status': 'UP', 'instance': INSTANCE_ID}


@app.get('/reports')
def get_all_reports():
    with database(DB_PATH) as db:
        rows = db.execute('SELECT * FROM reports ORDER BY created_at, assay_id').fetchall()
        inbox = [dict(row) for row in db.execute('SELECT * FROM inbox_messages ORDER BY processed_at DESC')]
        outbox = [dict(row) for row in db.execute('''SELECT message_id, assay_id, event_type,
            status, attempts, last_error FROM outbox_messages ORDER BY created_at DESC''')]
    reports = {row['barcode']: json.loads(row['payload_json']) for row in rows}
    return {'service': SERVICE_NAME, 'handled_by_instance': INSTANCE_ID,
            'inbox_messages_table': inbox, 'outbox_messages_table': outbox,
            'reports_count': len(reports), 'report_history_count': len(rows), 'reports': reports}


@app.get('/reports/{barcode}')
def get_report(barcode: str):
    with database(DB_PATH) as db:
        row = db.execute('''SELECT payload_json FROM reports WHERE barcode=?
            ORDER BY created_at DESC, assay_id DESC LIMIT 1''', (barcode,)).fetchone()
    return json.loads(row['payload_json']) if row else {'status': 'REPORT_NOT_AVAILABLE', 'barcode': barcode}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=SERVICE_PORT)
