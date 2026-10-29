"""Продьюсер SAGA: локальна транзакція, Outbox, успіх та компенсація з DLQ."""
import json
import logging
import os
import socket
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import requests
import uvicorn
from fastapi import FastAPI, HTTPException

from common.consul import RoundRobinDiscovery, ServiceRegistration
from common.contracts import AssayCompletedEvent, AssayRunRequest, AssayValidatedEvent, OrderLookup
from common.messaging import ASSAY_QUEUE, DLQ_QUEUE, SUCCESS_QUEUE, consume_forever, relay_forever
from common.storage import add_inbox, add_outbox, database, init_messaging_tables, now_iso

logger = logging.getLogger(__name__)
INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv('SERVICE_NAME', 'assay-execution-service')
SERVICE_PORT = int(os.getenv('SERVICE_PORT', '8002'))
DB_PATH = os.getenv('ASSAY_DB_PATH', '/data/assay_v2.sqlite3')
discovery = RoundRobinDiscovery()
DEMO_MEASUREMENTS = {
    'GLU': {'biomarker': 'GLU', 'optical_density': 0.412, 'value': 5.4, 'unit': 'mmol/L'},
    'ALT': {'biomarker': 'ALT', 'optical_density': 0.195, 'value': 28.0, 'unit': 'U/L'},
    'CREA': {'biomarker': 'CREA', 'optical_density': 0.310, 'value': 82.5, 'unit': 'umol/L'},
}


def init_db():
    with database(DB_PATH, write=True) as db:
        db.execute('''CREATE TABLE IF NOT EXISTS assays_data (
            assay_id TEXT PRIMARY KEY, message_id TEXT NOT NULL UNIQUE,
            order_id TEXT NOT NULL, barcode TEXT NOT NULL,
            saga_status TEXT NOT NULL, measurements_json TEXT NOT NULL,
            trace_chain TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )''')
        init_messaging_tables(db)


def load_order(req):
    nodes = discovery.candidates('order-sample-service')
    for node in nodes:
        try:
            response = requests.get(
                f'http://{node}/orders/by-barcode/{req.barcode}',
                params={'client_note': req.initial_message}, timeout=5,
            )
        except requests.RequestException:
            continue
        if response.status_code == 404:
            raise HTTPException(404, 'Замовлення не знайдено')
        if response.status_code >= 500:
            continue
        if response.status_code != 200:
            raise HTTPException(502, 'Неочікуваний статус відповіді OrderService')
        try:
            order = OrderLookup.model_validate(response.json())
            if order.order.barcode != req.barcode:
                raise ValueError('Barcode mismatch')
        except (ValueError, TypeError) as exc:
            raise HTTPException(502, 'Некоректна відповідь OrderService') from exc
        return order, node, sorted(nodes)
    raise HTTPException(503, 'Знайдені репліки OrderService недоступні')


def apply_terminal_event(*, inbox_id, original_id, assay_id, barcode, status, trace):
    """Атомарне завершення/компенсація, спільне для всіх реплік продьюсера."""
    with database(DB_PATH, write=True) as db:
        if db.execute('SELECT 1 FROM inbox_messages WHERE message_id=?', (inbox_id,)).fetchone():
            return
        assay = db.execute('SELECT * FROM assays_data WHERE assay_id=?', (assay_id,)).fetchone()
        if assay is None:
            # Не ACK: тимчасова проблема сховища/маршрутизації не має губити компенсацію.
            raise RuntimeError(f'Assay {assay_id} not found; retry required')
        if assay['message_id'] != original_id or assay['barcode'] != barcode:
            raise ValueError('Подія не відповідає аналізу')
        current = assay['saga_status']
        if current not in ('PENDING_VALIDATION', status):
            raise ValueError(f'Конфлікт термінальних станів SAGA: {current} -> {status}')
        if current == 'PENDING_VALIDATION':
            db.execute('''UPDATE assays_data SET saga_status=?, trace_chain=?, updated_at=?
                WHERE assay_id=? AND saga_status='PENDING_VALIDATION' ''',
                       (status, trace, now_iso(), assay_id))
        if status == 'ROLLED_BACK_BY_SAGA_DLQ':
            db.execute('''UPDATE outbox_messages SET status='COMPENSATED_FROM_DLQ',
                claim_token=NULL, lease_until=0 WHERE message_id=?''', (original_id,))
        add_inbox(db, inbox_id, assay_id, barcode)


def on_success(channel, method, properties, body):
    try:
        event = AssayValidatedEvent.model_validate_json(body)
        apply_terminal_event(
            inbox_id=event.message_id, original_id=event.original_message_id,
            assay_id=event.assay_id, barcode=event.barcode, status='COMPLETED_SUCCESSFULLY',
            trace=event.trace_chain + f' -> [SAGA SUCCESS: {INSTANCE_ID}]',
        )
    except ValueError:
        logger.exception('Invalid or conflicting success event rejected')
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    else:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def on_compensation(channel, method, properties, body):
    try:
        event = AssayCompletedEvent.model_validate_json(body)
        apply_terminal_event(
            inbox_id='DLQ-' + event.message_id, original_id=event.message_id,
            assay_id=event.assay_id, barcode=event.barcode, status='ROLLED_BACK_BY_SAGA_DLQ',
            trace=event.trace_chain + f' -> [SAGA COMPENSATION: аналіз скасовано; instance={INSTANCE_ID}]',
        )
    except ValueError:
        logger.exception('Invalid or conflicting DLQ event rejected')
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    else:
        channel.basic_ack(delivery_tag=method.delivery_tag)


@asynccontextmanager
async def lifespan(app):
    init_db()
    stopped = threading.Event()
    registration = ServiceRegistration(SERVICE_NAME, SERVICE_PORT, INSTANCE_ID)
    workers = [
        threading.Thread(target=relay_forever, args=(DB_PATH, stopped), daemon=True),
        threading.Thread(target=consume_forever, args=(SUCCESS_QUEUE, on_success, stopped), daemon=True),
        threading.Thread(target=consume_forever, args=(DLQ_QUEUE, on_compensation, stopped), daemon=True),
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


app = FastAPI(title='Assay Service — Transactional Outbox and SAGA', lifespan=lifespan)


@app.get('/health')
def health():
    with database(DB_PATH) as db:
        db.execute('SELECT 1 FROM assays_data LIMIT 1')
    # Відсутність RabbitMQ не блокує приймання запитів: для цього існує Outbox.
    return {'status': 'UP', 'instance': INSTANCE_ID}


@app.post('/assays/run')
def run_assay(req: AssayRunRequest):
    order, selected_node, nodes = load_order(req)
    assay_id = 'ASSAY-' + str(uuid4())
    message_id = 'MSG-' + str(uuid4())
    measurements = [DEMO_MEASUREMENTS[code].copy() for code in order.order.biomarkers]
    trace = order.trace_chain + f' -> [2. AssayService: навчальні результати; instance={INSTANCE_ID}; assay={assay_id}]'
    event = AssayCompletedEvent(
        message_id=message_id, assay_id=assay_id, barcode=req.barcode,
        created_at=datetime.now(timezone.utc), order_id=order.order.order_id,
        order_instance=order.handled_by_order_instance, assay_instance=INSTANCE_ID,
        measurements=measurements, trace_chain=trace,
        simulate_failure=req.simulate_failure or 'ERROR' in req.barcode.upper() or 'FAIL' in req.barcode.upper(),
    )
    with database(DB_PATH, write=True) as db:
        timestamp = now_iso()
        db.execute('''INSERT INTO assays_data
            (assay_id, message_id, order_id, barcode, saga_status, measurements_json, trace_chain, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'PENDING_VALIDATION', ?, ?, ?, ?)''', (
            assay_id, message_id, order.order.order_id, req.barcode,
            json.dumps(measurements), trace, timestamp, timestamp,
        ))
        add_outbox(db, event, ASSAY_QUEUE)
    return {
        'status': 'SAVED_TO_DB_AND_OUTBOX', 'assay_id': assay_id,
        'outbox_message_id': message_id, 'saga_status': 'PENDING_VALIDATION',
        'handled_by_order_instance': order.handled_by_order_instance,
        'handled_by_assay_instance': INSTANCE_ID, 'selected_order_service': selected_node,
        'consul_discovered_order_instances': nodes, 'trace_chain': trace,
        'measurements': measurements, 'simulation': True,
    }


@app.get('/assays/outbox')
def get_outbox_state():
    with database(DB_PATH) as db:
        assays = [dict(row) for row in db.execute('SELECT * FROM assays_data ORDER BY created_at DESC')]
        outbox = [dict(row) for row in db.execute('''SELECT message_id, assay_id, event_type,
            target_queue, status, attempts, last_error, created_at, published_at
            FROM outbox_messages ORDER BY created_at DESC''')]
    return {'service': SERVICE_NAME, 'instance': INSTANCE_ID,
            'assays_data_table': assays, 'outbox_messages_table': outbox}


@app.get('/assays/{assay_id}')
def get_assay(assay_id: str):
    with database(DB_PATH) as db:
        row = db.execute('SELECT * FROM assays_data WHERE assay_id=?', (assay_id,)).fetchone()
    if row is None:
        raise HTTPException(404, 'Аналіз не знайдено')
    return dict(row)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=SERVICE_PORT)
