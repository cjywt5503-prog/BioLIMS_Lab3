import os
import json
import uuid
import socket
import sqlite3
import threading
import time
import pika
import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Assay Execution Service (Transactional Outbox + SAGA Producer)")

INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "assay-execution-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8002))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-sample-service:8001")
DB_PATH = "assay_service.db"

class AssayRunRequest(BaseModel):
    barcode: str = "CODE128-1001"
    initial_message: str = "Start Lab Test"

def init_db():
    """Ініціалізація БД мікросервісу: основна таблиця даних та таблиця Transactional Outbox"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 1. Таблиця бізнес-даних (локальний стан SAGA)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS assays_data (
            assay_id TEXT PRIMARY KEY,
            barcode TEXT,
            saga_status TEXT,
            measurements_json TEXT,
            trace_chain TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    # 2. Таблиця Transactional Outbox для гарантованої доставки повідомлень
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS outbox_messages (
            message_id TEXT PRIMARY KEY,
            assay_id TEXT,
            event_type TEXT,
            payload_json TEXT,
            status TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def register_in_consul():
    url = f"http://{CONSUL_HOST}:8500/v1/agent/service/register"
    payload = {
        "ID": f"{SERVICE_NAME}-{INSTANCE_ID}",
        "Name": SERVICE_NAME,
        "Address": INSTANCE_ID,
        "Port": SERVICE_PORT
    }
    for _ in range(10):
        try:
            requests.put(url, json=payload, timeout=3)
            print(f"[{INSTANCE_ID}] Зареєстровано в Consul")
            break
        except Exception:
            time.sleep(3)

def discover_service(service_name: str):
    try:
        resp = requests.get(f"http://{CONSUL_HOST}:8500/v1/catalog/service/{service_name}", timeout=2)
        instances = resp.json()
        return [f"{item['ServiceAddress']}:{item['ServicePort']}" for item in instances]
    except Exception:
        return []

def setup_rabbitmq_topology(channel):
    """Налаштування основної черги та Dead Letter Queue (DLQ) для SAGA"""
    # Оголошуємо Dead Letter Exchange та чергу DLQ для помилкових транзакцій
    channel.exchange_declare(exchange="assay_dlx", exchange_type="direct", durable=True)
    channel.queue_declare(queue="assay_dlq_queue", durable=True)
    channel.queue_bind(exchange="assay_dlx", queue="assay_dlq_queue", routing_key="assay_failed")

    # Оголошуємо основну чергу з перенаправленням відхилених повідомлень у DLX
    channel.queue_declare(
        queue="assay_completed_queue",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "assay_dlx",
            "x-dead-letter-routing-key": "assay_failed"
        }
    )

def outbox_relay_worker():
    """
    Фоновий процес Transactional Outbox Relay:
    Вичитує повідомлення зі статусом PENDING із таблиці outbox_messages
    та публікує їх у RabbitMQ (гарантія At-least-once).
    """
    while True:
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM outbox_messages WHERE status = 'PENDING' ORDER BY created_at ASC")
            pending_rows = cursor.fetchall()

            if pending_rows:
                rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
                channel = rmq_conn.channel()
                setup_rabbitmq_topology(channel)

                for row in pending_rows:
                    msg_id = row["message_id"]
                    channel.basic_publish(
                        exchange="",
                        routing_key="assay_completed_queue",
                        body=row["payload_json"],
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            message_id=msg_id
                        )
                    )
                    cursor.execute(
                        "UPDATE outbox_messages SET status = 'SENT_TO_BROKER' WHERE message_id = ?",
                        (msg_id,)
                    )
                    conn.commit()
                    print(f"[OUTBOX RELAY] Повідомлення {msg_id} успішно відправлено з Outbox-таблиці в RabbitMQ")

                rmq_conn.close()
            conn.close()
        except Exception as e:
            print(f"[OUTBOX RELAY] Очікування RabbitMQ або БД... ({e})")
        time.sleep(2)

def saga_dlq_compensation_listener():
    """
    Слухач Dead Letter Queue (DLQ) для виконання компенсаційної транзакції SAGA:
    Якщо сервіс-консьюмер кинув помилку, повідомлення потрапляє в assay_dlq_queue,
    і сервіс-продьюсер виконує відкат (Rollback) локальної транзакції.
    """
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            setup_rabbitmq_topology(channel)

            def dlq_callback(ch, method, properties, body):
                event = json.loads(body.decode("utf-8"))
                assay_id = event["assay_id"]
                barcode = event["barcode"]
                msg_id = event["message_id"]

                # КОМПЕНСАЦІЙНА ТРАНЗАКЦІЯ (SAGA Rollback) — ідемпотентне скасування змін у БД
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                rollback_trace = (
                    f"{event['trace_chain']} -> "
                    f"[SAGA COMPENSATION у AssayService: отримано повідомлення з DLQ, "
                    f"транзакцію {assay_id} для {barcode} ВІДКОЧЕНО (ROLLED_BACK)]"
                )
                cursor.execute("""
                    UPDATE assays_data
                    SET saga_status = 'ROLLED_BACK_BY_SAGA_DLQ',
                        trace_chain = ?,
                        updated_at = datetime('now')
                    WHERE assay_id = ?
                """, (rollback_trace, assay_id))

                cursor.execute("""
                    UPDATE outbox_messages
                    SET status = 'COMPENSATED_FROM_DLQ'
                    WHERE message_id = ?
                """, (msg_id,))
                conn.commit()
                conn.close()

                print(f"[SAGA ROLLBACK] Компенсаційна транзакція виконана для assay_id={assay_id} (barcode={barcode})")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="assay_dlq_queue", on_message_callback=dlq_callback)
            print(f"[{INSTANCE_ID}] SAGA DLQ Listener запущено...")
            channel.start_consuming()
        except Exception as e:
            print(f"[SAGA DLQ] Очікування підключення до RabbitMQ... ({e})")
            time.sleep(4)

@app.on_event("startup")
def startup_event():
    init_db()
    threading.Thread(target=register_in_consul, daemon=True).start()
    threading.Thread(target=outbox_relay_worker, daemon=True).start()
    threading.Thread(target=saga_dlq_compensation_listener, daemon=True).start()

@app.post("/assays/run")
def run_assay(req: AssayRunRequest):
    """
    Запуск розподіленої транзакції SAGA через патерн Transactional Outbox:
    Атомарно зберігає дані аналізу в assays_data та подію в outbox_messages.
    """
    discovered_nodes = discover_service("order-sample-service")

    sync_url = f"{ORDER_SERVICE_URL}/orders/by-barcode/{req.barcode}"
    sync_resp = requests.get(
        sync_url,
        params={"client_note": req.initial_message},
        headers={"Connection": "close"},
        timeout=5
    ).json()

    measurements = [
        {"biomarker": "GLU", "optical_density": 0.412, "value": 5.4, "unit": "mmol/L"},
        {"biomarker": "ALT", "optical_density": 0.195, "value": 28.0, "unit": "U/L"},
        {"biomarker": "CREA", "optical_density": 0.310, "value": 82.5, "unit": "umol/L"}
    ]

    assay_id = f"ASSAY-{uuid.uuid4().hex[:8].upper()}"
    message_id = f"MSG-{uuid.uuid4().hex[:8].upper()}"

    full_trace = (
        f"{sync_resp['trace_chain']} -> "
        f"[2. AssayService (instance: {INSTANCE_ID}): збережено в БД та Outbox-таблицю (assay_id={assay_id})]"
    )

    event_data = {
        "message_id": message_id,
        "assay_id": assay_id,
        "event_type": "AssayCompletedEvent",
        "barcode": req.barcode,
        "order_instance": sync_resp["handled_by_order_instance"],
        "assay_instance": INSTANCE_ID,
        "measurements": measurements,
        "trace_chain": full_trace
    }

    # АТОМАРНА ЛОКАЛЬНА ТРАНЗАКЦІЯ БД (Transactional Outbox Pattern)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO assays_data (assay_id, barcode, saga_status, measurements_json, trace_chain, created_at, updated_at)
            VALUES (?, ?, 'PENDING_VALIDATION', ?, ?, datetime('now'), datetime('now'))
        """, (assay_id, req.barcode, json.dumps(measurements, ensure_ascii=False), full_trace))

        cursor.execute("""
            INSERT INTO outbox_messages (message_id, assay_id, event_type, payload_json, status, created_at)
            VALUES (?, ?, 'AssayCompletedEvent', ?, 'PENDING', datetime('now'))
        """, (message_id, assay_id, json.dumps(event_data, ensure_ascii=False)))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

    return {
        "status": "SAVED_TO_DB_AND_OUTBOX",
        "assay_id": assay_id,
        "outbox_message_id": message_id,
        "saga_status": "PENDING_VALIDATION",
        "handled_by_order_instance": sync_resp["handled_by_order_instance"],
        "handled_by_assay_instance": INSTANCE_ID,
        "trace_chain": full_trace
    }

@app.get("/assays/outbox")
def get_outbox_state():
    """Перегляд стану БД мікросервісу: основної таблиці даних та таблиці Transactional Outbox"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM assays_data ORDER BY created_at DESC")
    assays = [dict(row) for row in cursor.fetchall()]
    cursor.execute("SELECT message_id, assay_id, event_type, status, created_at FROM outbox_messages ORDER BY created_at DESC")
    outbox = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "assays_data_table": assays,
        "outbox_messages_table": outbox
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)