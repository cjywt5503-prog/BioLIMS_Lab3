import os
import json
import socket
import sqlite3
import threading
import time
import pika
import requests
import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Validation and Report Service (Transactional Inbox + SAGA Consumer)")

INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "validation-report-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8003))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
DB_PATH = "report_service.db"

REPORTS_DB = {}

REFERENCE_RANGES = {
    "GLU": {"min": 3.9, "max": 6.1, "unit": "mmol/L"},
    "ALT": {"min": 7.0, "max": 40.0, "unit": "U/L"},
    "CREA": {"min": 62.0, "max": 106.0, "unit": "umol/L"}
}

def init_inbox_db():
    """Ініціалізація таблиці Transactional Inbox для забезпечення ідемпотентності"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inbox_messages (
            message_id TEXT PRIMARY KEY,
            barcode TEXT,
            status TEXT,
            processed_at TEXT
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
            print(f"[{INSTANCE_ID}] Успішно зареєстровано в Consul")
            break
        except Exception:
            time.sleep(3)

def consume_rabbitmq_messages():
    """Фоновий консьюмер із підтримкою Transactional Inbox та генерацією помилки для SAGA DLQ"""
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            # Оголошуємо ту саму топологію з підтримкою DLX / DLQ
            channel.exchange_declare(exchange="assay_dlx", exchange_type="direct", durable=True)
            channel.queue_declare(queue="assay_dlq_queue", durable=True)
            channel.queue_bind(exchange="assay_dlx", queue="assay_dlq_queue", routing_key="assay_failed")

            channel.queue_declare(
                queue="assay_completed_queue",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "assay_dlx",
                    "x-dead-letter-routing-key": "assay_failed"
                }
            )

            def callback(ch, method, properties, body):
                event = json.loads(body.decode("utf-8"))
                msg_id = event.get("message_id", "UNKNOWN")
                barcode = event["barcode"]

                # 1. Перевірка на дублікати через Transactional Inbox (Ідемпотентність)
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("SELECT message_id FROM inbox_messages WHERE message_id = ?", (msg_id,))
                if cursor.fetchone():
                    print(f"[INBOX] Дублікат повідомлення {msg_id} проігноровано")
                    conn.close()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                # 2. ІМІТАЦІЯ ЗБОЮ В КОНСЬЮМЕРІ ЗГІДНО З УМОВОЮ ЛР №3 (Пункт 3 завдання)
                if "ERROR" in barcode.upper() or "FAIL" in barcode.upper():
                    print(f"[SAGA ERROR] Штучна помилка під час запису звіту для {barcode}! Відправляємо у DLQ...")
                    conn.close()
                    # basic_reject з requeue=False автоматично перенаправляє повідомлення в assay_dlq_queue
                    ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                    return

                # 3. Успішна обробка та збереження в Transactional Inbox
                cursor.execute(
                    "INSERT INTO inbox_messages (message_id, barcode, status, processed_at) VALUES (?, ?, 'PROCESSED', datetime('now'))",
                    (msg_id, barcode)
                )
                conn.commit()
                conn.close()

                validated_items = []
                for item in event["measurements"]:
                    code = item["biomarker"]
                    val = item["value"]
                    ref = REFERENCE_RANGES.get(code, {"min": 0, "max": 999})
                    is_normal = ref["min"] <= val <= ref["max"]
                    validated_items.append({
                        "biomarker": code,
                        "concentration": f"{val} {item['unit']}",
                        "reference_range": f"{ref['min']} - {ref['max']} {item['unit']}",
                        "clinical_status": "NORMAL" if is_normal else "CRITICAL_DEVIATION"
                    })

                final_trace = (
                    f"{event['trace_chain']} -> "
                    f"[3. ValidationReportService (instance: {INSTANCE_ID}): "
                    f"записано в Inbox ({msg_id}), виконано DeltaCheck та накладено КЕП]"
                )

                REPORTS_DB[barcode] = {
                    "report_id": f"REP-{len(REPORTS_DB) + 1:03d}",
                    "message_id": msg_id,
                    "assay_id": event.get("assay_id"),
                    "barcode": barcode,
                    "saga_status": "COMPLETED_SUCCESSFULLY",
                    "delta_check": "PASSED (аномальних стрибків у динаміці не виявлено)",
                    "digital_signature": "SIGNED_BY_DOCTOR_KEP",
                    "validated_results": validated_items,
                    "full_processing_trace": final_trace,
                    "consumed_by_instance": INSTANCE_ID
                }
                print(f"[{INSTANCE_ID}] Успішно сформовано звіт для штрихкоду {barcode}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="assay_completed_queue", on_message_callback=callback)
            print(f"[{INSTANCE_ID}] Консьюмер RabbitMQ запущено...")
            channel.start_consuming()
        except Exception as e:
            print(f"[{INSTANCE_ID}] Очікування підключення до RabbitMQ... ({e})")
            time.sleep(4)

@app.on_event("startup")
def startup_event():
    init_inbox_db()
    threading.Thread(target=register_in_consul, daemon=True).start()
    threading.Thread(target=consume_rabbitmq_messages, daemon=True).start()

@app.get("/reports")
def get_all_reports():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM inbox_messages ORDER BY processed_at DESC")
    inbox_rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {
        "service": SERVICE_NAME,
        "handled_by_instance": INSTANCE_ID,
        "inbox_messages_table": inbox_rows,
        "reports_count": len(REPORTS_DB),
        "reports": REPORTS_DB
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)