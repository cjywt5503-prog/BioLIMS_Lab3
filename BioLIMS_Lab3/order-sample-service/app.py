import json
import os
import socket
import sqlite3
from contextlib import asynccontextmanager
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException

from common.consul import ServiceRegistration
from common.contracts import CreateOrderRequest
from common.storage import database


INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "order-sample-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8001))
DB_PATH = os.getenv("ORDER_DB_PATH", "/data/orders_v2.sqlite3")


def init_db():
    seed = {
        "order_id": "ORD-001", "patient_id": "PAT-409", "barcode": "CODE128-1001",
        "biomarkers": ["GLU", "ALT", "CREA"], "status": "SAMPLE_COLLECTED",
    }
    error_seed = {
        "order_id": "ORD-DEMO-ERROR", "patient_id": "PAT-TEST", "barcode": "CODE128-ERROR",
        "biomarkers": ["GLU", "ALT", "CREA"], "status": "SAMPLE_COLLECTED",
    }
    with database(DB_PATH, write=True) as db:
        db.execute("CREATE TABLE IF NOT EXISTS orders (barcode TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        # Два явні навчальні замовлення. Інші невідомі штрихкоди повертають 404.
        for order in (seed, error_seed):
            db.execute("INSERT OR IGNORE INTO orders VALUES (?, ?)", (order["barcode"], json.dumps(order)))


@asynccontextmanager
async def lifespan(app):
    init_db()
    registration = ServiceRegistration(SERVICE_NAME, SERVICE_PORT, INSTANCE_ID)
    registration.start()
    try:
        yield
    finally:
        registration.stop()


app = FastAPI(title="Order and Sample Service (Intake Context)", lifespan=lifespan)


@app.get("/health")
def health():
    with database(DB_PATH) as db:
        db.execute("SELECT 1 FROM orders LIMIT 1")
    return {"status": "UP", "instance": INSTANCE_ID}


@app.get("/orders")
def list_orders():
    with database(DB_PATH) as db:
        orders = [json.loads(row["payload"]) for row in db.execute("SELECT payload FROM orders ORDER BY barcode")]
    return {
        "service": SERVICE_NAME, "handled_by_instance": INSTANCE_ID,
        "orders_count": len(orders), "orders": orders,
    }


@app.post("/orders", status_code=201)
def create_order(req: CreateOrderRequest):
    order = {"order_id": f"ORD-{uuid4()}", **req.model_dump(), "status": "SAMPLE_COLLECTED"}
    try:
        with database(DB_PATH, write=True) as db:
            db.execute("INSERT INTO orders VALUES (?, ?)", (req.barcode, json.dumps(order, ensure_ascii=False)))
    except sqlite3.IntegrityError as exc:
        raise HTTPException(409, "Замовлення з таким штрихкодом уже існує") from exc
    return {
        "message": "Замовлення створено та штрихкод нанесено на пробірку",
        "handled_by_instance": INSTANCE_ID, "order": order,
    }


@app.get("/orders/by-barcode/{barcode}")
def get_order_by_barcode(barcode: str, client_note: str = "Init"):
    """Open Host Service: читає реальне замовлення зі спільної БД реплік."""
    with database(DB_PATH) as db:
        row = db.execute("SELECT payload FROM orders WHERE barcode = ?", (barcode,)).fetchone()
    if row is None:
        raise HTTPException(404, "Замовлення з таким штрихкодом не знайдено")
    order = json.loads(row["payload"])
    return {
        "order": order,
        "handled_by_order_instance": INSTANCE_ID,
        "trace_chain": (
            f"{client_note} -> [1. OrderService (instance: {INSTANCE_ID}): "
            f"верифіковано SampleTube {barcode}, призначені тести {order['biomarkers']}]"
        ),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
