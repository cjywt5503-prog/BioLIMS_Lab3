import os
import socket
import threading
import time
import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Order and Sample Service (Intake Context)")

INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "order-sample-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8001))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")

# Локальне сховище (імітація Database-per-Service для Intake Context)
ORDERS_DB = {
    "CODE128-1001": {
        "order_id": "ORD-001",
        "patient_id": "PAT-409",
        "barcode": "CODE128-1001",
        "biomarkers": ["GLU", "ALT", "CREA"],
        "status": "SAMPLE_COLLECTED"
    }
}

class CreateOrderRequest(BaseModel):
    patient_id: str
    barcode: str
    biomarkers: List[str]

def register_in_consul():
    """Реєстрація інстанса мікросервісу в Service Discovery (Consul)"""
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
        except Exception as e:
            print(f"[{INSTANCE_ID}] Очікування Consul... ({e})")
            time.sleep(3)

@app.on_event("startup")
def startup_event():
    threading.Thread(target=register_in_consul, daemon=True).start()

@app.get("/orders")
def list_orders():
    """Отримати список усіх замовлень (демонстрація балансування навантаження)"""
    return {
        "service": SERVICE_NAME,
        "handled_by_instance": INSTANCE_ID,
        "orders_count": len(ORDERS_DB),
        "orders": list(ORDERS_DB.values())
    }

@app.post("/orders")
def create_order(req: CreateOrderRequest):
    """Створення нового замовлення LabOrder та маркування пробірки SampleTube"""
    order_data = {
        "order_id": f"ORD-{len(ORDERS_DB) + 1:03d}",
        "patient_id": req.patient_id,
        "barcode": req.barcode,
        "biomarkers": req.biomarkers,
        "status": "SAMPLE_COLLECTED"
    }
    ORDERS_DB[req.barcode] = order_data
    return {
        "message": "Замовлення створено та штрихкод нанесено на пробірку",
        "handled_by_instance": INSTANCE_ID,
        "order": order_data
    }

@app.get("/orders/by-barcode/{barcode}")
def get_order_by_barcode(barcode: str, client_note: str = "Init"):
    """
    Open Host Service (OHS) для синхронного виклику з Assay Execution Service.
    Дописує інформацію від поточного інстанса до вхідного рядка трасування.
    """
    order = ORDERS_DB.get(barcode)
    if not order:
        # Якщо штрихкод не знайдено (наприклад, при реплікації без спільної БД), створюємо демо-запис
        order = {
            "order_id": "ORD-AUTO",
            "patient_id": "PAT-DEMO",
            "barcode": barcode,
            "biomarkers": ["GLU", "ALT", "CREA"],
            "status": "SAMPLE_COLLECTED"
        }
    
    enriched_trace = (
        f"{client_note} -> "
        f"[1. OrderService (instance: {INSTANCE_ID}): верифіковано SampleTube {barcode}, "
        f"призначені тести {order['biomarkers']}]"
    )
    return {
        "order": order,
        "handled_by_order_instance": INSTANCE_ID,
        "trace_chain": enriched_trace
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)