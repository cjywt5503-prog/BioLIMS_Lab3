import logging
import os
import socket
import threading

import requests
from fastapi import HTTPException


logger = logging.getLogger(__name__)


class ServiceRegistration:
    def __init__(self, name: str, port: int, instance_id: str):
        self.base_url = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
        self.service_id = f"{name}-{instance_id}"
        address = os.getenv("SERVICE_ADDRESS") or socket.gethostbyname(socket.gethostname())
        self.payload = {
            "ID": self.service_id, "Name": name, "Address": address, "Port": port,
            "Check": {
                "HTTP": f"http://{address}:{port}/health", "Interval": "5s",
                "Timeout": "2s", "DeregisterCriticalServiceAfter": "1m",
            },
        }
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._register, daemon=True)

    def _register(self):
        # Повторна реєстрація відновлює запис після перезапуску Consul.
        while not self.stopped.is_set():
            delay = 30
            try:
                response = requests.put(
                    f"{self.base_url}/v1/agent/service/register", json=self.payload, timeout=3,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.warning("Consul registration failed: %s", exc)
                delay = 3
            self.stopped.wait(delay)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=4)
        try:
            response = requests.put(
                f"{self.base_url}/v1/agent/service/deregister/{self.service_id}", timeout=3,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Consul deregistration failed: %s", exc)


class RoundRobinDiscovery:
    """Client-side Round Robin серед healthy-реплік; лічильник на один AssayService."""

    def __init__(self):
        self.base_url = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
        self.next_index = 0
        self.lock = threading.Lock()

    def candidates(self, service_name: str) -> list[str]:
        try:
            response = requests.get(
                f"{self.base_url}/v1/health/service/{service_name}",
                params={"passing": "true"}, timeout=3,
            )
            response.raise_for_status()
            nodes = sorted({
                f"{item['Service']['Address']}:{int(item['Service']['Port'])}"
                for item in response.json() if item["Service"]["Address"]
            })
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            raise HTTPException(503, "Consul недоступний або повернув некоректні дані") from exc
        if not nodes:
            raise HTTPException(503, "У Consul немає здорових реплік OrderService")
        with self.lock:
            index = self.next_index % len(nodes)
            self.next_index += 1
        return nodes[index:] + nodes[:index]
