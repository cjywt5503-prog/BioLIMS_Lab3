"""Real FastAPI/SQLite component tests; HTTP discovery and AMQP transport are simulated."""
import importlib.util
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

import pika
import requests
import yaml
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common import messaging
from common.contracts import AssayRunRequest
from common.storage import database


def load(folder, db_path):
    name = 'test_service_' + uuid4().hex
    spec = importlib.util.spec_from_file_location(name, ROOT / folder / 'app.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    module.DB_PATH = str(db_path)
    module.init_db()
    return module


class SagaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.orders = [load('order-sample-service', base/'orders.sqlite3') for _ in range(2)]
        for i, order in enumerate(self.orders, start=1):
            order.INSTANCE_ID = f'order-{i}'
        self.producers = [load('assay-execution-service', base/'assay.sqlite3') for _ in range(2)]
        self.consumers = [load('validation-report-service', base/'reports.sqlite3') for _ in range(2)]
        self.p, self.c = self.producers[0], self.consumers[0]
        self.order_clients = [TestClient(order.app) for order in self.orders]
        self.api = TestClient(self.p.app)
        for client in [*self.order_clients, self.api]:
            self.addCleanup(client.close)
        self.method = SimpleNamespace(delivery_tag=10)

    def route_http(self, url, params=None, **kwargs):
        if '/v1/health/service/' in url:
            self.assertEqual(params, {'passing': 'true'})
            result = Mock(status_code=200)
            result.json.return_value = [{'Service': {'Address': f'node-{i}', 'Port': 8001}} for i in (2, 1)]
            return result
        for i, client in enumerate(self.order_clients, start=1):
            prefix = f'http://node-{i}:8001'
            if url.startswith(prefix):
                return client.get(url[len(prefix):], params=params)
        self.fail(f'Service discovery was bypassed: {url}')

    def create(self, barcode='CODE128-1001', **extra):
        with patch.object(self.p.requests, 'get', side_effect=self.route_http):
            result = self.p.run_assay(AssayRunRequest(barcode=barcode, **extra))
        with database(self.p.DB_PATH) as db:
            row = db.execute('SELECT payload_json FROM outbox_messages WHERE message_id=?',
                             (result['outbox_message_id'],)).fetchone()
        return result, row['payload_json'].encode()

    def deliver(self, body, consumer=None, channel=None):
        consumer = consumer or self.c
        channel = channel or Mock()
        consumer.on_message(channel, self.method, None, body)
        return channel

    def count(self, path, table):
        with database(path) as db:
            return db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]

    def original_outbox(self, result):
        with database(self.p.DB_PATH) as db:
            return dict(db.execute('SELECT * FROM outbox_messages WHERE message_id=?',
                                   (result['outbox_message_id'],)).fetchone())

    def finish_success(self, producer=None):
        producer = producer or self.p
        sent = []
        messaging.relay_once(self.c.DB_PATH, lambda queue, body, message_id: sent.append((queue, body)))
        self.assertEqual(sent[0][0], messaging.SUCCESS_QUEUE)
        channel = Mock()
        producer.on_success(channel, self.method, None, sent[0][1].encode())
        channel.basic_ack.assert_called_once_with(delivery_tag=10)

    def test_end_to_end_success_and_final_state(self):
        result, body = self.create()
        published = []
        messaging.relay_once(self.p.DB_PATH, lambda queue, payload, msg_id: published.append(payload))
        self.deliver(published[0].encode()).basic_ack.assert_called_once()
        self.finish_success(self.producers[1])
        self.assertEqual(self.p.get_assay(result['assay_id'])['saga_status'], 'COMPLETED_SUCCESSFULLY')
        report = self.consumers[1].get_report('CODE128-1001')
        self.assertEqual(report['assay_id'], result['assay_id'])
        self.assertEqual(report['digital_signature'], 'NOT_SIGNED')
        self.assertEqual(report['delta_check'], 'NOT_IMPLEMENTED')
        self.assertEqual(self.original_outbox(result)['status'], 'SENT_TO_BROKER')

    def test_error_rolls_back_report_and_compensates_on_another_replica(self):
        result, body = self.create('CODE128-ERROR')
        channel = self.deliver(body)
        channel.basic_reject.assert_called_once_with(delivery_tag=10, requeue=False)
        self.assertEqual(self.count(self.c.DB_PATH, 'reports'), 0)
        self.assertEqual(self.count(self.c.DB_PATH, 'outbox_messages'), 0)
        with database(self.c.DB_PATH) as db:
            self.assertEqual(db.execute('SELECT status FROM inbox_messages').fetchone()[0], 'REJECTED')
        dlq_channel = Mock()
        self.producers[1].on_compensation(dlq_channel, self.method, None, body)
        dlq_channel.basic_ack.assert_called_once()
        self.assertEqual(self.p.get_assay(result['assay_id'])['saga_status'], 'ROLLED_BACK_BY_SAGA_DLQ')
        self.assertEqual(self.original_outbox(result)['status'], 'COMPENSATED_FROM_DLQ')

    def test_producer_outbox_write_failure_rolls_back_business_data(self):
        with database(self.p.DB_PATH, write=True) as db:
            db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON outbox_messages BEGIN SELECT RAISE(ABORT, 'forced'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.create()
        self.assertEqual(self.count(self.p.DB_PATH, 'assays_data'), 0)
        self.assertEqual(self.count(self.p.DB_PATH, 'outbox_messages'), 0)

    def test_consumer_outbox_failure_rolls_back_report_and_inbox_then_retries(self):
        _, body = self.create()
        with database(self.c.DB_PATH, write=True) as db:
            db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON outbox_messages BEGIN SELECT RAISE(ABORT, 'forced'); END")
        channel = Mock()
        with self.assertRaises(sqlite3.IntegrityError):
            self.deliver(body, channel=channel)
        channel.basic_ack.assert_not_called()
        channel.basic_reject.assert_not_called()
        for table in ('reports', 'inbox_messages', 'outbox_messages'):
            self.assertEqual(self.count(self.c.DB_PATH, table), 0)
        with database(self.c.DB_PATH, write=True) as db:
            db.execute('DROP TRIGGER fail_insert')
        self.deliver(body).basic_ack.assert_called_once()
        for table in ('reports', 'inbox_messages', 'outbox_messages'):
            self.assertEqual(self.count(self.c.DB_PATH, table), 1)

    def test_restart_and_redelivery_preserve_report(self):
        result, body = self.create()
        self.deliver(body)
        restarted = load('validation-report-service', self.c.DB_PATH)
        self.deliver(body, consumer=restarted).basic_ack.assert_called_once()
        self.assertEqual(restarted.get_report('CODE128-1001')['assay_id'], result['assay_id'])
        for table in ('reports', 'inbox_messages', 'outbox_messages'):
            self.assertEqual(self.count(self.c.DB_PATH, table), 1)

    def test_concurrent_duplicate_consumption_is_idempotent(self):
        _, body = self.create()
        with ThreadPoolExecutor(max_workers=6) as pool:
            channels = list(pool.map(lambda i: self.deliver(body, consumer=self.consumers[i % 2]), range(12)))
        for channel in channels:
            channel.basic_ack.assert_called_once()
        for table in ('reports', 'inbox_messages', 'outbox_messages'):
            self.assertEqual(self.count(self.c.DB_PATH, table), 1)

    def test_ack_observes_all_committed_consumer_records(self):
        result, body = self.create()
        channel = Mock()
        def check_commit(**kwargs):
            for table in ('reports', 'inbox_messages', 'outbox_messages'):
                self.assertEqual(self.count(self.c.DB_PATH, table), 1)
            self.assertEqual(self.consumers[1].get_report('CODE128-1001')['assay_id'], result['assay_id'])
        channel.basic_ack.side_effect = check_commit
        self.deliver(body, channel=channel)

    def test_unconfirmed_publish_stays_pending_and_can_retry(self):
        result, _ = self.create()
        for failure in (pika.exceptions.AMQPConnectionError(), pika.exceptions.NackError([]), pika.exceptions.UnroutableError([])):
            with self.subTest(failure=type(failure).__name__):
                with self.assertRaises(type(failure)):
                    messaging.relay_once(self.p.DB_PATH, Mock(side_effect=failure))
                state = self.original_outbox(result)
                self.assertEqual(state['status'], 'PENDING')
                self.assertIsNone(state['claim_token'])
        messaging.relay_once(self.p.DB_PATH, Mock())
        self.assertEqual(self.original_outbox(result)['status'], 'SENT_TO_BROKER')

    def test_publisher_confirms_and_mandatory_routing(self):
        channel = Mock()
        connection = Mock(is_open=True)
        connection.channel.return_value = channel
        with patch.object(messaging.pika, 'BlockingConnection', return_value=connection):
            messaging.publish_confirmed(messaging.ASSAY_QUEUE, '{}', 'message-id')
        methods = [call[0] for call in channel.method_calls]
        self.assertLess(methods.index('confirm_delivery'), methods.index('basic_publish'))
        self.assertTrue(channel.basic_publish.call_args.kwargs['mandatory'])
        self.assertEqual(channel.basic_publish.call_args.kwargs['properties'].delivery_mode, 2)
        connection.close.assert_called_once()

    def test_compensation_cannot_be_overwritten_by_relay(self):
        result, body = self.create('CODE128-ERROR')
        def publish_then_reject(queue, payload, message_id):
            self.deliver(payload.encode()).basic_reject.assert_called_once()
            self.producers[1].on_compensation(Mock(), self.method, None, payload.encode())
        messaging.relay_once(self.p.DB_PATH, publish_then_reject)
        self.assertEqual(self.original_outbox(result)['status'], 'COMPENSATED_FROM_DLQ')
        self.assertEqual(self.p.get_assay(result['assay_id'])['saga_status'], 'ROLLED_BACK_BY_SAGA_DLQ')

    def test_duplicate_compensation_does_not_repeat_business_change(self):
        result, body = self.create('CODE128-ERROR')
        self.p.on_compensation(Mock(), self.method, None, body)
        first = self.p.get_assay(result['assay_id'])
        self.producers[1].on_compensation(Mock(), self.method, None, body)
        self.assertEqual(first, self.p.get_assay(result['assay_id']))
        self.assertEqual(self.count(self.p.DB_PATH, 'inbox_messages'), 1)

    def test_outbox_claim_is_exclusive_across_replicas(self):
        self.create()
        entered, release = threading.Event(), threading.Event()
        def publisher(*args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('Test synchronization timed out')
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(messaging.relay_once, self.p.DB_PATH, publisher)
            self.assertTrue(entered.wait(5))
            try:
                self.assertFalse(messaging.relay_once(self.producers[1].DB_PATH, Mock()))
            finally:
                release.set()
            self.assertTrue(first.result(timeout=5))

    def test_expired_claim_is_recovered_after_relay_crash(self):
        result, _ = self.create()
        with database(self.p.DB_PATH, write=True) as db:
            db.execute('UPDATE outbox_messages SET claim_token=?, lease_until=?', ('dead-process', time.time()-1))
        self.assertTrue(messaging.relay_once(self.producers[1].DB_PATH, Mock()))
        self.assertEqual(self.original_outbox(result)['status'], 'SENT_TO_BROKER')

    def test_success_notification_failure_is_retryable(self):
        result, body = self.create()
        self.deliver(body)
        with self.assertRaises(OSError):
            messaging.relay_once(self.c.DB_PATH, Mock(side_effect=OSError('broker down')))
        self.assertEqual(self.p.get_assay(result['assay_id'])['saga_status'], 'PENDING_VALIDATION')
        self.finish_success()
        self.assertEqual(self.p.get_assay(result['assay_id'])['saga_status'], 'COMPLETED_SUCCESSFULLY')

    def test_unknown_assay_in_dlq_is_not_acknowledged(self):
        _, body = self.create('CODE128-ERROR')
        event = json.loads(body)
        event['assay_id'] = 'unknown-assay'
        channel = Mock()
        with self.assertRaises(RuntimeError):
            self.p.on_compensation(channel, self.method, None, json.dumps(event))
        channel.basic_ack.assert_not_called()
        channel.basic_reject.assert_not_called()

    def test_consul_round_robin_and_only_assigned_biomarkers(self):
        created = self.order_clients[0].post('/orders', json={'barcode': 'NEW-GLU', 'patient_id': 'PAT', 'biomarkers': ['GLU']})
        self.assertEqual(created.status_code, 201)
        result = self.order_clients[1].get('/orders/by-barcode/NEW-GLU').json()
        self.assertEqual(result['order'], created.json()['order'])
        with patch.object(self.p.requests, 'get', side_effect=self.route_http):
            runs = [self.api.post('/assays/run', json={'barcode': 'NEW-GLU'}).json() for _ in range(4)]
        self.assertEqual([r['handled_by_order_instance'] for r in runs], ['order-1', 'order-2'] * 2)
        self.assertTrue(all([x['biomarker'] for x in r['measurements']] == ['GLU'] for r in runs))

    def test_unknown_orders_and_upstream_errors_are_handled(self):
        with patch.object(self.p.requests, 'get', side_effect=self.route_http):
            self.assertEqual(self.api.post('/assays/run', json={'barcode': 'MISSING'}).status_code, 404)
        with patch.object(self.p.requests, 'get', side_effect=requests.ConnectionError('offline')):
            self.assertEqual(self.api.post('/assays/run', json={}).status_code, 503)
        bad_response = Mock(status_code=200)
        bad_response.json.return_value = {'unexpected': True}
        with patch.object(self.p.discovery, 'candidates', return_value=['node:8001']), patch.object(self.p.requests, 'get', return_value=bad_response):
            self.assertEqual(self.api.post('/assays/run', json={}).status_code, 502)
        self.assertEqual(self.count(self.p.DB_PATH, 'assays_data'), 0)

    def test_duplicate_and_invalid_orders_are_rejected(self):
        body = {'patient_id': 'P', 'barcode': 'NEW', 'biomarkers': ['GLU']}
        self.assertEqual(self.order_clients[0].post('/orders', json=body).status_code, 201)
        self.assertEqual(self.order_clients[1].post('/orders', json=body).status_code, 409)
        for invalid in ([], ['BAD'], ['GLU', 'GLU']):
            self.assertEqual(self.order_clients[0].post('/orders', json={**body, 'biomarkers': invalid}).status_code, 422)

    def test_compose_persists_separate_service_databases(self):
        compose = yaml.safe_load((ROOT/'docker-compose.yml').read_text())
        volumes = []
        for service in ('order-sample-service', 'assay-execution-service', 'validation-report-service'):
            config = compose['services'][service]
            self.assertNotIn('container_name', config)
            self.assertTrue((ROOT/config['build']['dockerfile']).is_file())
            self.assertEqual(config['build']['context'], '.')
            volumes.append(config['volumes'][0])
        self.assertEqual(len(set(volumes)), 3)
        self.assertTrue(all(v.endswith(':/data') for v in volumes))


if __name__ == '__main__':
    unittest.main()
