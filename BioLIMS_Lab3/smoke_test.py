"""Check both SAGA branches against a running Docker Compose stack (stdlib only)."""
import argparse
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode('utf-8')
    req = Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urlopen(req, timeout=20) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_for(call, predicate, description, timeout=90):
    deadline = time.monotonic() + timeout
    while True:
        try:
            status, result = call()
            if status == 200 and predicate(result):
                return result
        except (URLError, OSError, ValueError):
            pass
        require(time.monotonic() < deadline, 'Timed out: ' + description)
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gateway', default='http://localhost:8080')
    parser.add_argument('--consul', default='http://localhost:8500')
    args = parser.parse_args()
    api = args.gateway.rstrip('/') + '/api'
    wait_for(lambda: request(args.consul.rstrip('/') + '/v1/health/service/order-sample-service?passing=true'),
             lambda nodes: len(nodes) > 0, 'healthy OrderService in Consul')
    wait_for(lambda: request(api + '/orders'), lambda result: 'orders' in result, 'API Gateway')

    for failure in (False, True):
        barcode = ('LAB3-ERROR-' if failure else 'LAB3-OK-') + uuid4().hex[:12]
        order = {'patient_id': 'PAT-TEST', 'barcode': barcode, 'biomarkers': ['GLU']}
        status, created = request(api + '/orders', order)
        require(status == 201, f'Order creation failed: {status} {created}')
        require(request(api + '/orders', order)[0] == 409, 'Duplicate order accepted')
        status, run = request(api + '/assays/run', {'barcode': barcode, 'simulate_failure': failure})
        require(status == 200, f'Assay request failed: {status} {run}')
        expected = 'ROLLED_BACK_BY_SAGA_DLQ' if failure else 'COMPLETED_SUCCESSFULLY'
        state = wait_for(lambda: request(api + '/assays/' + run['assay_id']),
                         lambda state: state.get('saga_status') == expected, expected)
        status, report = request(api + '/reports/' + barcode)
        require(status == 200, 'Report endpoint failed')
        if failure:
            require(report.get('status') == 'REPORT_NOT_AVAILABLE', 'Failed report was not rolled back')
            status, outbox = request(api + '/assays/outbox')
            require(status == 200, 'Outbox endpoint failed')
            message = next(row for row in outbox['outbox_messages_table'] if row['message_id'] == run['outbox_message_id'])
            require(message['status'] == 'COMPENSATED_FROM_DLQ', 'Compensation status was overwritten')
        else:
            require(report.get('assay_id') == run['assay_id'], 'Report and assay IDs differ')
            require([item['biomarker'] for item in report['validated_results']] == ['GLU'], 'Unexpected tests')
            require(report['digital_signature'] == 'NOT_SIGNED', 'Demo falsely claims a signature')
        print(f"PASS: {barcode} -> {state['saga_status']}")
    missing = 'MISSING-' + uuid4().hex
    require(request(api + '/assays/run', {'barcode': missing})[0] == 404, 'Unknown order was accepted')
    print('PASS: Outbox -> RabbitMQ -> atomic report/Inbox -> success event or DLQ compensation')


if __name__ == '__main__':
    main()
