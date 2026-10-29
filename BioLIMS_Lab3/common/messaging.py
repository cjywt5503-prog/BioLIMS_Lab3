"""RabbitMQ, Outbox Relay та повторне підключення споживачів."""
import logging
import os
import time
from uuid import uuid4

import pika

from common.storage import database, now_iso

logger = logging.getLogger(__name__)
# Нові імена відокремлюють контракт v2 від черг попередньої реалізації.
ASSAY_QUEUE = 'assay_completed_queue_v2'
DLQ_QUEUE = 'assay_dlq_queue_v2'
SUCCESS_QUEUE = 'assay_validated_queue_v2'
DLX = 'assay_dlx_v2'


def connection_parameters():
    return pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST', 'rabbitmq'),
        credentials=pika.PlainCredentials(
            os.getenv('RABBITMQ_USER', 'guest'), os.getenv('RABBITMQ_PASSWORD', 'guest')
        ),
        heartbeat=30, socket_timeout=5, stack_timeout=10,
        blocked_connection_timeout=10,
    )


def setup_topology(channel):
    channel.exchange_declare(exchange=DLX, exchange_type='direct', durable=True)
    channel.queue_declare(queue=DLQ_QUEUE, durable=True, arguments={'x-queue-type': 'quorum'})
    channel.queue_bind(exchange=DLX, queue=DLQ_QUEUE, routing_key='assay_failed')
    channel.queue_declare(queue=SUCCESS_QUEUE, durable=True, arguments={'x-queue-type': 'quorum'})
    channel.queue_declare(queue=ASSAY_QUEUE, durable=True, arguments={
        'x-queue-type': 'quorum',
        'x-dead-letter-exchange': DLX,
        'x-dead-letter-routing-key': 'assay_failed',
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
    })


def close_connection(connection):
    if connection is not None and connection.is_open:
        try:
            connection.close()
        except (pika.exceptions.AMQPError, OSError):
            logger.warning('RabbitMQ connection was already lost')


def publish_confirmed(target_queue, body, message_id):
    connection = None
    try:
        connection = pika.BlockingConnection(connection_parameters())
        channel = connection.channel()
        setup_topology(channel)
        channel.confirm_delivery()
        # У confirm mode Pika кидає виняток при NACK або неможливості маршрутизації.
        channel.basic_publish(
            exchange='', routing_key=target_queue, body=body.encode('utf-8'), mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode=2, content_type='application/json', message_id=message_id,
            ),
        )
    finally:
        close_connection(connection)


def relay_once(db_path, publisher=None):
    """Захопити одну подію, опублікувати й позначити лише після підтвердження."""
    publisher = publisher or publish_confirmed
    token = str(uuid4())
    now = time.time()
    with database(db_path, write=True) as db:
        row = db.execute('''SELECT * FROM outbox_messages
            WHERE status='PENDING' AND lease_until<=? ORDER BY created_at, message_id LIMIT 1''',
                         (now,)).fetchone()
        if row is None:
            return False
        row = dict(row)
        db.execute('''UPDATE outbox_messages SET claim_token=?, lease_until=?, attempts=attempts+1
            WHERE message_id=?''', (token, now + 90, row['message_id']))
    # Мережевий виклик поза транзакцією SQLite. Прострочена lease дозволяє
    # іншій репліці повторити спробу після аварії; дублікати відсіює Inbox.
    try:
        publisher(row['target_queue'], row['payload_json'], row['message_id'])
    except Exception as exc:
        with database(db_path, write=True) as db:
            db.execute('''UPDATE outbox_messages SET claim_token=NULL, lease_until=0, last_error=?
                WHERE message_id=? AND claim_token=? AND status='PENDING' ''',
                       (str(exc)[:500], row['message_id'], token))
        raise
    with database(db_path, write=True) as db:
        # Компенсація могла виконатися раніше: термінальний статус не перезаписуємо.
        db.execute('''UPDATE outbox_messages SET status='SENT_TO_BROKER', published_at=?,
            claim_token=NULL, lease_until=0, last_error=NULL
            WHERE message_id=? AND claim_token=? AND status='PENDING' ''',
                   (now_iso(), row['message_id'], token))
    return True


def relay_forever(db_path, stopped):
    while not stopped.is_set():
        try:
            worked = relay_once(db_path)
        except Exception:
            logger.exception('Outbox delivery failed; the message remains pending')
            worked = False
        if not worked:
            stopped.wait(2)


def consume_forever(queue, callback, stopped):
    while not stopped.is_set():
        connection = None
        try:
            connection = pika.BlockingConnection(connection_parameters())
            channel = connection.channel()
            setup_topology(channel)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
            while not stopped.is_set():
                connection.process_data_events(time_limit=1)
        except Exception:
            # Якщо callback не завершив COMMIT/ACK, закриття з'єднання повертає
            # непідтверджене повідомлення в чергу. Нічого не позначається успішним.
            logger.exception('Consumer failed; reconnecting to %s', queue)
        finally:
            close_connection(connection)
        stopped.wait(2)
