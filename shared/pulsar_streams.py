"""Pulsar client wrapper — no domain or UI dependencies."""
import json

import pulsar

from shared.schema import PULSAR_URL


def make_client() -> pulsar.Client:
    return pulsar.Client(
        PULSAR_URL,
        logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error),
    )


def make_producer(client: pulsar.Client, topic: str) -> pulsar.Producer:
    return client.create_producer(topic, send_timeout_millis=0)


def publish_batch(producer: pulsar.Producer, records: list[dict]) -> None:
    for r in records:
        producer.send(json.dumps(r).encode())
    producer.flush()


def make_reader(client: pulsar.Client, topic: str, start=None) -> pulsar.Reader:
    if start is None:
        start = pulsar.MessageId.latest
    return client.create_reader(topic, start_message_id=start)


def drain(reader: pulsar.Reader) -> list[dict]:
    """Read all currently available messages, returning them newest-first."""
    messages = []
    try:
        while True:
            msg = reader.read_next(timeout_millis=5)
            messages.append(json.loads(msg.data().decode()))
    except pulsar.exceptions.Timeout:
        pass
    messages.reverse()
    return messages
