# Kafka test utilities for Testcontainers host mode
import time
from typing import Any

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# JSON Schema for customer events
CUSTOMER_EVENT_SCHEMA = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "eventType": {"type": "string"},
        "customerId": {"type": "integer"},
        "customerName": {"type": "string"},
        "customerEmail": {"type": "string"},
        "timestamp": {"type": "integer"}
    },
    "required": ["eventType", "customerId", "customerName", "customerEmail", "timestamp"]
}
"""


def create_json_serializer(schema_registry_url: str, schema_str: str) -> JSONSerializer:
    """Create a JSON serializer for Kafka messages."""
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    return JSONSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )


def create_json_deserializer(
    schema_registry_url: str,
    schema_str: str,
) -> JSONDeserializer:
    """Create a JSON deserializer for Kafka messages."""
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    return JSONDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )


def produce_message(
    topic: str,
    message: Any,
    bootstrap_servers: str = "localhost:9092",
    schema_registry_url: str = "http://localhost:8081",
    schema_str: str | None = None,
) -> None:
    """Produce a message to Kafka topic."""
    producer_conf = {"bootstrap.servers": bootstrap_servers}
    producer = Producer(producer_conf)

    if schema_str:
        json_serializer = create_json_serializer(schema_registry_url, schema_str)
        serialized_value = json_serializer(
            message,
            SerializationContext(topic, MessageField.VALUE),
        )
    else:
        serialized_value = message

    producer.produce(topic=topic, value=serialized_value)
    producer.flush()


def consume_messages(  # noqa: PLR0913
    topic: str,
    num_messages: int = 1,
    bootstrap_servers: str = "localhost:9092",
    schema_registry_url: str = "http://localhost:8081",
    schema_str: str | None = None,
    group_id: str | None = None,
) -> list[Any]:
    """Consume messages from Kafka topic."""
    if group_id is None:
        group_id = f"test-group-{int(time.time())}"

    consumer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    json_deserializer = None
    if schema_str:
        json_deserializer = create_json_deserializer(schema_registry_url, schema_str)

    messages = []
    timeout = 10  # seconds
    start_time = time.time()
    while time.time() - start_time < timeout and len(messages) < num_messages:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        if json_deserializer:
            deserialized_value = json_deserializer(
                msg.value(),
                SerializationContext(topic, MessageField.VALUE),
            )
        else:
            deserialized_value = msg.value()
        messages.append(deserialized_value)

    consumer.close()
    return messages


def produce_customer_event(
    topic: str,
    event_data: dict[str, Any],
    bootstrap_servers: str = "localhost:9092",
    schema_registry_url: str = "http://localhost:8081",
    schema_str: str | None = None,
) -> None:
    """Produce a customer event message to Kafka."""
    produce_message(
        topic,
        event_data,
        bootstrap_servers,
        schema_registry_url,
        schema_str,
    )


def consume_customer_events(  # noqa: PLR0913
    topic: str,
    num_events: int = 1,
    bootstrap_servers: str = "localhost:9092",
    schema_registry_url: str = "http://localhost:8081",
    schema_str: str | None = None,
    group_id: str | None = None,
) -> list[dict[str, Any]]:
    """Consume customer event messages from Kafka."""
    return consume_messages(
        topic,
        num_events,
        bootstrap_servers,
        schema_registry_url,
        schema_str,
        group_id,
    )
