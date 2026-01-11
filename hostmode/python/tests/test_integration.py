import os

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from customers import customers
from kafka import kafka as kafka_utils

EXPECTED_CUSTOMERS = 2
EXPECTED_EVENTS = 2

postgres = PostgresContainer("postgres:16-alpine")
postgres.ports = {}
postgres.with_kwargs(network_mode="host")

kafka_container = (
    DockerContainer("confluentinc/cp-kafka:7.6.0")
    .with_env("KAFKA_NODE_ID", "1")
    .with_env(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    )
    .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093")
    .with_env("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092")
    .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
    .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
    .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
    .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
    .with_env("CLUSTER_ID", "4L6_R_m3S0qK4f3Y-1Dshg")
    .with_kwargs(network_mode="host")
)

schema_registry = (
    DockerContainer("confluentinc/cp-schema-registry:7.6.0")
    .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
    .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "localhost:9092")
    .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    .with_env("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", "schema-registry-group")
    .with_kwargs(network_mode="host")
)


@pytest.fixture(scope="module", autouse=True)
def setup_integration():
    import time
    from db.connection import get_connection

    postgres.start()
    # Wait for postgres to be ready
    for _ in range(30):
        try:
            with get_connection() as conn:
                conn.cursor().execute("SELECT 1")
            break
        except:
            time.sleep(1)
    else:
        pytest.skip("PostgreSQL not ready")

    kafka_container.start()
    wait_for_logs(kafka_container, "Kafka Server started")
    schema_registry.start()
    wait_for_logs(schema_registry, "Server started, listening for requests")
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5432"
    os.environ["DB_USERNAME"] = postgres.username
    os.environ["DB_PASSWORD"] = postgres.password
    os.environ["DB_NAME"] = postgres.dbname
    customers.create_table()

    yield
    postgres.stop()
    kafka_container.stop()
    schema_registry.stop()


@pytest.fixture(autouse=True)
def setup_data():
    customers.delete_all_customers()


def test_customer_creation_with_kafka_event():
    topic = "customer-events-1"
    customer_name = "Jane Doe"
    customer_email = "jane.doe@example.com"
    bootstrap_server = "localhost:9092"
    schema_registry_url = "http://localhost:8081"

    # JSON Schema for customer events
    customer_event_schema_str = kafka_utils.CUSTOMER_EVENT_SCHEMA

    # Create schema registry client
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    # Create JSON serializer
    json_serializer = JSONSerializer(customer_event_schema_str, schema_registry_client)

    # Create customer in database
    customers.create_customer(customer_name, customer_email)

    # Verify customer was created
    created_customer = customers.get_customer_by_email(customer_email)
    assert created_customer is not None
    assert created_customer.name == customer_name
    assert created_customer.email == customer_email

    # Send event to Kafka using JSON Schema
    event_data = {
        "eventType": "CUSTOMER_CREATED",
        "customerId": created_customer.id,
        "customerName": customer_name,
        "customerEmail": customer_email,
        "timestamp": 1234567890,  # Mock timestamp for testing
    }

    # Producer with JSON serialization
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_server,
        },
    )

    # Serialize the message
    serialized_value = json_serializer(
        event_data,
        SerializationContext(topic, MessageField.VALUE),
    )

    # Produce the message
    producer.produce(
        topic=topic,
        value=serialized_value,
        key=str(created_customer.id).encode("utf-8"),
    )
    producer.flush()


def test_multiple_customers_with_events():
    topic = "customer-events-2"
    bootstrap_server = "localhost:9092"
    schema_registry_url = "http://localhost:8081"

    # JSON Schema for customer events
    customer_event_schema_str = kafka_utils.CUSTOMER_EVENT_SCHEMA

    # Create schema registry client
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    # Create JSON serializer (no deserializer needed for JSON)
    json_serializer = JSONSerializer(customer_event_schema_str, schema_registry_client)
    json_deserializer = JSONDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=customer_event_schema_str,
    )

    # Create multiple customers
    customers.create_customer("Charlie", "charlie@example.com")
    customers.create_customer("Diana", "diana@example.com")

    # Verify customers in database
    all_customers = customers.get_all_customers()
    assert len(all_customers) == EXPECTED_CUSTOMERS

    # Send events for each customer using JSON Schema
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_server,
        },
    )

    for customer in all_customers:
        event_data = {
            "eventType": "CUSTOMER_REGISTERED",
            "customerId": customer.id,
            "customerName": customer.name,
            "customerEmail": customer.email,
            "timestamp": 1234567890,
        }
        # Serialize the message
        serialized_value = json_serializer(
            event_data,
            SerializationContext(topic, MessageField.VALUE),
        )
        producer.produce(
            topic=topic,
            value=serialized_value,
            key=str(customer.id).encode("utf-8"),
        )

    producer.flush()

    # Consume and verify all events using JSON Consumer
    consumer_conf = {
        "bootstrap.servers": bootstrap_server,
        "group.id": "test-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)

    consumer.subscribe([topic])

    messages = []
    try:
        while len(messages) < EXPECTED_EVENTS:
            msg = consumer.poll(10.0)
            if msg is None:
                break
            if msg.error():
                continue
            deserialized_value = json_deserializer(
                msg.value(),
                SerializationContext(topic, MessageField.VALUE),
            )
            messages.append(deserialized_value)
    finally:
        consumer.close()

    assert len(messages) == EXPECTED_EVENTS

    # Verify each event
    for event in messages:
        assert event["eventType"] == "CUSTOMER_REGISTERED"
        assert "customerId" in event
        assert "customerName" in event
        assert "customerEmail" in event
        assert "timestamp" in event
