import pytest
import time

from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import (
    JSONSerializer,
    JSONDeserializer,
)
from confluent_kafka.schema_registry import SerializationContext, MessageField
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

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
    .with_env("SCHEMA_REGISTRY_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
    .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "localhost:9092")
    .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    .with_env("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", "schema-registry-group")
    .with_kwargs(network_mode="host")
)


@pytest.fixture(scope="module", autouse=True)
def setup_kafka(request):
    kafka_container.start()
    wait_for_logs(kafka_container, "Kafka Server started")
    schema_registry.start()
    wait_for_logs(schema_registry, "Server started, listening for requests")

    def remove_container():
        kafka_container.stop()
        schema_registry.stop()

    request.addfinalizer(remove_container)


def test_kafka_produce_and_consume():
    topic = "test-topic"
    bootstrap_server = "localhost:9092"
    schema_registry_url = "http://localhost:8081"

    # Topic will be auto created

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "TestMessage",
      "type": "object",
      "properties": {
        "message": {
          "type": "string"
        }
      },
      "required": ["message"]
    }
    """

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    # Producer
    producer_conf = {"bootstrap.servers": bootstrap_server}
    producer = Producer(producer_conf)
    json_serializer = JSONSerializer(
        schema_registry_client=schema_registry_client, schema_str=schema_str
    )
    message = {"message": "Hello, Kafka with KRaft and Schema Registry!"}

    # Produce the message
    print("Producing message...")
    producer.produce(
        topic,
        value=json_serializer(message, SerializationContext(topic, MessageField.VALUE)),
    )
    producer.flush()
    print("Message produced")

    # Consumer
    consumer_conf = {
        "bootstrap.servers": bootstrap_server,
        "group.id": f"test-group-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    json_deserializer = JSONDeserializer(
        schema_registry_client=schema_registry_client, schema_str=schema_str
    )

    received_messages = []
    timeout = 10  # seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        print("Polling for message...")
        msg = consumer.poll(1.0)
        if msg is None:
            print("No message received")
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        print("Message received, deserializing...")
        deserialized_value = json_deserializer(
            msg.value(), SerializationContext(topic, MessageField.VALUE)
        )
        received_messages.append(deserialized_value)
        print(f"Deserialized: {deserialized_value}")
        break  # Only need one message

    consumer.close()

    assert len(received_messages) == 1
    assert (
        received_messages[0]["message"]
        == "Hello, Kafka with KRaft and Schema Registry!"
    )
