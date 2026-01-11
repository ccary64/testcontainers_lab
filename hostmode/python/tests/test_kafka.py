import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from kafka import kafka as kafka_utils

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
def setup_kafka():
    kafka_container.start()
    wait_for_logs(kafka_container, "Kafka Server started")
    schema_registry.start()
    wait_for_logs(schema_registry, "Server started, listening for requests")
    yield
    kafka_container.stop()
    schema_registry.stop()


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

    message = {"message": "Hello, Kafka with KRaft and Schema Registry!"}

    # Produce the message
    kafka_utils.produce_message(
        topic,
        message,
        bootstrap_server,
        schema_registry_url,
        schema_str,
    )

    # Consume the message
    received_messages = kafka_utils.consume_messages(
        topic,
        1,
        bootstrap_server,
        schema_registry_url,
        schema_str,
    )

    assert len(received_messages) == 1
    assert (
        received_messages[0]["message"]
        == "Hello, Kafka with KRaft and Schema Registry!"
    )
