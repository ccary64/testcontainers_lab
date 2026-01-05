import pytest
from kafka import KafkaProducer, KafkaConsumer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs



kafka_container = DockerContainer("confluentinc/cp-kafka:7.6.0") \
    .with_env("KAFKA_PROCESS_ROLES", "broker,controller") \
    .with_env("KAFKA_NODE_ID", "1") \
    .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093") \
    .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093") \
    .with_env("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092") \
    .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER") \
    .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT") \
    .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1") \
    .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") \
    .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") \
    .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true") \
    .with_env("CLUSTER_ID", "4L6_R_m3S0qK4f3Y-1Dshg") \
    .with_kwargs(network_mode="host")

@pytest.fixture(scope="module", autouse=True)
def setup_kafka(request):
    kafka_container.start()
    wait_for_logs(kafka_container, "Kafka Server started")

    def remove_container():
        kafka_container.stop()

    request.addfinalizer(remove_container)


def test_kafka_produce_and_consume():
    topic = "test-topic"
    message = "Hello, Kafka with KRaft!"
    bootstrap_server = "localhost:9092"


    # Producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: v.encode('utf-8')
    )
    producer.send(topic, message)
    producer.flush()  # Ensure message is sent
    producer.close()

    # Consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: v.decode('utf-8'),
        group_id='test-group'
    )

    # Poll for messages
    messages = []
    for i in range(10):  # Try up to 10 polls
        records = consumer.poll(timeout_ms=1000)
        for topic_partition, records_list in records.items():
            for record in records_list:
                messages.append(record.value)
        if messages:
            break

    consumer.close()

    assert len(messages) == 1
    assert messages[0] == "Hello, Kafka with KRaft!"