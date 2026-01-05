# Test Containers Python Project

This is a Python project demonstrating customer management with PostgreSQL and Kafka integration using Testcontainers for integration testing, specifically using **host network mode**.

**Note:** This implementation successfully uses Testcontainers Python with host network mode, which works reliably for podman/devpod environments. This contrasts with Testcontainers Java, which has known issues with host mode.

## Structure

- `customers/`: Customer-related classes and functions
- `db/`: Database connection utilities
- `tests/`: Test files using Testcontainers with PostgreSQL and Kafka

## Test Files

- `test_customers.py`: Tests PostgreSQL database operations
- `test_kafka.py`: Tests Kafka messaging with KRaft mode
- `test_integration.py`: Integration tests combining PostgreSQL and Kafka

## Running Tests

To run the tests with Testcontainers:

```bash
uv run pytest
```

**Note:** The tests require Docker to be running and use host network mode to connect to PostgreSQL and Kafka containers. Make sure Docker is accessible and configured for host networking. This implementation demonstrates proper Testcontainers host mode usage.