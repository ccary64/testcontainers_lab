# Testcontainers Host Mode Demo - AI Agent Instructions

## Project Overview
This is a demonstration project showing Testcontainers usage in Java and Python, with a focus on **host network mode** for podman/devpod environments. The project implements a customer management system with PostgreSQL database and Kafka messaging, including integration tests.

## Architecture
- **Java Implementation**: Uses direct Docker/Podman commands due to Testcontainers Java limitations with host mode ([issue #5151](https://github.com/testcontainers/testcontainers-java/issues/5151))
- **Python Implementation**: Uses Testcontainers library properly with `network_mode="host"`
- **Services**: PostgreSQL (port 5432), Kafka KRaft mode (port 9092), Schema Registry (port 8081)
- **Key Pattern**: Host networking eliminates port conflicts in nested container environments

## Critical Workflows

### Java Testing (`hostmode/java/`)
```bash
mvn test  # Runs integration tests with direct Docker commands
```
- Uses `ContainerHelper` class for container lifecycle management
- Automatically detects Docker vs Podman runtime
- Sets system properties for database connections

### Python Testing (`hostmode/python/`)
```bash
uv run pytest  # Runs Testcontainers-based integration tests
```
- Uses `testcontainers[postgres,kafka]` with host network mode
- Container setup: `postgres.with_kwargs(network_mode="host")`, `ports = {}`
- Environment variables configured in test fixtures
- Includes Schema Registry container for Avro serialization
- Uses `confluent-kafka[json]` for schema-aware producers/consumers

## Key Patterns & Conventions

### Container Configuration
- **Host Mode**: Always use `network_mode="host"` and disable port mapping (`ports = {}`)
- **Kafka KRaft**: Specific environment variables for controller/broker roles (see `test_integration.py` lines 12-22)
- **Schema Registry**: Confluent Schema Registry container with Kafka store configuration
- **Postgres**: Standard alpine image with test credentials

### Test Structure
- **Setup**: Start containers in `@BeforeAll` (Java) or module fixtures (Python)
- **Teardown**: Stop containers in `@AfterAll` (Java) or finalizers (Python)
- **Integration**: Combine database operations with Kafka messaging in single tests

### Database Connection
- Environment variables: `DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_PASSWORD`, `DB_NAME`
- Connection pooling via `psycopg` (Python) or JDBC (Java)
- Table creation happens in test setup

### Java Workarounds
- `ContainerHelper.java`: Utility for direct container management when Testcontainers fails
- Process execution for Docker commands instead of library calls
- Container ID tracking for cleanup

## Common Pitfalls
- Don't use Testcontainers Java for host mode - it doesn't work reliably
- Ensure Docker/Podman is accessible in test environments
- Wait sufficient time for Kafka startup (15+ seconds)
- Use localhost for connections in host mode, not container IPs

## File References
- `hostmode/python/tests/test_integration.py`: Exemplar Testcontainers host mode setup
- `hostmode/java/src/test/java/com/example/ContainerHelper.java`: Java container management workaround
- `hostmode/java/src/test/java/com/example/KafkaPostgresIntegrationTest.java`: Integration test pattern</content>
<parameter name="filePath">/home/ccary/workspace/test-containers/.github/copilot-instructions.md