# Test Containers Java Project

This is a Java project demonstrating customer management with PostgreSQL and containerized testing using **host network mode**.

**Note:** This implementation is part of the `hostmode` folder, which is specifically for when using podman inside environments like devpod, where host network mode is required for container connectivity.

Due to Testcontainers Java limitations with host network mode (see [issue #5151](https://github.com/testcontainers/testcontainers-java/issues/5151)), this implementation uses direct Docker/Podman commands instead of the Testcontainers library to start PostgreSQL containers in host mode.

The implementation automatically detects and uses either Docker or Podman if available on the system.

## Structure

- `src/main/java/com/example/`: Main application code
  - `customers/`: Customer-related classes
  - `db/`: Database connection utilities
- `src/test/java/com/example/`: Test classes using direct Docker commands for host mode PostgreSQL

## Running Tests

To run the tests with host mode PostgreSQL:

```bash
mvn test
```

**Note:** The tests use direct Docker or Podman commands to start PostgreSQL containers in host network mode. Make sure either Docker or Podman is accessible and you have permission to run container commands.