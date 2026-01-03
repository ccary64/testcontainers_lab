# Test Containers Project

This repository demonstrates containerized testing with Testcontainers in both Python and Java, showcasing different approaches to integration testing.

## Project Structure

- `hostmode/`: Host network mode implementations for podman environments like devpod
  - `python/`: Python implementation using Testcontainers with PostgreSQL in host mode
  - `java/`: Java implementation using direct Docker/Podman commands for host mode PostgreSQL (Testcontainers workaround)
  - `README.md`: Detailed documentation for hostmode implementations

## Overview

The project implements a simple customer management system with database integration, demonstrating different approaches to containerized testing:

- **Python Version**: Uses Testcontainers with PostgreSQL in host network mode
- **Java Version**: Uses direct Docker or Podman commands for PostgreSQL in host mode (Testcontainers workaround)

## Testcontainers Host Mode Support

This project highlights an important difference in Testcontainers implementations:

### Python Testcontainers ✅
- **Works perfectly** with `network_mode="host"`
- Properly handles host networking for podman/devpod environments
- No port mapping conflicts in host mode

### Java Testcontainers ❌
- **Has known issues** with host network mode ([GitHub issue #5151](https://github.com/testcontainers/testcontainers-java/issues/5151))
- Testcontainers maintainers do not officially support host mode
- Fails to detect Docker environment when using host networking
- **Workaround implemented**: Uses direct Docker commands instead

### Why Host Mode Matters
Host network mode is essential for:
- **Podman environments** (like devpod) where containers need direct host access
- **CI/CD pipelines** with nested containerization
- **Development environments** requiring fixed port access

The Python implementation demonstrates proper Testcontainers host mode usage, while the Java version shows a practical workaround for environments where Testcontainers host mode doesn't work.

## Getting Started

Each implementation has its own README with specific setup and running instructions:

- [Host Mode Python](hostmode/python/README.md)
- [Host Mode Java](hostmode/java/README.md)

## Technologies Used

- Python with Testcontainers and pytest
- Java with Maven and H2
- PostgreSQL for database operations
- Docker/Podman for containerization