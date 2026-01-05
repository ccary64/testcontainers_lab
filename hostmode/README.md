# Test Containers Project

This project demonstrates containerized testing with Testcontainers in both Python and Java, specifically testing **host network mode** for container connectivity.

## Project Structure

- `python/`: Python implementation with Testcontainers
- `java/`: Java implementation with Testcontainers

## Overview

The project implements a simple customer management system and Kafka messaging examples that:
- Uses PostgreSQL database and Kafka with KRaft mode
- Demonstrates integration testing with containers
- Shows host network mode configuration for Testcontainers
- Includes integration tests combining both PostgreSQL and Kafka services

**Note:** This `hostmode` folder is specifically for when using podman inside environments like devpod, where host network mode is required for container connectivity.

## Host Mode Testing

This project demonstrates containerized testing with **host network mode**, which allows containers to communicate directly with services on the host machine without port mapping.

### Testcontainers Implementation Differences

#### Python Implementation ✅
- Uses Testcontainers Python library
- Successfully configures `network_mode="host"`
- Disables port mapping with `ports = {}`
- Works reliably in podman/devpod environments

#### Java Implementation (Workaround) ⚠️
- **Testcontainers Java has known issues** with host mode ([issue #5151](https://github.com/testcontainers/testcontainers-java/issues/5151))
- Cannot detect Docker environment when using `withNetworkMode("host")`
- **Workaround**: Uses direct Docker or Podman commands instead of Testcontainers library
- Automatically detects and uses available container runtime (Docker or Podman)
- Still demonstrates host mode networking concept

### Why These Differences Matter

- **Python Testcontainers**: Properly supports host mode for podman environments
- **Java Testcontainers**: Officially doesn't support host mode due to port mapping conflicts and environment detection issues
- **Practical Impact**: Python version shows ideal Testcontainers usage; Java version shows real-world workaround

### Python Version
Uses Testcontainers with PostgreSQL and Kafka (KRaft mode) in host mode for integration testing.

### Java Version
Uses direct Docker or Podman commands to start PostgreSQL and Kafka (KRaft mode) in host mode (workaround for Testcontainers limitations). Includes a `ContainerHelper` utility class to consolidate container management logic across tests.