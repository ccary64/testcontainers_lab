package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ContainerHelper {
    private String containerRuntime;
    private List<String> containerIds;

    public ContainerHelper() {
        this.containerIds = new ArrayList<>();
    }

    public void initialize() throws IOException, InterruptedException {
        this.containerRuntime = detectContainerRuntime();
    }

    private String detectContainerRuntime() throws IOException, InterruptedException {
        // Try docker first
        try {
            ProcessBuilder checkDocker = new ProcessBuilder("docker", "--version");
            Process dockerProcess = checkDocker.start();
            if (dockerProcess.waitFor() == 0) {
                return "docker";
            }
        } catch (Exception e) {
            // Docker not available
        }

        // Try podman
        try {
            ProcessBuilder checkPodman = new ProcessBuilder("podman", "--version");
            Process podmanProcess = checkPodman.start();
            if (podmanProcess.waitFor() == 0) {
                return "podman";
            }
        } catch (Exception e) {
            // Podman not available
        }

        throw new RuntimeException("Neither docker nor podman is available on this system");
    }

    public void startPostgresContainer() throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(containerRuntime);
        command.add("run");
        command.add("-d");
        command.add("--network");
        command.add("host");
        command.add("-e");
        command.add("POSTGRES_DB=test");
        command.add("-e");
        command.add("POSTGRES_USER=test");
        command.add("-e");
        command.add("POSTGRES_PASSWORD=test");
        command.add("postgres:16-alpine");

        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = pb.start();
        process.waitFor();

        // Get container ID
        ProcessBuilder getIdPb = new ProcessBuilder(containerRuntime, "ps", "-q", "-l");
        Process getIdProcess = getIdPb.start();
        String containerId = new String(getIdProcess.getInputStream().readAllBytes()).trim();
        containerIds.add(containerId);
    }

    public void startKafkaContainer() throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(containerRuntime);
        command.add("run");
        command.add("-d");
        command.add("--network");
        command.add("host");
        command.add("-e");
        command.add("KAFKA_NODE_ID=1");
        command.add("-e");
        command.add("KAFKA_PROCESS_ROLES=broker,controller");
        command.add("-e");
        command.add("KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093");
        command.add("-e");
        command.add("KAFKA_LISTENERS=PLAINTEXT@localhost:9092,CONTROLLER@localhost:9093");
        command.add("-e");
        command.add("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT@localhost:9092");
        command.add("-e");
        command.add("KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER");
        command.add("-e");
        command.add("KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT");
        command.add("-e");
        command.add("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1");
        command.add("-e");
        command.add("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1");
        command.add("-e");
        command.add("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1");
        command.add("confluentinc/cp-kafka:7.6.0");

        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = pb.start();
        process.waitFor();

        // Get container ID
        ProcessBuilder getIdPb = new ProcessBuilder(containerRuntime, "ps", "-q", "-l");
        Process getIdProcess = getIdPb.start();
        String containerId = new String(getIdProcess.getInputStream().readAllBytes()).trim();
        containerIds.add(containerId);
    }

    public void waitForServices(int seconds) throws InterruptedException {
        Thread.sleep(seconds * 1000L);
    }

    public void cleanup() throws IOException, InterruptedException {
        for (String containerId : containerIds) {
            if (containerId != null && !containerId.isEmpty()) {
                new ProcessBuilder(containerRuntime, "stop", containerId).start().waitFor();
                new ProcessBuilder(containerRuntime, "rm", containerId).start().waitFor();
            }
        }
        containerIds.clear();
    }

    public String getContainerRuntime() {
        return containerRuntime;
    }
}