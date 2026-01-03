package com.example;

import com.example.customers.Customer;
import com.example.customers.CustomerService;
import org.junit.jupiter.api.*;
import java.io.IOException;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CustomerServiceTest {

    private static Process postgresProcess;
    private static String containerId;
    private static String containerRuntime;

    private CustomerService customerService;

    private static String detectContainerRuntime() throws IOException, InterruptedException {
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

    @BeforeAll
    public static void setUpDatabase() throws IOException, InterruptedException {
        containerRuntime = detectContainerRuntime();

        // Start PostgreSQL container directly with detected runtime
        ProcessBuilder pb = new ProcessBuilder(
            containerRuntime, "run", "-d", "--network", "host",
            "-e", "POSTGRES_DB=test",
            "-e", "POSTGRES_USER=test",
            "-e", "POSTGRES_PASSWORD=test",
            "postgres:16-alpine"
        );
        // pb.inheritIO(); // Remove to avoid corrupted channel warning
        postgresProcess = pb.start();
        postgresProcess.waitFor();

        // Get container ID
        ProcessBuilder getIdPb = new ProcessBuilder(containerRuntime, "ps", "-q", "-l");
        Process getIdProcess = getIdPb.start();
        containerId = new String(getIdProcess.getInputStream().readAllBytes()).trim();

        // Wait a bit for PostgreSQL to start
        Thread.sleep(3000);

        System.setProperty("DB_HOST", "localhost");
        System.setProperty("DB_PORT", "5432");
        System.setProperty("DB_USERNAME", "test");
        System.setProperty("DB_PASSWORD", "test");
        System.setProperty("DB_NAME", "test");
    }

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        if (containerId != null && !containerId.isEmpty()) {
            new ProcessBuilder(containerRuntime, "stop", containerId).start().waitFor();
            new ProcessBuilder(containerRuntime, "rm", containerId).start().waitFor();
        }
    }

    @BeforeEach
    public void setUp() throws SQLException {
        customerService = new CustomerService();
        customerService.createTable();
        customerService.deleteAllCustomers();
    }

    @Test
    public void testGetAllCustomers() throws SQLException {
        customerService.createCustomer("Siva", "siva@gmail.com");
        customerService.createCustomer("James", "james@gmail.com");
        List<Customer> customers = customerService.getAllCustomers();
        assertEquals(2, customers.size());
    }

    @Test
    public void testGetCustomerByEmail() throws SQLException {
        customerService.createCustomer("John", "john@gmail.com");
        Customer customer = customerService.getCustomerByEmail("john@gmail.com");
        assertEquals("John", customer.getName());
        assertEquals("john@gmail.com", customer.getEmail());
    }
}