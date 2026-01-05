package com.example;

import com.example.customers.Customer;
import com.example.customers.CustomerService;
import org.junit.jupiter.api.*;
import java.io.IOException;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CustomerServiceTest {

    private static ContainerHelper containerHelper;
    private CustomerService customerService;

    @BeforeAll
    public static void setUpDatabase() throws IOException, InterruptedException {
        containerHelper = new ContainerHelper();
        containerHelper.initialize();
        containerHelper.startPostgresContainer();
        containerHelper.waitForServices(3);

        System.setProperty("DB_HOST", "localhost");
        System.setProperty("DB_PORT", "5432");
        System.setProperty("DB_USERNAME", "test");
        System.setProperty("DB_PASSWORD", "test");
        System.setProperty("DB_NAME", "test");
    }

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        if (containerHelper != null) {
            containerHelper.cleanup();
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