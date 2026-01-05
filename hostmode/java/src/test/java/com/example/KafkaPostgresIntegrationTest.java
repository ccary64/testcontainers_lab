package com.example;

import com.example.customers.Customer;
import com.example.customers.CustomerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaPostgresIntegrationTest {

    private static ContainerHelper containerHelper;
    private CustomerService customerService;

    @BeforeAll
    public static void setUp() throws IOException, InterruptedException {
        containerHelper = new ContainerHelper();
        containerHelper.initialize();
        containerHelper.startPostgresContainer();
        containerHelper.startKafkaContainer();
        containerHelper.waitForServices(15);

        // Set system properties
        System.setProperty("DB_HOST", "localhost");
        System.setProperty("DB_PORT", "5432");
        System.setProperty("DB_USERNAME", "test");
        System.setProperty("DB_PASSWORD", "test");
        System.setProperty("DB_NAME", "test");
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    }

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        if (containerHelper != null) {
            containerHelper.cleanup();
        }
    }

    @BeforeEach
    public void setUpEach() throws SQLException {
        customerService = new CustomerService();
        customerService.createTable();
        customerService.deleteAllCustomers();
    }

    @Test
    public void testCustomerCreationWithKafkaNotification() throws Exception {
        String topic = "customer-events";
        String customerName = "John Doe";
        String customerEmail = "john.doe@example.com";

        // Create customer in database
        customerService.createCustomer(customerName, customerEmail);

        // Verify customer was created
        Customer createdCustomer = customerService.getCustomerByEmail(customerEmail);
        assertNotNull(createdCustomer);
        assertEquals(customerName, createdCustomer.getName());
        assertEquals(customerEmail, createdCustomer.getEmail());

        // Send notification to Kafka
        JSONObject eventData = new JSONObject();
        eventData.put("eventType", "CUSTOMER_CREATED");
        eventData.put("customerId", createdCustomer.getId());
        eventData.put("customerName", customerName);
        eventData.put("customerEmail", customerEmail);
        eventData.put("timestamp", System.currentTimeMillis());

        // Producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Send event to Kafka
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(createdCustomer.getId()), eventData.toString());
            producer.send(record).get();
        }

        // Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Consume and verify the event
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertEquals(1, records.count());
            ConsumerRecord<String, String> record = records.iterator().next();

            // Verify the message content
            JSONObject receivedEvent = new JSONObject(record.value());
            assertEquals("CUSTOMER_CREATED", receivedEvent.getString("eventType"));
            assertEquals(createdCustomer.getId(), receivedEvent.getInt("customerId"));
            assertEquals(customerName, receivedEvent.getString("customerName"));
            assertEquals(customerEmail, receivedEvent.getString("customerEmail"));
            assertTrue(receivedEvent.has("timestamp"));
        }
    }

    @Test
    public void testMultipleCustomersWithEvents() throws Exception {
        String topic = "customer-events";

        // Create multiple customers
        customerService.createCustomer("Alice", "alice@example.com");
        customerService.createCustomer("Bob", "bob@example.com");

        // Verify customers in database
        var customers = customerService.getAllCustomers();
        assertEquals(2, customers.size());

        // Send events for each customer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (Customer customer : customers) {
                JSONObject eventData = new JSONObject();
                eventData.put("eventType", "CUSTOMER_REGISTERED");
                eventData.put("customerId", customer.getId());
                eventData.put("customerName", customer.getName());
                eventData.put("customerEmail", customer.getEmail());
                eventData.put("timestamp", System.currentTimeMillis());

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(customer.getId()), eventData.toString());
                producer.send(record).get();
            }
        }

        // Consume and verify all events
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "multi-customer-test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertEquals(2, records.count());

            // Verify each event
            for (ConsumerRecord<String, String> record : records) {
                JSONObject event = new JSONObject(record.value());
                assertEquals("CUSTOMER_REGISTERED", event.getString("eventType"));
                assertTrue(event.has("customerId"));
                assertTrue(event.has("customerName"));
                assertTrue(event.has("customerEmail"));
                assertTrue(event.has("timestamp"));
            }
        }
    }
}