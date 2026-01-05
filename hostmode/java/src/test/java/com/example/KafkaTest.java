package com.example;

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
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaTest {

    private static ContainerHelper containerHelper;

    @BeforeAll
    public static void setUp() throws IOException, InterruptedException {
        containerHelper = new ContainerHelper();
        containerHelper.initialize();
        containerHelper.startKafkaContainer();
        containerHelper.waitForServices(10);

        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    }

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        if (containerHelper != null) {
            containerHelper.cleanup();
        }
    }

    @Test
    public void testKafkaProduceAndConsume() throws Exception {
        String topic = "test-topic";
        String message = "Hello, Kafka with KRaft!";

        // Producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Produce message
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            producer.send(record).get(); // Wait for send to complete
        }

        // Consume message
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            ConsumerRecord<String, String> record = records.iterator().next();
            assertEquals(message, record.value());
        }
    }
}