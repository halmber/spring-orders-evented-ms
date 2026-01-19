package com.halmber.springordersapi.service;

import com.halmber.springordersapi.model.dto.messaging.EmailMessageDto;
import com.halmber.springordersapi.model.dto.request.customer.CustomerCreateDto;
import com.halmber.springordersapi.model.dto.response.customer.CustomerResponseDto;
import com.halmber.springordersapi.repository.CustomerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {"emailSend"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("CustomerService Email Integration Tests")
class CustomerServiceEmailIntegrationTest {

    @Autowired
    private CustomerService customerService;

    @Autowired
    private CustomerRepository customerRepository;

    private KafkaMessageListenerContainer<String, EmailMessageDto> container;
    private BlockingQueue<ConsumerRecord<String, EmailMessageDto>> records;

    @BeforeEach
    void setUp() {
        records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "test-customer-group");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EmailMessageDto.class.getName());

        DefaultKafkaConsumerFactory<String, EmailMessageDto> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProperties = new ContainerProperties("emailSend");
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setupMessageListener((MessageListener<String, EmailMessageDto>) records::add);
        container.start();

        ContainerTestUtils.waitForAssignment(container, 2);
    }

    @AfterEach
    void tearDown() {
        customerRepository.deleteAll();
        if (container != null) {
            container.stop();
        }
    }

    @Test
    @DisplayName("Should send welcome email when customer is created")
    void shouldSendWelcomeEmailWhenCustomerCreated() throws InterruptedException {
        // Given
        CustomerCreateDto createDto = CustomerCreateDto.builder()
                .firstName("John")
                .lastName("Doe")
                .email("john.doe@example.com")
                .phone("+380501234567")
                .city("Kyiv")
                .build();

        // When
        CustomerResponseDto response = customerService.create(createDto);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();

        EmailMessageDto emailDto = record.value();
        assertThat(emailDto).isNotNull();
        assertThat(emailDto.id()).startsWith("customer-welcome-");
        assertThat(emailDto.id()).endsWith(response.id().toString());
        assertThat(emailDto.recipientEmail()).isEqualTo("john.doe@example.com");
        assertThat(emailDto.subject()).isEqualTo("Welcome to Orders API!");
        assertThat(emailDto.content()).contains("John Doe");
        assertThat(emailDto.content()).contains("Kyiv");
        assertThat(emailDto.content()).contains("john.doe@example.com");
    }

    @Test
    @DisplayName("Should send email with correct customer details")
    void shouldSendEmailWithCorrectCustomerDetails() throws InterruptedException {
        // Given
        CustomerCreateDto createDto = CustomerCreateDto.builder()
                .firstName("Alice")
                .lastName("Smith")
                .email("alice.smith@example.com")
                .phone("+380509876543")
                .city("Lviv")
                .build();

        // When
        customerService.create(createDto);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        EmailMessageDto emailDto = record.value();

        assertThat(emailDto.content()).contains("Alice Smith");
        assertThat(emailDto.content()).contains("Lviv");
        assertThat(emailDto.content()).contains("alice.smith@example.com");
        assertThat(emailDto.content()).contains("Welcome to our Orders API platform");
    }

    @Test
    @DisplayName("Should handle special characters in customer name")
    void shouldHandleSpecialCharactersInCustomerName() throws InterruptedException {
        // Given
        CustomerCreateDto createDto = CustomerCreateDto.builder()
                .firstName("María")
                .lastName("García-López")
                .email("maria@example.com")
                .phone("+380501234567")
                .city("Одеса")
                .build();

        // When
        customerService.create(createDto);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        EmailMessageDto emailDto = record.value();

        assertThat(emailDto.content()).contains("María García-López");
        assertThat(emailDto.content()).contains("Одеса");
    }
}

