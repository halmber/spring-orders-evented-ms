package com.halmber.springordersapi.service;

import com.halmber.springordersapi.model.dto.messaging.EmailMessageDto;
import com.halmber.springordersapi.model.dto.request.order.OrderCreateDto;
import com.halmber.springordersapi.model.dto.response.order.OrderResponseDto;
import com.halmber.springordersapi.model.entity.Customer;
import com.halmber.springordersapi.repository.CustomerRepository;
import com.halmber.springordersapi.repository.OrderRepository;
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
@DisplayName("OrderService Email Integration Tests")
class OrderServiceEmailIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private OrderRepository orderRepository;

    private KafkaMessageListenerContainer<String, EmailMessageDto> container;
    private BlockingQueue<ConsumerRecord<String, EmailMessageDto>> records;
    private Customer testCustomer;

    @BeforeEach
    void setUp() {
        records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "test-order-group");
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

        // Create test customer
        testCustomer = Customer.builder()
                .firstName("Test")
                .lastName("Customer")
                .email("test.customer@example.com")
                .phone("+380501234567")
                .city("Kharkiv")
                .build();
        testCustomer = customerRepository.save(testCustomer);
    }

    @AfterEach
    void tearDown() {
        orderRepository.deleteAll();
        customerRepository.deleteAll();
        if (container != null) {
            container.stop();
        }
    }

    @Test
    @DisplayName("Should send confirmation email when order is created")
    void shouldSendConfirmationEmailWhenOrderCreated() throws InterruptedException {
        // Given
        OrderCreateDto createDto = OrderCreateDto.builder()
                .customerId(testCustomer.getId().toString())
                .amount(150.50)
                .status("NEW")
                .paymentMethod("CARD")
                .build();

        // When
        OrderResponseDto response = orderService.create(createDto);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();

        EmailMessageDto emailDto = record.value();
        assertThat(emailDto).isNotNull();
        assertThat(emailDto.id()).startsWith("order-confirmation-");
        assertThat(emailDto.id()).endsWith(response.id().toString());
        assertThat(emailDto.recipientEmail()).isEqualTo(testCustomer.getEmail());
        assertThat(emailDto.subject()).contains("Order Confirmation");
        assertThat(emailDto.subject()).contains(response.id().toString());
        assertThat(emailDto.content()).contains("Test Customer");
        assertThat(emailDto.content()).contains("150.50");
        assertThat(emailDto.content()).contains("NEW");
        assertThat(emailDto.content()).contains("CARD");
    }

    @Test
    @DisplayName("Should include all order details in email content")
    void shouldIncludeAllOrderDetailsInEmailContent() throws InterruptedException {
        // Given
        OrderCreateDto createDto = OrderCreateDto.builder()
                .customerId(testCustomer.getId().toString())
                .amount(299.99)
                .status("PROCESSING")
                .paymentMethod("PAYPAL")
                .build();

        // When
        OrderResponseDto response = orderService.create(createDto);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        EmailMessageDto emailDto = record.value();

        assertThat(emailDto.content()).contains("Order ID: " + response.id());
        assertThat(emailDto.content()).contains("Amount: 299.99");
        assertThat(emailDto.content()).contains("Status: PROCESSING");
        assertThat(emailDto.content()).contains("Payment Method: PAYPAL");
        assertThat(emailDto.content()).contains("Dear Test Customer");
    }

    @Test
    @DisplayName("Should send email to correct customer email")
    void shouldSendEmailToCorrectCustomerEmail() throws InterruptedException {
        // Given
        Customer anotherCustomer = Customer.builder()
                .firstName("Another")
                .lastName("Customer")
                .email("another.customer@example.com")
                .phone("+380509876543")
                .city("Lviv")
                .build();
        anotherCustomer = customerRepository.save(anotherCustomer);

        OrderCreateDto createDto = OrderCreateDto.builder()
                .customerId(anotherCustomer.getId().toString())
                .amount(99.99)
                .status("NEW")
                .paymentMethod("CASH")
                .build();

        // When
        orderService.create(createDto);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        EmailMessageDto emailDto = record.value();

        assertThat(emailDto.recipientEmail()).isEqualTo("another.customer@example.com");
        assertThat(emailDto.content()).contains("Another Customer");
    }
}
