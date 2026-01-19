package com.halmber.springordersapi.service;

import com.halmber.springordersapi.model.dto.messaging.EmailMessageDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
@DisplayName("EmailMessageProducerService Integration Tests")
class EmailMessageProducerServiceIntegrationTest {

    @Autowired
    private EmailMessageProducerService producerService;

    @Value("${kafka.topics.emailSend}")
    private String emailSendTopic;

    private KafkaMessageListenerContainer<String, EmailMessageDto> container;
    private BlockingQueue<ConsumerRecord<String, EmailMessageDto>> records;

    @BeforeEach
    void setUp() {
        records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EmailMessageDto.class.getName());

        DefaultKafkaConsumerFactory<String, EmailMessageDto> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProperties = new ContainerProperties(emailSendTopic);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setupMessageListener((MessageListener<String, EmailMessageDto>) records::add);
        container.start();

        ContainerTestUtils.waitForAssignment(container, 2);
    }

    @AfterEach
    void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    @DisplayName("Should send email notification to Kafka topic")
    void shouldSendEmailNotificationToKafka() throws InterruptedException {
        // Given
        String id = "test-email-001";
        String recipientEmail = "test@example.com";
        String subject = "Test Subject";
        String content = "Test Content";

        // When
        producerService.sendEmailNotification(id, recipientEmail, subject, content);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(id);

        EmailMessageDto receivedDto = record.value();
        assertThat(receivedDto).isNotNull();
        assertThat(receivedDto.id()).isEqualTo(id);
        assertThat(receivedDto.recipientEmail()).isEqualTo(recipientEmail);
        assertThat(receivedDto.subject()).isEqualTo(subject);
        assertThat(receivedDto.content()).isEqualTo(content);
    }

    @Test
    @DisplayName("Should send multiple email notifications")
    void shouldSendMultipleEmailNotifications() throws InterruptedException {
        // Given
        String id1 = "test-email-001";
        String id2 = "test-email-002";
        String id3 = "test-email-003";

        // When
        producerService.sendEmailNotification(id1, "user1@example.com", "Subject 1", "Content 1");
        producerService.sendEmailNotification(id2, "user2@example.com", "Subject 2", "Content 2");
        producerService.sendEmailNotification(id3, "user3@example.com", "Subject 3", "Content 3");

        // Then
        ConsumerRecord<String, EmailMessageDto> record1 = records.poll(10, TimeUnit.SECONDS);
        ConsumerRecord<String, EmailMessageDto> record2 = records.poll(10, TimeUnit.SECONDS);
        ConsumerRecord<String, EmailMessageDto> record3 = records.poll(10, TimeUnit.SECONDS);

        assertThat(record1).isNotNull();
        assertThat(record2).isNotNull();
        assertThat(record3).isNotNull();

        assertThat(record1.value().id()).isEqualTo(id1);
        assertThat(record2.value().id()).isEqualTo(id2);
        assertThat(record3.value().id()).isEqualTo(id3);
    }

    @Test
    @DisplayName("Should handle special characters in email content")
    void shouldHandleSpecialCharactersInEmailContent() throws InterruptedException {
        // Given
        String id = "test-email-special";
        String recipientEmail = "test@example.com";
        String subject = "Special: «Тест» #123 & <tag>";
        String content = "Content with\nnewlines\tand\ttabs and 'quotes' and \"double quotes\"";

        // When
        producerService.sendEmailNotification(id, recipientEmail, subject, content);

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        EmailMessageDto receivedDto = record.value();
        assertThat(receivedDto.subject()).isEqualTo(subject);
        assertThat(receivedDto.content()).isEqualTo(content);
    }

    @Test
    @DisplayName("Should use correct message key for partitioning")
    void shouldUseCorrectMessageKeyForPartitioning() throws InterruptedException {
        // Given
        String id = "test-email-key";
        String recipientEmail = "test@example.com";

        // When
        producerService.sendEmailNotification(id, recipientEmail, "Subject", "Content");

        // Then
        ConsumerRecord<String, EmailMessageDto> record = records.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(id);
    }
}

