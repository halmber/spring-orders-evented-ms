package com.halmber.emailsenderservice.listener;

import com.halmber.emailsenderservice.model.dto.EmailMessageDto;
import com.halmber.emailsenderservice.model.entity.EmailMessage;
import com.halmber.emailsenderservice.repository.EmailMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.reset;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {"emailSend"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9093",
                "port=9093"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("EmailSendMessageListener Integration Tests")
class EmailSendMessageListenerIntegrationTest {

    @Container
    static ElasticsearchContainer elasticsearchContainer = new ElasticsearchContainer(
            "docker.elastic.co/elasticsearch/elasticsearch:8.6.1"
    )
            .withEnv("xpack.security.enabled", "false")
            .withEnv("discovery.type", "single-node")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("elasticsearch.address",
                () -> elasticsearchContainer.getHttpHostAddress());
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9093");
        registry.add("scheduling.enabled", () -> "false");
    }

    @Autowired
    private KafkaTemplate<String, EmailMessageDto> kafkaTemplate;

    @Autowired
    private EmailMessageRepository emailMessageRepository;

    @MockitoBean
    private JavaMailSender mailSender;

    @BeforeEach
    void setUp() {
        emailMessageRepository.deleteAll();
        reset(mailSender);
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));
    }

    @Test
    @DisplayName("Should process email message from Kafka topic")
    void shouldProcessEmailMessageFromKafkaTopic() {
        // Given
        EmailMessageDto dto = EmailMessageDto.builder()
                .id("kafka-test-001")
                .recipientEmail("kafka@example.com")
                .subject("Kafka Test")
                .content("Message from Kafka")
                .build();

        // When
        kafkaTemplate.send("emailSend", dto.id(), dto);

        // Then
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    Optional<EmailMessage> savedMessage = emailMessageRepository.findById("kafka-test-001");

                    assertThat(savedMessage).isPresent();
                    EmailMessage message = savedMessage.get();

                    assertThat(message.getRecipientEmail()).isEqualTo("kafka@example.com");
                    assertThat(message.getSubject()).isEqualTo("Kafka Test");
                    assertThat(message.getContent()).isEqualTo("Message from Kafka");
                    assertThat(message.getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                });
    }

    @Test
    @DisplayName("Should process multiple messages from Kafka")
    void shouldProcessMultipleMessagesFromKafka() {
        // Given
        EmailMessageDto dto1 = EmailMessageDto.builder()
                .id("kafka-multi-1")
                .recipientEmail("user1@example.com")
                .subject("Message 1")
                .content("Content 1")
                .build();

        EmailMessageDto dto2 = EmailMessageDto.builder()
                .id("kafka-multi-2")
                .recipientEmail("user2@example.com")
                .subject("Message 2")
                .content("Content 2")
                .build();

        EmailMessageDto dto3 = EmailMessageDto.builder()
                .id("kafka-multi-3")
                .recipientEmail("user3@example.com")
                .subject("Message 3")
                .content("Content 3")
                .build();

        // When
        kafkaTemplate.send("emailSend", dto1.id(), dto1);
        kafkaTemplate.send("emailSend", dto2.id(), dto2);
        kafkaTemplate.send("emailSend", dto3.id(), dto3);

        // Then
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(emailMessageRepository.findById("kafka-multi-1")).isPresent();
                    assertThat(emailMessageRepository.findById("kafka-multi-2")).isPresent();
                    assertThat(emailMessageRepository.findById("kafka-multi-3")).isPresent();

                    assertThat(emailMessageRepository.findById("kafka-multi-1").get().getStatus())
                            .isEqualTo(EmailMessage.EmailStatus.SENT);
                    assertThat(emailMessageRepository.findById("kafka-multi-2").get().getStatus())
                            .isEqualTo(EmailMessage.EmailStatus.SENT);
                    assertThat(emailMessageRepository.findById("kafka-multi-3").get().getStatus())
                            .isEqualTo(EmailMessage.EmailStatus.SENT);
                });
    }

    @Test
    @DisplayName("Should skip messages without ID")
    void shouldSkipMessagesWithoutId() {
        // Given
        EmailMessageDto dtoWithoutId = EmailMessageDto.builder()
                .id(null)
                .recipientEmail("test@example.com")
                .subject("Test")
                .content("Content")
                .build();

        EmailMessageDto dtoWithBlankId = EmailMessageDto.builder()
                .id("")
                .recipientEmail("test2@example.com")
                .subject("Test 2")
                .content("Content 2")
                .build();

        // When
        kafkaTemplate.send("emailSend", "null-key", dtoWithoutId);
        kafkaTemplate.send("emailSend", "blank-key", dtoWithBlankId);

        // Then - Wait a bit to ensure messages are processed (or not)
        await().pollDelay(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    // No messages should be saved
                    assertThat(emailMessageRepository.count()).isZero();
                });
    }

    @Test
    @DisplayName("Should handle duplicate messages correctly")
    void shouldHandleDuplicateMessagesCorrectly() {
        // Given
        EmailMessageDto dto = EmailMessageDto.builder()
                .id("kafka-duplicate")
                .recipientEmail("duplicate@example.com")
                .subject("Duplicate Test")
                .content("Content")
                .build();

        // When - Send same message twice
        kafkaTemplate.send("emailSend", dto.id(), dto);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("kafka-duplicate");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                });

        kafkaTemplate.send("emailSend", dto.id(), dto);

        // Then - Should still have only one message with SENT status
        await().pollDelay(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("kafka-duplicate");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                    // Retry count should still be 0
                    assertThat(message.get().getRetryCount()).isZero();
                });
    }

    @Test
    @DisplayName("Should handle special characters in messages from Kafka")
    void shouldHandleSpecialCharactersInMessagesFromKafka() {
        // Given
        EmailMessageDto dto = EmailMessageDto.builder()
                .id("kafka-special-chars")
                .recipientEmail("special@example.com")
                .subject("Спеціальні символи: «Тест» #123")
                .content("Content with\nnewlines and\ttabs and 'quotes'")
                .build();

        // When
        kafkaTemplate.send("emailSend", dto.id(), dto);

        // Then
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    Optional<EmailMessage> savedMessage = emailMessageRepository.findById("kafka-special-chars");

                    assertThat(savedMessage).isPresent();
                    EmailMessage message = savedMessage.get();

                    assertThat(message.getSubject()).isEqualTo("Спеціальні символи: «Тест» #123");
                    assertThat(message.getContent()).contains("newlines");
                    assertThat(message.getContent()).contains("tabs");
                    assertThat(message.getContent()).contains("quotes");
                });
    }
}

