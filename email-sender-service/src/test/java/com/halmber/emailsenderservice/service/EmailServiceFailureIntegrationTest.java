package com.halmber.emailsenderservice.service;

import com.halmber.emailsenderservice.model.dto.EmailMessageDto;
import com.halmber.emailsenderservice.model.entity.EmailMessage;
import com.halmber.emailsenderservice.repository.EmailMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@DisplayName("EmailService Failure Integration Tests")
class EmailServiceFailureIntegrationTest {

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
        registry.add("scheduling.enabled", () -> "false");
    }

    @Autowired
    private EmailServiceImpl emailService;

    @Autowired
    private EmailMessageRepository emailMessageRepository;

    @MockitoBean
    private JavaMailSender mailSender;

    @BeforeEach
    void setUp() {
        emailMessageRepository.deleteAll();
        reset(mailSender);
    }

    @Test
    @DisplayName("Should save email with FAILED status when sending fails")
    void shouldSaveEmailWithFailedStatusWhenSendingFails() {
        // Given
        doThrow(new MailSendException("Connection timeout"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-failed-001")
                .recipientEmail("fail@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When
        emailService.processEmailMessage(dto);

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> savedMessage = emailMessageRepository.findById("test-email-failed-001");

                    assertThat(savedMessage).isPresent();
                    EmailMessage message = savedMessage.get();

                    assertThat(message.getStatus()).isEqualTo(EmailMessage.EmailStatus.FAILED);
                    assertThat(message.getErrorMessage()).contains("MailSendException");
                    assertThat(message.getErrorMessage()).contains("Connection timeout");
                    assertThat(message.getRetryCount()).isEqualTo(1);
                    assertThat(message.getLastAttemptAt()).isNotNull();
                    assertThat(message.getSentAt()).isNull();
                });

        verify(mailSender, times(1)).send(any(SimpleMailMessage.class));
    }

    @Test
    @DisplayName("Should increment retry count on each failed attempt")
    void shouldIncrementRetryCountOnEachFailedAttempt() {
        // Given
        doThrow(new MailSendException("SMTP error"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-retry")
                .recipientEmail("retry@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When - First attempt
        emailService.processEmailMessage(dto);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-email-retry");
                    assertThat(message).isPresent();
                    assertThat(message.get().getRetryCount()).isEqualTo(1);
                });

        // When - Retry (second attempt)
        emailService.retryFailedMessages();

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-email-retry");
                    assertThat(message).isPresent();
                    assertThat(message.get().getRetryCount()).isEqualTo(2);
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.FAILED);
                });

        verify(mailSender, times(2)).send(any(SimpleMailMessage.class));
    }

    @Test
    @DisplayName("Should retry failed messages and succeed on retry")
    void shouldRetryFailedMessagesAndSucceedOnRetry() {
        // Given - First attempt fails
        doThrow(new MailSendException("Temporary error"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-retry-success")
                .recipientEmail("retry-success@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When - First attempt (fails)
        emailService.processEmailMessage(dto);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-email-retry-success");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.FAILED);
                });

        // Given - Second attempt succeeds
        reset(mailSender);
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        // When - Retry
        emailService.retryFailedMessages();

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-email-retry-success");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                    assertThat(message.get().getErrorMessage()).isNull();
                    assertThat(message.get().getSentAt()).isNotNull();
                });
    }

    @Test
    @DisplayName("Should handle multiple failed messages in retry batch")
    void shouldHandleMultipleFailedMessagesInRetryBatch() {
        // Given
        doThrow(new MailSendException("Error"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto1 = EmailMessageDto.builder()
                .id("test-failed-1")
                .recipientEmail("user1@example.com")
                .subject("Subject 1")
                .content("Content 1")
                .build();

        EmailMessageDto dto2 = EmailMessageDto.builder()
                .id("test-failed-2")
                .recipientEmail("user2@example.com")
                .subject("Subject 2")
                .content("Content 2")
                .build();

        EmailMessageDto dto3 = EmailMessageDto.builder()
                .id("test-failed-3")
                .recipientEmail("user3@example.com")
                .subject("Subject 3")
                .content("Content 3")
                .build();

        // When - All fail initially
        emailService.processEmailMessage(dto1);
        emailService.processEmailMessage(dto2);
        emailService.processEmailMessage(dto3);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    List<EmailMessage> failedMessages = emailMessageRepository
                            .findByStatus(EmailMessage.EmailStatus.FAILED);
                    assertThat(failedMessages).hasSize(3);
                });

        // When - Retry all failed messages
        emailService.retryFailedMessages();

        // Then
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    Optional<EmailMessage> msg1 = emailMessageRepository.findById("test-failed-1");
                    Optional<EmailMessage> msg2 = emailMessageRepository.findById("test-failed-2");
                    Optional<EmailMessage> msg3 = emailMessageRepository.findById("test-failed-3");

                    assertThat(msg1).isPresent();
                    assertThat(msg2).isPresent();
                    assertThat(msg3).isPresent();

                    // All should have retry count = 2
                    assertThat(msg1.get().getRetryCount()).isEqualTo(2);
                    assertThat(msg2.get().getRetryCount()).isEqualTo(2);
                    assertThat(msg3.get().getRetryCount()).isEqualTo(2);
                });

        verify(mailSender, times(6)).send(any(SimpleMailMessage.class)); // 3 initial + 3 retries
    }

    @Test
    @DisplayName("Should update lastAttemptAt on each failed attempt")
    void shouldUpdateLastAttemptAtOnEachFailedAttempt() {
        // Given
        doThrow(new MailSendException("Error"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-last-attempt")
                .recipientEmail("test@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When
        emailService.processEmailMessage(dto);

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-last-attempt");
                    assertThat(message).isPresent();
                    assertThat(message.get().getLastAttemptAt()).isNotNull();
                    assertThat(message.get().getCreatedAt()).isNotNull();
                    assertThat(message.get().getLastAttemptAt())
                            .isAfterOrEqualTo(message.get().getCreatedAt());
                });
    }

    @Test
    @DisplayName("Should not retry messages that were already sent successfully")
    void shouldNotRetryMessagesThatWereAlreadySentSuccessfully() {
        // Given - One successful and one failed message
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto successDto = EmailMessageDto.builder()
                .id("test-success")
                .recipientEmail("success@example.com")
                .subject("Success")
                .content("Content")
                .build();

        emailService.processEmailMessage(successDto);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-success");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                });

        reset(mailSender);
        doThrow(new MailSendException("Error"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto failDto = EmailMessageDto.builder()
                .id("test-fail")
                .recipientEmail("fail@example.com")
                .subject("Fail")
                .content("Content")
                .build();

        emailService.processEmailMessage(failDto);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-fail");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.FAILED);
                });

        // When - Retry failed messages
        reset(mailSender);
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        emailService.retryFailedMessages();

        // Then - Only the failed message should be retried
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    verify(mailSender, times(1)).send(any(SimpleMailMessage.class));

                    // Success message should still have retry count = 0
                    Optional<EmailMessage> successMsg = emailMessageRepository.findById("test-success");
                    assertThat(successMsg).isPresent();
                    assertThat(successMsg.get().getRetryCount()).isZero();
                });
    }
}
