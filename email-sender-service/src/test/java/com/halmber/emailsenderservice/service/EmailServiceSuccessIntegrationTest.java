package com.halmber.emailsenderservice.service;

import com.halmber.emailsenderservice.model.dto.EmailMessageDto;
import com.halmber.emailsenderservice.model.entity.EmailMessage;
import com.halmber.emailsenderservice.repository.EmailMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false"
})
@Testcontainers
@ActiveProfiles("test")
@DisplayName("EmailService Success Integration Tests")
class EmailServiceSuccessIntegrationTest {

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
    @DisplayName("Should successfully send email and save with SENT status")
    void shouldSuccessfullySendEmailAndSaveWithSentStatus() {
        // Given
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-001")
                .recipientEmail("success@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When
        emailService.processEmailMessage(dto);

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> savedMessage = emailMessageRepository.findById("test-email-001");

                    assertThat(savedMessage).isPresent();
                    EmailMessage message = savedMessage.get();

                    assertThat(message.getId()).isEqualTo("test-email-001");
                    assertThat(message.getRecipientEmail()).isEqualTo("success@example.com");
                    assertThat(message.getSubject()).isEqualTo("Test Subject");
                    assertThat(message.getContent()).isEqualTo("Test Content");
                    assertThat(message.getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                    assertThat(message.getSentAt()).isNotNull();
                    assertThat(message.getErrorMessage()).isNull();
                    assertThat(message.getRetryCount()).isZero();
                });

        verify(mailSender, times(1)).send(any(SimpleMailMessage.class));
    }

    @Test
    @DisplayName("Should send correct email message via JavaMailSender")
    void shouldSendCorrectEmailMessageViaJavaMailSender() {
        // Given
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-002")
                .recipientEmail("test@example.com")
                .subject("Important Subject")
                .content("Important Content")
                .build();

        // When
        emailService.processEmailMessage(dto);

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    verify(mailSender, times(1))
                            .send(argThat((SimpleMailMessage msg) ->
                                    msg.getTo() != null &&
                                            msg.getTo().length == 1 &&
                                            "test@example.com".equals(msg.getTo()[0]) &&
                                            "Important Subject".equals(msg.getSubject()) &&
                                            "Important Content".equals(msg.getText())
                            ));
                });

    }

    @Test
    @DisplayName("Should handle multiple successful email sends")
    void shouldHandleMultipleSuccessfulEmailSends() {
        // Given
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto1 = EmailMessageDto.builder()
                .id("test-email-multi-1")
                .recipientEmail("user1@example.com")
                .subject("Subject 1")
                .content("Content 1")
                .build();

        EmailMessageDto dto2 = EmailMessageDto.builder()
                .id("test-email-multi-2")
                .recipientEmail("user2@example.com")
                .subject("Subject 2")
                .content("Content 2")
                .build();

        EmailMessageDto dto3 = EmailMessageDto.builder()
                .id("test-email-multi-3")
                .recipientEmail("user3@example.com")
                .subject("Subject 3")
                .content("Content 3")
                .build();

        // When
        emailService.processEmailMessage(dto1);
        emailService.processEmailMessage(dto2);
        emailService.processEmailMessage(dto3);

        // Then
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertThat(emailMessageRepository.findById("test-email-multi-1"))
                            .isPresent()
                            .get()
                            .extracting(EmailMessage::getStatus)
                            .isEqualTo(EmailMessage.EmailStatus.SENT);

                    assertThat(emailMessageRepository.findById("test-email-multi-2"))
                            .isPresent()
                            .get()
                            .extracting(EmailMessage::getStatus)
                            .isEqualTo(EmailMessage.EmailStatus.SENT);

                    assertThat(emailMessageRepository.findById("test-email-multi-3"))
                            .isPresent()
                            .get()
                            .extracting(EmailMessage::getStatus)
                            .isEqualTo(EmailMessage.EmailStatus.SENT);
                });

        verify(mailSender, times(3)).send(any(SimpleMailMessage.class));
    }

    @Test
    @DisplayName("Should skip already sent messages")
    void shouldSkipAlreadySentMessages() {
        // Given
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-duplicate")
                .recipientEmail("test@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When
        emailService.processEmailMessage(dto);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> savedMessage = emailMessageRepository.findById("test-email-duplicate");
                    assertThat(savedMessage).isPresent();
                    assertThat(savedMessage.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                });

        // Process same message again
        emailService.processEmailMessage(dto);

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    // Should still have only one message with SENT status
                    Optional<EmailMessage> message = emailMessageRepository.findById("test-email-duplicate");
                    assertThat(message).isPresent();
                    assertThat(message.get().getStatus()).isEqualTo(EmailMessage.EmailStatus.SENT);
                });

        // Verify mail was sent only once (not twice)
        verify(mailSender, times(1)).send(any(SimpleMailMessage.class));
    }

    @Test
    @DisplayName("Should set sentAt timestamp when email is sent successfully")
    void shouldSetSentAtTimestampWhenEmailSentSuccessfully() {
        // Given
        doNothing().when(mailSender).send(any(SimpleMailMessage.class));

        EmailMessageDto dto = EmailMessageDto.builder()
                .id("test-email-timestamp")
                .recipientEmail("test@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .build();

        // When
        emailService.processEmailMessage(dto);

        // Then
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Optional<EmailMessage> savedMessage = emailMessageRepository.findById("test-email-timestamp");

                    assertThat(savedMessage).isPresent();
                    EmailMessage message = savedMessage.get();

                    assertThat(message.getSentAt()).isNotNull();
                    assertThat(message.getCreatedAt()).isNotNull();
                    assertThat(message.getSentAt()).isAfterOrEqualTo(message.getCreatedAt());
                });
    }
}
