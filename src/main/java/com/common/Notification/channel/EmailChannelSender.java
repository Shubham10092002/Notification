package com.common.Notification.channel;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.exception.NotificationDeliveryException;
import com.common.Notification.support.Redaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/**
 * Email delivery over SMTP via Spring's {@link JavaMailSender}.
 *
 * <p>Configured through {@code spring.mail.*}; credentials come from the environment, never
 * from a committed file.
 */
@Component
@Slf4j
public class EmailChannelSender implements ChannelSender {

    private final JavaMailSender mailSender;
    private final String fromAddress;

    public EmailChannelSender(JavaMailSender mailSender,
                              @Value("${notification.email.from}") String fromAddress) {
        this.mailSender = mailSender;
        this.fromAddress = fromAddress;
    }

    @Override
    public Channel channel() {
        return Channel.EMAIL;
    }

    @Override
    public void send(NotificationRecord record) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(fromAddress);
        message.setTo(record.getRecipient());
        message.setSubject(record.getSubject());
        message.setText(record.getBody());

        try {
            mailSender.send(message);
            log.info("Email delivered notificationId={} recipient={}",
                    record.getId(), Redaction.mask(record.getRecipient()));
        } catch (MailException ex) {
            // Spring does not distinguish "bad address" from "SMTP is down" in the exception
            // type, so everything is treated as retryable and the DLT catches the permanent ones.
            throw new NotificationDeliveryException(
                    "SMTP send failed for notificationId=" + record.getId(), ex);
        }
    }
}