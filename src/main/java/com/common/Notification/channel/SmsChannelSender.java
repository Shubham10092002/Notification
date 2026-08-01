package com.common.Notification.channel;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.exception.NotificationDeliveryException;
import com.common.Notification.support.Redaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SmsChannelSender implements ChannelSender {

    private final SmsClient smsClient;

    @Override
    public Channel channel() {
        return Channel.SMS;
    }

    @Override
    public void send(NotificationRecord record) {
        try {
            String providerMessageId = smsClient.send(record.getRecipient(), record.getBody());
            log.info("SMS dispatched notificationId={} recipient={} providerMessageId={}",
                    record.getId(), Redaction.mask(record.getRecipient()), providerMessageId);
        } catch (NotificationDeliveryException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new NotificationDeliveryException(
                    "SMS send failed for notificationId=" + record.getId(), ex);
        }
    }
}