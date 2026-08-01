package com.common.Notification.channel;

import com.common.Notification.support.Redaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Placeholder SMS client. It does NOT send anything — it logs and returns a synthetic id.
 *
 * <p>Active by default so the platform runs end-to-end without a provider account. Everything
 * around it is real: the record is persisted, the event flows through Kafka, retries and the
 * DLT behave normally. Only the final hop to a carrier is missing.
 *
 * <p>Swap it by setting {@code notification.sms.provider} to something other than {@code log}
 * and supplying a {@link SmsClient} bean for that provider.
 */
@Component
@ConditionalOnProperty(name = "notification.sms.provider", havingValue = "log", matchIfMissing = true)
@Slf4j
public class LoggingSmsClient implements SmsClient {

    @Override
    public String send(String phoneNumber, String message) {
        String syntheticId = "log-" + UUID.randomUUID();
        log.warn("SMS NOT ACTUALLY SENT (no provider configured) to={} chars={} syntheticId={}",
                Redaction.mask(phoneNumber), message == null ? 0 : message.length(), syntheticId);
        return syntheticId;
    }
}