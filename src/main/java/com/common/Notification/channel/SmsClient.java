package com.common.Notification.channel;

/**
 * Provider seam for SMS (Twilio, AWS SNS, MSG91, ...).
 *
 * <p>No provider is wired up yet because none has been chosen or credentialed. A real client
 * implements this and replaces {@link LoggingSmsClient} by setting
 * {@code notification.sms.provider}.
 */
public interface SmsClient {

    /**
     * @return the provider's message id, for reconciliation.
     * @throws com.common.Notification.exception.NotificationDeliveryException on a retryable failure.
     */
    String send(String phoneNumber, String message);
}