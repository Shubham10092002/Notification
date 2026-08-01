package com.common.Notification.support;

/**
 * Recipients are PII. Everything that reaches a log line, a metric tag or an error message
 * goes through here first, so an operator can correlate a delivery without the log becoming
 * a dump of customer emails and phone numbers.
 */
public final class Redaction {

    private Redaction() {
    }

    /**
     * {@code jane.doe@example.com -> j***e@example.com}, {@code +919876543210 -> +91*****3210}.
     * Never returns the full value.
     */
    public static String mask(String recipient) {
        if (recipient == null || recipient.isBlank()) {
            return "<none>";
        }
        int at = recipient.indexOf('@');
        if (at > 0) {
            return maskLocalPart(recipient.substring(0, at)) + recipient.substring(at);
        }
        return maskPhone(recipient);
    }

    private static String maskLocalPart(String local) {
        if (local.length() <= 2) {
            return "*".repeat(local.length());
        }
        return local.charAt(0) + "***" + local.charAt(local.length() - 1);
    }

    private static String maskPhone(String phone) {
        if (phone.length() <= 4) {
            return "*".repeat(phone.length());
        }
        String tail = phone.substring(phone.length() - 4);
        return phone.substring(0, Math.min(3, phone.length() - 4)) + "*****" + tail;
    }
}