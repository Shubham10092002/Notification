package com.common.Notification.domain;

/**
 * Communication channels supported by the platform.
 *
 * <p>The design doc (Project 8: Notification Platform) lists EMAIL, SMS, PUSH and WHATSAPP.
 * Only EMAIL and SMS are implemented today; the other two are intentionally absent rather
 * than stubbed, so an unsupported channel fails at request validation instead of silently
 * being accepted and never delivered.
 */
public enum Channel {
    EMAIL,
    SMS
}