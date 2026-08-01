package com.common.Notification.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables the background sweep that recovers notifications whose Kafka publish never landed.
 *
 * <p>Note for multi-instance deployments: every replica runs this sweep. That is safe — the
 * republished event is keyed by notification id and delivery is idempotent — but it does mean
 * duplicated broker traffic during an outage. A distributed lock (the design doc's Redis
 * Distributed Lock) would elect a single sweeper; see the README.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}