package com.common.Notification.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TemplateRepository extends JpaRepository<NotificationTemplate, String> {

    Optional<NotificationTemplate> findByCodeAndChannelAndActiveIsTrue(String code, Channel channel);
}