package com.common.Notification.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

/**
 * Redis-backed caching for template lookups.
 *
 * <p>JSON rather than JDK serialization so cached entries stay readable with redis-cli and
 * survive unrelated class changes.
 *
 * <p>Uses {@code GenericJacksonJsonRedisSerializer} and {@code tools.jackson} — Spring Boot 4
 * ships Jackson 3; the {@code com.fasterxml} / {@code Jackson2} variants are the legacy pair.
 */
@Configuration
@EnableCaching
public class CacheConfig {

    @Bean
    RedisCacheConfiguration cacheConfiguration(
            ObjectMapper objectMapper,
            @Value("${notification.cache.template-ttl-minutes:30}") long templateTtlMinutes) {

        return RedisCacheConfiguration.defaultCacheConfig()
                // Bounded TTL so a template edited directly in the DB cannot be served stale forever.
                .entryTtl(Duration.ofMinutes(templateTtlMinutes))
                // Caching a null would pin a "template missing" answer in Redis after it is created.
                .disableCachingNullValues()
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new GenericJacksonJsonRedisSerializer(objectMapper)));
    }
}