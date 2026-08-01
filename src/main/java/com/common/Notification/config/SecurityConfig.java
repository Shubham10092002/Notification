package com.common.Notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Service-to-service security.
 *
 * <p>HTTP Basic over an internal network is the starting point, not the destination. The design
 * doc's common stack specifies JWT/OAuth2, which is the right target before this is exposed
 * beyond the cluster — see the README's Security section.
 *
 * <p>CSRF is disabled because this API is stateless and token-authenticated with no browser
 * session to forge against.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**").permitAll()
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults());
        return http.build();
    }
}