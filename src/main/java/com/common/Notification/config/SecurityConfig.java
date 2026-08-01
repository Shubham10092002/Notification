package com.common.Notification.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.util.StringUtils;

/**
 * Service-to-service security.
 *
 * <p>HTTP Basic over an internal network is the starting point, not the destination. The design
 * doc's common stack specifies JWT/OAuth2, which is the right target before this is exposed
 * beyond the cluster — see the README's Known Gaps.
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

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * The calling services' credential.
     *
     * <p>Configured explicitly rather than left to Boot's generated random password, which
     * changes on every restart and would make the API uncallable in any deployed environment.
     *
     * <p>Startup fails when no password is set. That is deliberate: defaulting to a well-known
     * password would put an unauthenticated notification sender on the network, and a service
     * that can email and text customers is not something to leave open by accident.
     */
    @Bean
    UserDetailsService userDetailsService(
            @Value("${notification.api.username}") String username,
            @Value("${notification.api.password}") String password,
            PasswordEncoder passwordEncoder) {

        if (!StringUtils.hasText(password)) {
            throw new IllegalStateException(
                    "notification.api.password is not set. Provide NOTIFICATION_API_PASSWORD "
                            + "(see application-local.properties.example).");
        }

        return new InMemoryUserDetailsManager(User.withUsername(username)
                .password(passwordEncoder.encode(password))
                .roles("NOTIFICATION_CLIENT")
                .build());
    }
}