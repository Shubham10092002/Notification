# Notification

A shared notification platform used by every other service in the estate. Callers hand over a
notification; this service renders it from a template, queues it, and delivers it over the
requested channel with retries, rate limiting and a dead-letter path.

Implements **Project 8: Notification Platform** from the *Distributed Systems Project Ideas*
design doc.

## Status

| Capability | State |
|---|---|
| REST entry point | Working |
| Kafka entry point | Working |
| Email delivery (SMTP) | Working — needs real SMTP credentials |
| SMS delivery | **Not sending.** Provider seam exists; the default client only logs |
| Templates + Redis cache | Working |
| Rate limiting (Redis) | Working |
| Retry + DLQ | Working |
| Push / WhatsApp | Not implemented (in the doc, out of current scope) |

## Architecture

```
Caller ──REST POST /api/v1/notifications ──┐
                                           ├──> NotificationService ──> MySQL (system of record)
Caller ──Kafka notification.requests ──────┘          │
                                                      └──> Kafka: notification.email
                                                                  notification.sms
                                                                       │
                                                     EmailWorker ──────┤────── SmsWorker
                                                          │                        │
                                                     JavaMailSender           SmsClient
                                                          │                        │
                                                          └── retry (exp. backoff) ─┘
                                                                       │
                                                       notification.email.DLT / .sms.DLT
```

Both entry points converge on `NotificationService.submit`, so idempotency and rendering behave
identically regardless of how a request arrived.

### Design decisions worth knowing

- **Kafka messages carry only the notification id**, not the recipient or rendered body. Keeps
  PII off the broker and guarantees workers act on current state.
- **Templates render at accept time**, not delivery time, so a bad template fails fast with a
  4xx instead of surfacing later as a mystery dead-letter.
- **`requestId` is a caller-owned idempotency key** with a unique index behind it. Resubmitting
  returns the original record instead of sending twice.
- **One topic per channel**, each with its own consumer group, so an SMS backlog cannot stall
  email.
- **Rate limiting lives in Redis**, not in-process — N replicas must share one provider budget.
  It **fails open**: a Redis outage degrades rate limiting rather than halting all delivery.
- **Recipients are masked in every log line and API response** (`Redaction`).

## Running locally

Requires **MySQL**, **Kafka** and **Redis** running locally. There is no Docker Compose file yet.

1. Copy the config template and fill in your values:
   ```bash
   cp application-local.properties.example application-local.properties
   ```
   This file is gitignored. Never commit credentials.

2. Start the app:
   ```bash
   ./gradlew bootRun
   ```

Tables are created automatically (`ddl-auto=update`). Kafka topics are created on startup.

3. Insert a template to send against:
   ```sql
   INSERT INTO notification_template (id, code, channel, subject, body, active)
   VALUES (UUID(), 'WELCOME', 'EMAIL', 'Welcome {{name}}',
           'Hi {{name}}, your account is ready.', true);
   ```

## API

### Submit a notification

`POST /api/v1/notifications` → **202 Accepted** (delivery is asynchronous)

```json
{
  "requestId": "order-4711-confirmation",
  "sourceService": "order-service",
  "channel": "EMAIL",
  "recipient": "jane@example.com",
  "templateCode": "WELCOME",
  "variables": { "name": "Jane" }
}
```

`requestId` must be stable for a given logical notification — it is the idempotency key.

### Check status

```
GET /api/v1/notifications/{notificationId}
GET /api/v1/notifications?requestId=order-4711-confirmation
```

Status is one of `ACCEPTED`, `QUEUED`, `SENT`, `FAILED`, `DEAD_LETTER`. The returned `recipient`
is masked.

### Kafka entry point

Publish an `InboundNotificationEvent` to `notification.requests` with the same fields as the
REST body. Set `requestId` — redelivery without it means duplicate sends.

## Configuration

Everything below is overridable by environment variable.

| Property | Default | Purpose |
|---|---|---|
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` | — | MySQL connection (required) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis |
| `MAIL_HOST` / `MAIL_PORT` | `localhost` / `1025` | SMTP |
| `NOTIFICATION_EMAIL_FROM` | `no-reply@example.com` | Sender address |
| `SMS_PROVIDER` | `log` | `log` does **not** send real SMS |
| `notification.rate-limit.email-per-minute` | `600` | Email budget |
| `notification.rate-limit.sms-per-minute` | `120` | SMS budget |
| `notification.retry.max-elapsed-time-ms` | `900000` | Retry window before DLT (15 min) |

## Adding a channel

1. Add the value to `Channel`.
2. Implement `ChannelSender` for it.
3. Add a topic in `KafkaTopics` + `KafkaConfig`, and a worker modelled on `SmsWorker`.

Nothing in the accept path changes.

## Known gaps

- **SMS does not reach a carrier.** `LoggingSmsClient` is active by default and only logs.
  Implement `SmsClient` against a real provider and set `SMS_PROVIDER`.
- **Security is HTTP Basic.** The design doc's common stack specifies JWT/OAuth2; do that before
  exposing this outside the cluster.
- **`ddl-auto=update`** is convenient but wrong for a shared service. Move to Flyway or Liquibase
  before this has real consumers.
- **No Docker Compose**, so local Kafka/Redis/MySQL are your responsibility.
- **No metrics beyond Actuator health.** Prometheus counters per channel and DLT alerting are
  the obvious next step.
- **Fixed-window rate limiting** tolerates up to 2x the limit across a window boundary. Fine for
  protecting a provider quota, not for billing-grade limits.

## Notes on Spring Boot 4

Boot 4 modularised auto-configuration and moved to Jackson 3. Two consequences that cost time
here and are easy to hit again:

- `spring-boot-starter-data-redis` alone provides **no** `CacheManager` — `spring-boot-starter-cache`
  is required as well.
- Bare `org.springframework.kafka:spring-kafka` provides **no** auto-configuration; use
  `spring-boot-starter-kafka`.
- Jackson is `tools.jackson.*`, and the Redis serializer is `GenericJacksonJsonRedisSerializer`
  (the `Jackson2` variants are the legacy Jackson-2 pair).

## Testing

```bash
./gradlew test
```

Unit tests cover template rendering, idempotency, delivery state transitions, rate-limit
deferral and PII masking. `contextLoads` needs a reachable MySQL.