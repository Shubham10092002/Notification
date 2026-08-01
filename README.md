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
| Circuit breaker (per channel) | Working |
| DLT replay endpoint | Working |
| Stuck-publish recovery sweeper | Working |
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

## Failure handling

Every failure mode and what the platform does about it:

| Failure | Behaviour |
|---|---|
| **Kafka down at accept time** | Row commits as `ACCEPTED`; publish is attempted *after* commit and only a broker ack promotes it to `QUEUED`. `StuckNotificationSweeper` republishes anything left in `ACCEPTED`. The caller is never told "accepted" for something that silently vanished. |
| **App crashes between commit and publish** | Same path — the row is `ACCEPTED`, the sweeper picks it up. |
| **Provider (SMTP/SMS) transient failure** | `markFailed` commits in its own transaction, then the exception propagates so Kafka retries with exponential backoff. |
| **Provider hangs (no response)** | SMTP connect/read/write timeouts cap an attempt at ~25s. Without them JavaMail waits forever and parks a consumer thread permanently. |
| **Provider failing repeatedly** | The per-channel circuit breaker opens after a 50% failure rate over 20 calls, then fails fast for 30s instead of spending a timeout per message. Rejections stay retryable, and each channel has its own breaker so SMS cannot stop email. |
| **Invalid inbound event** | Validated at the Kafka boundary with the same constraints as the REST body. Rejected as non-retryable straight to `notification.requests.DLT`. |
| **Provider down beyond the retry window** | Dead-lettered to `<topic>.DLT` and the row becomes `DEAD_LETTER`. Recover with `POST /{id}/replay` once the provider is healthy. |
| **Redis down** | Rate limiter **fails open** and template lookups fall through to the database. Delivery continues. |
| **Rate limit hit** | Throws before counting an attempt, so Kafka redelivers after the window rolls. Rejections do not increment the counter, so a throttled channel recovers. |
| **Duplicate submission (caller retry)** | Resolved by `requestId` lookup; returns the original record. |
| **Duplicate submission (true race)** | Unique index fires; the loser re-queries outside the failed transaction and returns the winning row. |
| **Kafka redelivers an already-sent message** | Worker re-reads the row and skips anything already `SENT`. |
| **Malformed event from another service** | Fails conversion, retries, then lands on `notification.requests.DLT` where `InboundRequestDltListener` logs it loudly. There is no row to mark — acceptance never happened. |
| **Unknown/unroutable channel** | Dead-lettered immediately rather than retried; waiting cannot register a sender. |
| **Missing template** | Non-retryable, fails fast with 422 at accept time. |
| **DB unreachable** | Accept fails with an error to the caller; nothing is queued. Deliberately not degraded — the record *is* the outbox. |

## Scalability notes

- `spring.kafka.listener.concurrency` (default 3) matches partition count; raise both together.
- Kafka send callbacks run on a dedicated executor, never the producer's I/O thread — a database
  write there would serialise every publish in the JVM behind JDBC latency.
- Provider calls are bounded by SMTP timeouts and guarded by a circuit breaker, so an outage
  cannot consume consumer threads on calls that are certain to fail.
- Provider calls happen **outside** any database transaction, so a slow SMTP server cannot pin
  connections from the Hikari pool.
- State transitions are short `REQUIRES_NEW` transactions, not long-lived ones.
- Per-channel topics and consumer groups scale and fail independently.
- Rate limiting is centralised in Redis, so adding replicas does not multiply provider load.
- Retry backoff is capped at 2 minutes, comfortably under Kafka's 5-minute
  `max.poll.interval.ms`, to bound head-of-line blocking on a partition.

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

### Replay a dead-lettered notification

```
POST /api/v1/notifications/{notificationId}/replay
```

204 on success, 409 if the notification is not in `DEAD_LETTER`, 404 if unknown. Idempotent.

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
| `notification.retry.max-elapsed-time-ms` | `120000` | Retry window before DLT (2 min) |
| `MAIL_CONNECT_TIMEOUT_MS` / `MAIL_READ_TIMEOUT_MS` / `MAIL_WRITE_TIMEOUT_MS` | `5000` / `10000` / `10000` | SMTP timeouts — **JavaMail defaults to infinite** |
| `notification.circuit-breaker.failure-rate-threshold` | `50` | % failures over the sliding window before opening |
| `notification.circuit-breaker.open-seconds` | `30` | How long the breaker stays open before probing |
| `NOTIFICATION_API_USER` / `NOTIFICATION_API_PASSWORD` | — | HTTP Basic credential; **startup fails without the password** |
| `KAFKA_LISTENER_CONCURRENCY` | `3` | Consumer threads per listener |
| `notification.sweeper.stuck-after-seconds` | `60` | Age at which an `ACCEPTED` row is republished |

## Adding a channel

1. Add the value to `Channel`.
2. Implement `ChannelSender` for it.
3. Add a topic in `KafkaTopics` + `KafkaConfig`, and a worker modelled on `SmsWorker`.

Nothing in the accept path changes.

## Known gaps

- **SMS does not reach a carrier.** `LoggingSmsClient` is active by default and only logs.
  Implement `SmsClient` against a real provider and set `SMS_PROVIDER`.
- **Security is HTTP Basic.** A single configured credential, shared by all callers — no
  per-service identity and no rotation. The design doc's common stack specifies JWT/OAuth2; do
  that before exposing this outside the cluster.
- **`ddl-auto=update`** is convenient but wrong for a shared service. Move to Flyway or Liquibase
  before this has real consumers.
- **Email is plain text.** `SimpleMailMessage` sends no HTML and no attachments.
- **Every replica runs the sweeper.** Safe (delivery is idempotent) but duplicates broker traffic
  during an outage. A Redis distributed lock would elect one sweeper.
- **Retries are blocking.** `DefaultErrorHandler` pauses the partition during backoff, so one
  slow message delays those behind it. Non-blocking retry topics (`@RetryableTopic`) are the
  scalable fix; the short retry window plus DLT replay is the current mitigation.
- **Rate limiting is per channel only** — no per-recipient or per-source limit, so one noisy
  caller can consume the whole channel budget.
- **Fixed-window rate limiting** with a non-atomic check-then-increment can overshoot slightly
  under concurrency. A Lua script would make it exact.
- **No Docker Compose**, so local Kafka/Redis/MySQL are your responsibility.
- **No metrics beyond Actuator health.** Prometheus counters per channel and DLT alerting are
  the obvious next step.
- **DLT replay is one notification at a time.** A bulk replay endpoint is missing.

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