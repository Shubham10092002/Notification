# Notification Platform

A shared, event-driven notification service. Every other service in the estate hands it a
notification instead of talking to email and SMS providers itself.

Implements **Project 8: Notification Platform** from the *Distributed Systems Project Ideas*
design doc (`src/Distributed Systems Project Ideas copy.pdf`).

---

## What problem it solves

Without a shared platform, every service that needs to notify a customer reimplements the same
things — provider SDKs, credentials, retry logic, template storage, rate limiting, "did it
actually send?" tracking. They implement them slightly differently, each one holds a copy of the
provider credentials, and nobody can answer "why didn't the customer get their OTP?" without
reading five sets of logs.

This service centralises that. A caller says *who*, *which template*, *which channel*, and gets an
immediate acknowledgement. Everything after that — rendering, queueing, sending, retrying,
throttling, recording the outcome — is the platform's problem.

### What it guarantees

| Guarantee | How |
|---|---|
| **Accepted means durable** | The row commits before anything is queued. A caller is never told "accepted" for something that then vanishes. |
| **Send once** | A caller-owned `requestId` with a unique index; workers re-check status before sending. |
| **Nothing silently lost** | Failed publishes, failed sends and exhausted retries all end in a queryable state, never a swallowed exception. |
| **One slow provider can't stop the rest** | Per-channel topics, consumer groups, rate limits and circuit breakers. |
| **PII stays contained** | Recipients are masked in logs and API responses, and never travel on Kafka. |

---

## Technologies

| Technology | Version | Why it's here |
|---|---|---|
| **Java** | 21 | Records for DTOs and events, switch expressions, virtual-thread-ready |
| **Spring Boot** | 4.0.7 | Application framework |
| **Spring Web MVC** | — | The REST entry point |
| **Spring Data JPA** + **Hibernate** | — | Persistence; the notification table is the system of record |
| **MySQL** | 8.x | Notification records and templates |
| **Apache Kafka** (Spring Kafka) | 4.0.6 | Async backbone: decouples accept from delivery, gives retry and DLQ |
| **Redis** (Spring Data Redis) | — | Template cache and cluster-wide rate limiter |
| **Spring Cache** | — | Declarative caching over Redis |
| **Resilience4j** | 2.3.0 (core) | Per-channel circuit breakers |
| **Spring Security** | — | HTTP Basic auth on the API |
| **Spring Mail** (JavaMail) | — | SMTP delivery |
| **Jakarta Bean Validation** | — | One constraint set, enforced on both entry points |
| **Lombok** | — | Boilerplate reduction |
| **Gradle** | 9.5.1 | Build |
| **JUnit 5**, **Mockito**, **AssertJ** | — | 66 tests |

> **Spring Boot 4 notes.** Boot 4 modularised auto-configuration and moved to Jackson 3. Three
> things that cost time here and are easy to hit again:
> `spring-boot-starter-data-redis` provides **no** `CacheManager` without `spring-boot-starter-cache`;
> bare `org.springframework.kafka:spring-kafka` provides **no** auto-configuration (use
> `spring-boot-starter-kafka`); Jackson is `tools.jackson.*` and the Redis serializer is
> `GenericJacksonJsonRedisSerializer`.

---

## How it works

```
                    ┌──────────────────────── accept ────────────────────────┐
                    │                                                        │
  REST  POST /api/v1/notifications ──┐                                       │
                                     ├──> NotificationService                │
  Kafka notification.requests ───────┘         │                             │
                                               ├─ dedupe on requestId        │
                                               ├─ validate (one rule set)    │
                                               ├─ render template (Redis)    │
                                               └─ INSERT status=ACCEPTED ────┤
                                                                             │
                    ┌──────────────── after commit only ─────────────────────┘
                    │
                    └─> publish to notification.email / notification.sms
                             │
                             │  broker ack ──> status=QUEUED
                             │
              ┌──────────────┴──────────────┐
        EmailWorker                    SmsWorker         (own consumer group each)
              │                             │
        rate limit (Redis)            rate limit (Redis)
              │                             │
        circuit breaker               circuit breaker
              │                             │
        JavaMailSender                 SmsClient
              │                             │
              └────── success → SENT ───────┘
                      failure → FAILED, retry with backoff
                      exhausted → <topic>.DLT + status=DEAD_LETTER
```

### The accept path

Both entry points converge on `NotificationService.submit`, so idempotency, validation and
rendering behave identically no matter how a request arrived.

1. **Deduplicate** on `requestId`. A repeat returns the original record — no second send.
2. **Validate.** Constraints live on `SendNotificationCommand` and are enforced by method
   validation, so the REST body and the Kafka event are held to the same rules by construction.
3. **Render** the template now, not at delivery time — a bad template fails fast with a 422
   instead of surfacing later as a mystery dead-letter.
4. **Insert** as `ACCEPTED` and return **202**.
5. **Publish after commit.** Only a confirmed broker ack promotes the row to `QUEUED`.

### The delivery path

Each channel has its own topic and consumer group. A worker re-reads the row (so it acts on
current state, and PII never rides on Kafka), checks it isn't already `SENT`, takes a rate-limit
permit, and calls the provider through a circuit breaker.

### The outbox, without an outbox table

The `notification` table **is** the outbox:

- committed first as `ACCEPTED`;
- published after commit, promoted to `QUEUED` only on a broker ack;
- anything still `ACCEPTED` after 60s is a publish that never landed, and
  `StuckNotificationSweeper` republishes it.

A broker outage therefore degrades to a delayed send, not a lost notification — no second table,
no CDC pipeline.

---

## Features, and where each lives

| Feature | Implementation | How it's used |
|---|---|---|
| **REST entry point** | `api/NotificationController` | `POST /api/v1/notifications` |
| **Event entry point** | `messaging/InboundNotificationConsumer` | Publish to `notification.requests` |
| **Idempotency** | `requestId` unique index + `isAlreadyDelivered()` | Caller supplies a stable key |
| **Templates** | `template/TemplateService` | `{{placeholder}}` substitution |
| **Template cache** | `template/TemplateLookup` (`@Cacheable`) | Redis, 30-min TTL, caches `TemplateView` not the entity |
| **Channel strategy** | `channel/ChannelSender` | One implementation per channel |
| **Email delivery** | `channel/EmailChannelSender` | SMTP with bounded timeouts |
| **SMS delivery** | `channel/SmsClient` (+ `LoggingSmsClient`) | Provider seam — **not sending yet** |
| **Rate limiting** | `ratelimit/RedisRateLimiter` | Fixed window, shared cluster-wide, fails open |
| **Circuit breaker** | `channel/CircuitBreakingChannelSender` | Applied by `ChannelSenderRegistry`, one per channel |
| **Retry + DLQ** | `config/KafkaConfig` | Exponential backoff → `<topic>.DLT` |
| **Dead-letter recovery** | `POST /{id}/replay` | Operator action after fixing the provider |
| **Stuck-publish recovery** | `messaging/StuckNotificationSweeper` | Scheduled sweep every 30s |
| **PII redaction** | `support/Redaction` | Logs and API responses |
| **Error mapping** | `api/ApiExceptionHandler` | RFC 7807 problem responses |

### Package layout

```
com.common.Notification
├── api/            REST controller, DTOs, exception handling
├── channel/        ChannelSender strategy, senders, circuit breaker, registry
├── config/         Kafka, cache, security, resilience, async, scheduling
├── domain/         Entities, enums, repositories, SendNotificationCommand
│   └── event/      NotificationAcceptedEvent (neutral, keeps packages acyclic)
├── exception/      Domain exceptions (retryable vs not)
├── messaging/      Producer, dispatcher, workers, DLT listeners, sweeper
├── ratelimit/      Redis rate limiter
├── service/        Accept and delivery orchestration, state writers
├── support/        Redaction
└── template/       Template lookup, rendering, cached view
```

---

## API

All endpoints require HTTP Basic auth. `/actuator/health/**` is open.

### Submit — `POST /api/v1/notifications` → **202 Accepted**

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

`requestId` must be **stable** for a given logical notification — it is the idempotency key.
202 means durably accepted, not delivered.

### Check status

```
GET /api/v1/notifications/{notificationId}
GET /api/v1/notifications?requestId=order-4711-confirmation
```

Status is one of `ACCEPTED` → `QUEUED` → `SENT`, or `FAILED` / `DEAD_LETTER`. The returned
`recipient` is masked.

### Replay a dead-lettered notification

```
POST /api/v1/notifications/{notificationId}/replay
```

204 on success, 409 if not in `DEAD_LETTER`, 404 if unknown. Idempotent.

### Kafka entry point

Publish to `notification.requests`:

```json
{
  "schemaVersion": 1,
  "requestId": "order-4711-confirmation",
  "sourceService": "order-service",
  "channel": "EMAIL",
  "recipient": "jane@example.com",
  "templateCode": "WELCOME",
  "variables": { "name": "Jane" }
}
```

Plain JSON — no Java type headers — so publishers in any language work. An unknown
`schemaVersion` or a constraint failure is non-retryable and goes to `notification.requests.DLT`.

### Error responses

| Status | When |
|---|---|
| 400 | Validation failure or malformed JSON |
| 401 | Missing or invalid credentials |
| 404 | Unknown notification |
| 409 | Conflicting request, or replay of a non-dead-lettered notification |
| 422 | Unknown template |
| 429 | Rate limit exceeded |
| 500 | Unexpected — returns a correlation `errorId`, never internals |

---

## Failure handling

| Failure | Behaviour |
|---|---|
| **Kafka down at accept** | Row stays `ACCEPTED`; the sweeper republishes it |
| **Crash between commit and publish** | Same — sweeper recovers it |
| **Provider hangs** | SMTP timeouts cap an attempt at ~25s (JavaMail defaults to infinite) |
| **Provider transient failure** | Recorded as `FAILED`, retried with exponential backoff |
| **Provider failing repeatedly** | Breaker opens at 50% failures over 20 calls, fails fast for 30s |
| **Provider down past the retry window** | `DEAD_LETTER` + DLT; recover with replay |
| **Redis down** | Rate limiter fails open, cache falls through to the DB — delivery continues |
| **Rate limited** | Throws before counting an attempt; redelivered when the window rolls |
| **Duplicate submission** | `requestId` lookup, or the unique index for a true race |
| **Kafka redelivers a sent message** | Worker skips anything already `SENT` |
| **Invalid inbound event** | Non-retryable → `notification.requests.DLT`, logged loudly |
| **Unroutable channel** | Dead-lettered immediately — waiting cannot register a sender |
| **Missing template** | Non-retryable, 422 at accept time |
| **DB unreachable** | Accept fails outright; the record *is* the outbox, so it is not degraded |

---

## Running locally

**Prerequisites:** JDK 21, MySQL, Kafka, Redis. There is no Docker Compose file yet.

```bash
cp application-local.properties.example application-local.properties
# fill in DB_URL, DB_USERNAME, DB_PASSWORD, NOTIFICATION_API_PASSWORD
./gradlew bootRun
```

`application-local.properties` is gitignored — never commit credentials. Startup **fails** if
`NOTIFICATION_API_PASSWORD` is unset; a service that can email and text customers must not be
left open by accident.

Tables and Kafka topics are created on startup. Add a template:

```sql
INSERT INTO notification_template (id, code, channel, subject, body, active)
VALUES (UUID(), 'WELCOME', 'EMAIL', 'Welcome {{name}}',
        'Hi {{name}}, your account is ready.', true);
```

Send one:

```bash
curl -u notification-client:<password> -X POST http://localhost:8085/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-1","sourceService":"manual","channel":"EMAIL",
       "recipient":"jane@example.com","templateCode":"WELCOME","variables":{"name":"Jane"}}'
```

```bash
./gradlew test    # 66 tests; contextLoads needs a reachable MySQL
```

---

## Configuration

Everything is environment-overridable.

| Property | Default | Purpose |
|---|---|---|
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | — | MySQL (required) |
| `NOTIFICATION_API_USER` / `NOTIFICATION_API_PASSWORD` | `notification-client` / — | HTTP Basic; **startup fails without the password** |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers |
| `KAFKA_LISTENER_CONCURRENCY` | `3` | Consumer threads per listener |
| `KAFKA_TOPIC_PARTITIONS` | `3` | Partitions per channel topic |
| `KAFKA_TOPIC_REPLICAS` / `KAFKA_MIN_ISR` | `1` / `1` | **Local defaults.** Production needs `3` / `2`, or `acks=all` is meaningless |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis |
| `MAIL_HOST` / `MAIL_PORT` | `localhost` / `1025` | SMTP |
| `MAIL_CONNECT_TIMEOUT_MS` / `MAIL_READ_TIMEOUT_MS` / `MAIL_WRITE_TIMEOUT_MS` | `5000` / `10000` / `10000` | JavaMail defaults to **infinite** — do not remove |
| `NOTIFICATION_EMAIL_FROM` | `no-reply@example.com` | Sender address |
| `SMS_PROVIDER` | `log` | `log` does **not** send real SMS |
| `notification.rate-limit.channels.<CHANNEL>.permits` / `.window` | 600/1m, 120/1m | Per-channel budgets |
| `notification.retry.max-elapsed-time-ms` | `120000` | Retry window before DLT |
| `notification.circuit-breaker.*` | 50% / 30s | Breaker thresholds |
| `notification.sweeper.stuck-after-seconds` | `60` | Age at which `ACCEPTED` is republished |

---

## Adding a channel

The design doc lists Push and WhatsApp; neither is implemented. To add one:

1. Add the value to `domain/Channel`.
2. Implement `channel/ChannelSender`.
3. Add a worker modelled on `messaging/SmsWorker`.
4. Add `notification.rate-limit.channels.<CHANNEL>.permits` / `.window`.

Topics are derived from the registered senders and created automatically, and the circuit breaker
is applied by the registry. Nothing in the accept path changes. Miss step 4 and the channel falls
back to a conservative default rather than being blocked.

---

## Known gaps

- **SMS does not reach a carrier.** `LoggingSmsClient` only logs. Implement `SmsClient` against a
  real provider and set `SMS_PROVIDER`.
- **Email is plain text** — no HTML, no attachments.
- **Security is HTTP Basic** with one shared credential; no per-service identity or rotation. The
  doc's stack specifies JWT/OAuth2.
- **`ddl-auto=update`** — move to Flyway or Liquibase before this has real consumers.
- **Every replica runs the sweeper.** Safe, since delivery is idempotent, but it duplicates broker
  traffic during an outage. A Redis distributed lock would elect one.
- **Retries block the partition** during backoff. Non-blocking retry topics (`@RetryableTopic`)
  are the scalable fix; the short window plus DLT replay is the current mitigation.
- **Rate limiting is per channel only** — no per-recipient or per-source limit.
- **No template output escaping.** Fine for plain text; must be fixed before HTML email.
- **No Docker Compose**, and **no metrics beyond Actuator health**.
- **Push / WhatsApp** not implemented.

A full architecture review — 20 findings with implementations, trade-offs and verdicts, of which
1–12 are fixed — is in [`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md).
