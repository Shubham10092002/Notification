# Architecture Review — Notification Platform

Reviewed at commit `8a5bf16`. 44 source files, ~1,900 lines.

> **Status:** findings 1–9, 11 and 12 are **fixed**. Finding 10 is **partially done** — the
> projection type exists but the repository and sweeper still use full entities. Findings 13–20
> remain open.

Findings are ranked by what they cost you in production, not by how interesting they are.
Dimensions where the code is already sound are listed at the end rather than padded with
invented problems.

| # | Finding | Dimension | Severity | Worth doing? |
|---|---|---|---|---|
| 1 | ✅ No SMTP timeouts — a hung provider blocks a consumer thread forever | Performance, Concurrency | **Critical** | Yes, immediately |
| 2 | ✅ Database write on the Kafka producer I/O thread | Async, Performance | **High** | Yes |
| 3 | ✅ Kafka entry point bypasses all validation | Validation | **High** | Yes |
| 4 | ✅ No Circuit Breaker on provider calls | Circuit Breaker | **High** | Yes |
| 5 | ✅ Exception handling gaps — unmapped exceptions become 500s | Exception handling | **High** | Yes |
| 6 | ✅ Circular package dependency `service` ↔ `messaging` | Circular deps | Medium | Yes |
| 7 | ✅ Service and messaging layers depend on a web DTO | Coupling, Package structure | Medium | Yes |
| 8 | ✅ Adding a channel requires editing 5 files | SOLID (OCP) | Medium | Yes |
| 9 | ✅ A JPA entity is cached in Redis | Redis strategy | Medium | Yes |
| 10 | ⚠️ Sweeper loads `@Lob` bodies to read two fields | Memory | Medium | Yes |
| 11 | ✅ `replicas(1)` hardcoded in topic definitions | Kafka design | Medium | Yes |
| 12 | ✅ No schema version on the public inbound event | EDA | Medium | Yes |
| 13 | 7-parameter static factory invites transposed arguments | Builder | Low | Judgement call |
| 14 | No outbound delivery-outcome events | EDA opportunity | Low | Yes, when asked for |
| 15 | Field injection in the sweeper | DI | Low | Yes, trivial |
| 16 | Scheduler thread pool of 1 | Scheduler | Low | Yes, trivial |
| 17 | `ack-mode=record` commits per record | Kafka design | Low | Only under load |
| 18 | No output escaping in template rendering | Security | Low now, High with HTML | Yes, before HTML email |
| 19 | No rate limiting on the REST API itself | Security | Low | Judgement call |
| 20 | Layer-based packaging with no enforcement | Package structure | Low | No, not at this size |

---

## 1. ✅ FIXED — No SMTP timeouts — a hung provider blocks a consumer thread forever

**Problem.** `spring.mail.*` sets host, port and credentials but no timeouts. JavaMail's defaults
are infinite. `EmailChannelSender.send` therefore has no upper bound on how long it can block.

**Architectural impact.** This is the single worst failure mode in the system. Delivery runs on
Kafka consumer threads (`concurrency=3`). An SMTP server that accepts the TCP connection and then
stops responding — a firewall blackhole, an overloaded relay — parks a consumer thread
indefinitely. Three such messages and email delivery stops permanently. The container stops
polling, the group rebalances, and the partition is reassigned to another instance which hangs on
the same message. Retry, DLQ and the circuit breaker are all downstream of a call that never
returns, so none of them ever fire. A bounded 2-minute retry window means nothing when a single
attempt can hang for hours.

**Why this recommendation.** Timeouts are the precondition for every other resilience mechanism.
A circuit breaker cannot count failures that never complete.

**Implementation** — `application.properties`:

```properties
## Mail (SMTP)
# Infinite by default in JavaMail. Without these, a blackholed SMTP server parks a Kafka
# consumer thread permanently and no retry or breaker ever fires.
spring.mail.properties.mail.smtp.connectiontimeout=${MAIL_CONNECT_TIMEOUT_MS:5000}
spring.mail.properties.mail.smtp.timeout=${MAIL_READ_TIMEOUT_MS:10000}
spring.mail.properties.mail.smtp.writetimeout=${MAIL_WRITE_TIMEOUT_MS:10000}
```

Total worst case per attempt becomes ~25s, comfortably inside the 2-minute retry budget.

**Pros.** Three lines. Converts an unbounded hang into a normal retryable failure. No API change.
**Cons.** A genuinely slow-but-working relay may now time out; tune per provider.

**Verdict. Do this before anything else in this document.** Highest value-to-effort ratio in the
codebase.

---

## 2. ✅ FIXED — Database write on the Kafka producer I/O thread

**Problem.** `NotificationDispatcher.dispatch` (line 44) attaches `whenComplete` to the send
future. Without an executor, that callback runs on the Kafka producer's single I/O thread, and it
performs a database write (`stateWriter.markQueued`) — a `REQUIRES_NEW` transaction: acquire a
pooled connection, begin, update, commit.

```java
eventProducer.publish(notificationId, channel)
        .whenComplete((result, throwable) -> {
            ...
            stateWriter.markQueued(notificationId);   // JDBC on the Kafka sender thread
        });
```

**Architectural impact.** One producer instance has one I/O thread serving every send in the JVM.
Blocking it on JDBC serialises all Kafka publishing behind database latency. Throughput ceiling
becomes roughly `1 / db_write_latency` — a few hundred per second at best, and if the connection
pool is saturated the thread blocks on pool acquisition while broker acknowledgements queue behind
it. This silently caps the accept path no matter how many partitions or replicas you add. Worse,
an exception thrown inside the callback can destabilise the producer's I/O loop.

**Why this recommendation.** Never do blocking I/O on a framework's event-loop thread. Hand off
to a bounded pool sized for the database, not the broker.

**Implementation** — a dedicated executor:

```java
package com.common.Notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class AsyncConfig {

    /**
     * Runs Kafka send callbacks off the producer's I/O thread.
     *
     * <p>The callback writes to the database; doing that inline would serialise every publish in
     * the JVM behind JDBC latency and can destabilise the producer's event loop.
     *
     * <p>CallerRunsPolicy on saturation deliberately pushes back on the producer rather than
     * discarding a status update — a dropped markQueued leaves a row the sweeper will republish,
     * which is correct but wasteful.
     */
    @Bean("kafkaCallbackExecutor")
    ThreadPoolTaskExecutor kafkaCallbackExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("kafka-cb-");
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(1000);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }
}
```

```java
// NotificationDispatcher
private final Executor kafkaCallbackExecutor;   // @Qualifier("kafkaCallbackExecutor")

public void dispatch(String notificationId, Channel channel) {
    eventProducer.publish(notificationId, channel)
            .whenCompleteAsync((result, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to publish notificationId={} to {}; left ACCEPTED for retry",
                            notificationId, KafkaTopics.forChannel(channel), throwable);
                    return;
                }
                stateWriter.markQueued(notificationId);
            }, kafkaCallbackExecutor);
}
```

**Pros.** Removes a hard throughput ceiling. Isolates producer health from database health.
**Cons.** One more pool to size and monitor. Status updates become marginally more delayed.

**Verdict. Yes.** This is the difference between a service that scales with partitions and one
that does not.

---

## 3. ✅ FIXED — The Kafka entry point bypasses all validation

**Problem.** `NotificationRequest` carries `@NotBlank`, `@NotNull` and `@Size` constraints, and
the controller applies them with `@Valid`. `InboundNotificationConsumer` constructs the same
record directly and calls `submit` — no validator ever runs.

**Architectural impact.** The two documented entry points enforce different contracts. An event
with a null `requestId` destroys idempotency (`findByRequestId(null)` matches nothing, so every
redelivery inserts a new row and sends again). A 500-character recipient throws a database
truncation error at insert instead of a clean rejection. The failure surfaces deep in persistence
rather than at the boundary, and the resulting exception is not a validation error, so the error
handler classifies it as retryable and it cycles until it dead-letters. "Both entry points
converge on the same accept path" is only true *after* the point where validation should have
happened.

**Why this recommendation.** Validation belongs at every boundary, not at the HTTP boundary.
Programmatic validation in the listener keeps one set of constraints on one class.

**Implementation:**

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class InboundNotificationConsumer {

    private final NotificationService notificationService;
    private final Validator validator;   // jakarta.validation.Validator, auto-configured

    @KafkaListener(topics = KafkaTopics.INBOUND_REQUESTS, groupId = "notification-inbound")
    public void onInboundRequest(InboundNotificationEvent event) {
        NotificationRequest request = new NotificationRequest(
                event.requestId(), event.sourceService(), event.channel(),
                event.recipient(), event.templateCode(), event.variables());

        Set<ConstraintViolation<NotificationRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .collect(Collectors.joining("; "));
            // Non-retryable: the payload will not become valid on redelivery. Throwing a type
            // registered as non-retryable sends it straight to the DLT.
            throw new InvalidNotificationEventException(detail);
        }

        notificationService.submit(request);
    }
}
```

Register it as non-retryable so it dead-letters on the first attempt:

```java
errorHandler.addNotRetryableExceptions(
        TemplateNotFoundException.class,
        InvalidNotificationEventException.class);
```

**Pros.** One constraint definition, both boundaries. Bad events fail fast and land on the DLT
where `InboundRequestDltListener` already logs them.
**Cons.** Constructing the DTO purely to validate it is slightly awkward — resolved properly by
recommendation 7, where both boundaries map to a domain command that is validated once.

**Verdict. Yes.** A null `requestId` silently breaking idempotency is a customer-visible bug —
duplicate emails and texts.

---

## 4. ✅ FIXED — No Circuit Breaker on provider calls

**Problem.** The design doc lists Circuit Breaker as a required concept. There is none. Every
delivery attempt calls the provider regardless of how many just failed.

**Architectural impact.** During a provider outage the platform behaves at its worst. Each message
attempts, waits for the timeout, backs off, retries — occupying consumer threads doing work
guaranteed to fail. With a 2-minute retry window and a 10-second timeout, one message can consume
~12 pointless provider calls. Multiply by a backlog and you generate a thundering herd against a
service that is already unhealthy, delay its recovery, and stall the partition for everything
behind it — including messages for a *different*, healthy provider, since email and SMS share
neither a breaker nor an isolation boundary today.

**Why this recommendation.** A breaker converts "wait for the timeout, then fail" into "fail
immediately, and periodically probe". Failing fast means the message dead-letters quickly and the
partition keeps moving; replay handles recovery.

**Implementation.** Resilience4j, wrapped at the `ChannelSender` boundary so it applies to every
channel uniformly:

```java
package com.common.Notification.channel;

/**
 * Wraps a ChannelSender in a per-channel circuit breaker.
 *
 * <p>Decorating rather than annotating keeps resilience policy out of the individual senders and
 * guarantees a new channel cannot forget to apply it.
 */
public class CircuitBreakingChannelSender implements ChannelSender {

    private final ChannelSender delegate;
    private final CircuitBreaker circuitBreaker;

    @Override
    public Channel channel() {
        return delegate.channel();
    }

    @Override
    public void send(NotificationRecord record) {
        try {
            circuitBreaker.executeRunnable(() -> delegate.send(record));
        } catch (CallNotPermittedException ex) {
            // Breaker open: fail fast, still retryable so the message is redelivered rather
            // than dead-lettered on the first rejection.
            throw new NotificationDeliveryException(
                    "Circuit open for channel " + delegate.channel(), ex);
        }
    }
}
```

Registered per channel in configuration, with each channel getting its own breaker instance so an
SMS outage cannot open the email breaker. Suggested settings: sliding window 20 calls, 50%
failure threshold, 30s open duration, 3 permitted calls half-open.

**Resolved:** `io.github.resilience4j:resilience4j-circuitbreaker:2.3.0` (core, no Spring
integration) resolves cleanly on Boot 4, so the starter's Boot 3 targeting is a non-issue. The
implementation uses the core library with a hand-built `CircuitBreakerRegistry` bean.

**Pros.** Bounded damage during outages. Faster recovery for the provider. Per-channel isolation.
**Cons.** A new dependency with a compatibility question. Adds a tuning surface — a badly
configured breaker can open on normal error rates and dead-letter healthy traffic.

**Verdict. Yes**, but do recommendation 1 first — timeouts are what make failures countable.

---

## 5. ✅ FIXED — Exception handling gaps

**Problem.** `ApiExceptionHandler` maps exactly two exception types. Everything else falls through
to Spring's default handling.

**Architectural impact.** `DataIntegrityViolationException` from the unresolvable-duplicate path,
`RateLimitExceededException`, and any unexpected runtime exception all become a generic 500. Three
consequences: callers cannot distinguish "retry me" from "your request is wrong", so client retry
logic is guesswork; a 500 is the wrong signal for a client error and pollutes error-rate alerting;
and default error responses can echo exception messages, which for a data-integrity failure may
include column values — recipients are PII, and the codebase is otherwise careful to mask them
(`Redaction`). The error path quietly undoes that discipline.

**Implementation:**

```java
@ExceptionHandler(RateLimitExceededException.class)
ProblemDetail onRateLimited(RateLimitExceededException ex) {
    ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.TOO_MANY_REQUESTS);
    problem.setTitle("Rate limit exceeded");
    problem.setDetail("Notification channel is over its send budget. Retry shortly.");
    return problem;
}

@ExceptionHandler(DataIntegrityViolationException.class)
ProblemDetail onDataIntegrityViolation(DataIntegrityViolationException ex) {
    // Logged in full, never returned: the message can contain column values, and recipients
    // are PII.
    log.warn("Data integrity violation handling notification request", ex);
    ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.CONFLICT);
    problem.setTitle("Conflicting notification request");
    problem.setDetail("The request conflicts with an existing notification.");
    return problem;
}

@ExceptionHandler(Exception.class)
ProblemDetail onUnexpected(Exception ex) {
    String errorId = UUID.randomUUID().toString();
    log.error("Unhandled exception errorId={}", errorId, ex);
    ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
    problem.setTitle("Internal error");
    // Correlation id instead of internals — supportable without leaking anything.
    problem.setDetail("Unexpected error. Quote errorId " + errorId + " to support.");
    return problem;
}
```

**Pros.** Correct status codes, no information leakage,supportable errors via correlation id.
**Cons.** A catch-all handler can mask exceptions you would rather see fail loudly — mitigated by
logging at ERROR with the stack trace.

**Verdict. Yes.** Small, and it closes a PII leak on the error path.

---

## 6. ✅ FIXED — Circular package dependency: `service` ↔ `messaging`

**Problem.** Measured import graph:

```
service    -> api.dto channel domain exception messaging ratelimit support template
messaging  -> api.dto domain service
api        -> api.dto domain exception service support
```

`service` → `messaging` (`NotificationService` uses `NotificationDispatcher`) and `messaging` →
`service` (`DeliveryService`, `NotificationStateWriter`, `NotificationAcceptedEvent`). A cycle.

**Architectural impact.** Neither package can be understood, tested or extracted independently.
When the doc's "separate microservices" option comes back — and for a platform meant to serve
every other service, it will — this cycle is the thing that blocks it: there is no cut point.
Cycles also make dependency direction unenforceable, so the coupling worsens silently over time.

**Why this recommendation.** The cycle exists only because `NotificationService.replay` calls the
dispatcher directly. The codebase already has the right mechanism for this — `NotificationWriter`
publishes `NotificationAcceptedEvent` and the dispatcher listens. Replay should use the same path
instead of inventing a second one.

**Implementation.** Move the event types to a neutral package (`domain.event`), then have replay
publish rather than call:

```java
// NotificationService — no dispatcher dependency
private final ApplicationEventPublisher eventPublisher;

public boolean replay(String notificationId) {
    NotificationRecord record = notificationRepository.findById(notificationId).orElse(null);
    if (record == null || !stateWriter.resetForReplay(notificationId)) {
        return false;
    }
    // Same route as a fresh acceptance — one dispatch path, no dependency on messaging.
    eventPublisher.publishEvent(new NotificationAcceptedEvent(notificationId, record.getChannel()));
    log.info("Replayed dead-lettered notificationId={}", notificationId);
    return true;
}
```

Resulting direction: `api → service → domain ← messaging`. Acyclic.

Add an ArchUnit test so it stays that way:

```java
@Test
void packagesAreAcyclic() {
    slices().matching("com.common.Notification.(*)..").should().beFreeOfCycles()
            .check(new ClassFileImporter().importPackages("com.common.Notification"));
}
```

**Pros.** Acyclic graph, one dispatch path instead of two, replay automatically inherits any
future dispatch logic. Slightly simpler `NotificationService`.
**Cons.** Indirection — the link from replay to publish is no longer a call you can follow in an
IDE. ArchUnit is another test dependency.

**Verdict. Yes.** It removes a dependency rather than adding a layer, and it deletes duplicate
logic in the process.

---

## 7. ✅ FIXED — Service and messaging layers depend on a web DTO

**Problem.** `NotificationService.submit` takes `com.common.Notification.api.dto.NotificationRequest`.
So does `InboundNotificationConsumer` — a Kafka listener importing a web DTO to call a service.

**Architectural impact.** The dependency arrow points inward from the delivery mechanism to the
core, which is backwards. Concretely: adding a field for one HTTP client changes the type the Kafka
consumer must construct; adding a Jackson annotation for JSON shaping changes a type the business
layer depends on; and the HTTP request contract cannot evolve independently of the event contract
even though they are consumed by different parties on different schedules. It is also the root
cause of the awkwardness in recommendation 3 — the consumer builds a *web request object* purely
to satisfy a validator.

**Implementation.** A domain command both boundaries map to, validated once:

```java
package com.common.Notification.domain;

/**
 * The platform's internal contract for "send this notification".
 *
 * <p>Both entry points map to this, so the HTTP body and the Kafka event schema can evolve
 * independently of the business logic and of each other.
 */
public record SendNotificationCommand(
        @NotBlank @Size(max = 128) String requestId,
        @Size(max = 64) String sourceService,
        @NotNull Channel channel,
        @NotBlank @Size(max = 320) String recipient,
        @NotBlank @Size(max = 64) String templateCode,
        Map<String, Object> variables) {
}
```

`NotificationRequest` and `InboundNotificationEvent` each gain a `toCommand()`; `NotificationService.submit`
takes `SendNotificationCommand`. Validation moves onto the command, so both boundaries enforce
identical rules by construction rather than by discipline.

**Pros.** Correct dependency direction; independent evolution of HTTP and event contracts; solves
recommendation 3 cleanly; `service` and `messaging` stop importing `api`.
**Cons.** A third representation of nearly the same data, and two mapping functions. At this size
that is real duplication for a payoff that is mostly future-facing.

**Verdict. Yes**, and do it together with recommendation 3 — separately they are each marginal;
together they fix the validation gap *and* the layering in one change.

---

## 8. ✅ FIXED — Adding a channel requires editing five files (OCP violation)

**Problem.** The README claims adding a channel is: add the enum value, implement `ChannelSender`,
add a worker. Actually required:

1. `Channel` — new enum constant
2. `KafkaTopics.forChannel` — new `switch` branch (compile error otherwise)
3. `KafkaConfig` — two new `NewTopic` beans
4. `ChannelRateLimits.forChannel` — new `switch` branch, plus a new field and getter/setter pair
5. New `ChannelSender` implementation
6. New worker class

**Architectural impact.** The Strategy pattern is applied to *sending* but abandoned for
everything around it. The two `switch` statements are the classic OCP violation: exhaustive
switches over a domain enum scattered across packages, so behaviour lives in `switch` arms instead
of in the abstraction. Since the doc lists Push and WhatsApp as in-scope, this is not hypothetical.
The compile errors do at least make omissions loud — the switches are exhaustive — but `ChannelRateLimits`
silently needs a matching properties key, and forgetting it yields a zero limit that blocks the
channel entirely.

**Implementation.** Push the per-channel knowledge into the strategy and into configuration:

```java
public interface ChannelSender {
    Channel channel();
    void send(NotificationRecord record);

    /** Topic this channel consumes. Derived, so a new channel cannot forget to register one. */
    default String topic() {
        return "notification." + channel().name().toLowerCase(Locale.ROOT);
    }
}
```

```java
@Component
@ConfigurationProperties(prefix = "notification.rate-limit")
public class ChannelRateLimits {

    /** Keyed by channel, so a new channel needs a properties entry, not a code change. */
    private Map<Channel, ChannelLimit> channels = new EnumMap<>(Channel.class);
    private ChannelLimit defaultLimit = new ChannelLimit(120, Duration.ofMinutes(1));

    public ChannelLimit forChannel(Channel channel) {
        return channels.getOrDefault(channel, defaultLimit);
    }
    // getters/setters
}
```

```properties
notification.rate-limit.channels.EMAIL.permits=600
notification.rate-limit.channels.EMAIL.window=1m
notification.rate-limit.channels.SMS.permits=120
notification.rate-limit.channels.SMS.window=1m
```

Topics then get declared by iterating the registered senders:

```java
@Bean
KafkaAdmin.NewTopics channelTopics(List<ChannelSender> senders,
                                   @Value("${notification.kafka.partitions:3}") int partitions,
                                   @Value("${notification.kafka.replicas:1}") int replicas) {
    return new KafkaAdmin.NewTopics(senders.stream()
            .flatMap(sender -> Stream.of(
                    TopicBuilder.name(sender.topic()).partitions(partitions).replicas(replicas).build(),
                    TopicBuilder.name(sender.topic() + KafkaTopics.DLT_SUFFIX).partitions(1).replicas(replicas).build()))
            .toArray(NewTopic[]::new));
}
```

A default limit means a missing properties entry throttles conservatively instead of blocking.

**Pros.** Adding a channel becomes: implement `ChannelSender`, add a worker, add two properties.
Removes both switches. Folds in recommendation 11 (`replicas` becomes configurable). Limits become
tunable per environment without a rebuild.
**Cons.** Losing the exhaustive `switch` means the compiler stops reminding you — traded for a
safe runtime default. Convention-derived topic names are less greppable than constants.

**Verdict. Yes**, if Push or WhatsApp are actually coming. If email and SMS are the permanent
scope, the current code is honest and this is over-engineering — the switches are not hurting
anything with two values.

---

## 9. ✅ FIXED — A JPA entity is cached in Redis

**Problem.** `TemplateLookup.find` is `@Cacheable` and returns `NotificationTemplate`, a
`@Entity`. Hibernate-managed objects are serialised to Redis and deserialised back as detached
instances.

**Architectural impact.** Three distinct problems. The cached object is detached, so any future
lazy association throws `LazyInitializationException` on a cache hit but works on a miss —
behaviour that differs by cache state is genuinely hard to debug. The Redis payload is coupled to
the persistence schema, so adding a column can break deserialisation of entries written by the
previous version during a rolling deploy. And a detached entity handed to callers can be mutated
and accidentally re-attached, writing cache-shaped data back to the database.

**Implementation.** Cache an immutable projection:

```java
/** Immutable, persistence-free view of a template. Safe to cache and to hand around. */
public record TemplateView(String code, Channel channel, String subject, String body) {

    static TemplateView from(NotificationTemplate entity) {
        return new TemplateView(entity.getCode(), entity.getChannel(),
                entity.getSubject(), entity.getBody());
    }
}
```

```java
@Cacheable(cacheNames = "templates", key = "#code + ':' + #channel")
public TemplateView find(String code, Channel channel) {
    return templateRepository.findByCodeAndChannelAndActiveIsTrue(code, channel)
            .map(TemplateView::from)
            .orElseThrow(() -> new TemplateNotFoundException(code, channel));
}
```

`NotificationTemplate` then drops `implements Serializable` — it never needed it.

**Pros.** No detached-entity semantics, no schema coupling in Redis, immutable so trivially thread
safe, smaller payload.
**Cons.** One more type and a mapping function.

**Verdict. Yes.** Small change, removes a whole class of rolling-deploy and lazy-loading bugs.

---

## 10. ⚠️ PARTIAL — The sweeper loads `@Lob` bodies to read two fields

**Problem.** `findTop200ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc` returns full
`NotificationRecord` entities. The loop uses only `getId()` and `getChannel()`.

**Architectural impact.** Every sweep can load 200 entities each carrying a `@Lob` body and a
320-character recipient. For HTML emails of 50–100 KB that is 10–20 MB per sweep, every 30
seconds, entirely discarded. Precisely when it matters — a broker outage with a large backlog —
the recovery mechanism creates memory pressure and GC churn on an already-degraded service. It
also drags PII (recipients) and message bodies into memory for no reason.

**Implementation.** A projection:

```java
/** Only what dispatch needs. Keeps @Lob bodies and recipient PII out of the sweep. */
public interface NotificationDispatchView {
    String getId();
    Channel getChannel();
}
```

```java
List<NotificationDispatchView> findTop200ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
        NotificationStatus status, Instant createdBefore);
```

Spring Data generates a constrained `select id, channel from notification ...`.

**Pros.** Roughly two orders of magnitude less memory and network per sweep. Less PII in memory.
No behaviour change.
**Cons.** Projection interfaces are less obvious than entities to developers unfamiliar with them.

**Verdict. Yes.** One-line interface, meaningful benefit exactly when the system is under stress.

> **Partially applied.** `NotificationDispatchView` exists, but `NotificationRepository` still
> returns `List<NotificationRecord>` and `StuckNotificationSweeper` still iterates entities, so
> the memory benefit is not yet realised. Switching the return type and the sweeper's loop
> variable completes it.

---

## 11. ✅ FIXED — `replicas(1)` hardcoded in topic definitions

**Problem.** All five `NewTopic` beans hardcode `.replicas(1)`.

**Architectural impact.** If the topics do not already exist, the application creates
single-replica topics on the production cluster. A single broker failure then permanently loses
every unconsumed notification on those partitions — for a platform whose entire value is not
losing notifications. It also silently violates the cluster's durability standard, and because
`KafkaAdmin` does not reduce replication on existing topics, the failure mode depends on
deployment order, which makes it intermittent and easy to miss in staging.

**Implementation.** Configuration-driven, defaulting to 1 for local development only:

```properties
notification.kafka.partitions=${KAFKA_TOPIC_PARTITIONS:3}
notification.kafka.replicas=${KAFKA_TOPIC_REPLICAS:1}
notification.kafka.min-insync-replicas=${KAFKA_MIN_ISR:1}
```

```java
TopicBuilder.name(name)
        .partitions(partitions)
        .replicas(replicas)
        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minInSync))
        .build();
```

Production sets replicas=3, min ISR=2. Combined with the existing `acks=all`, that gives real
durability: a write is acknowledged only once two replicas hold it.

**Pros.** Correct durability per environment; `acks=all` finally means something. Folded into
recommendation 8 for free.
**Cons.** None. The current value is a local-development default that escaped into topic
definitions.

**Verdict. Yes.** Do it before the first production deploy, not after.

---

## 12. ✅ FIXED — No schema version on the public inbound event

**Problem.** `InboundNotificationEvent` is the contract every other service publishes against.
It has no version field, and with `spring.json.add.type.headers=false` there is no type header
either.

**Architectural impact.** This contract is the hardest thing in the system to change, because
consumers are other teams deploying on their own schedules. Without a version, a breaking change
has no safe rollout: you cannot route by version, cannot reject unsupported payloads with a clear
error, and cannot tell from a DLT message which producer version emitted it. Additive changes are
fine — Jackson ignores unknown fields — but the first genuinely breaking change forces a new topic
and a coordinated migration across every publisher.

**Implementation.** Add a version now, while there is one consumer and zero external publishers:

```java
public record InboundNotificationEvent(
        /** Schema version. Absent is treated as v1 for the existing publishers. */
        Integer schemaVersion,
        String requestId,
        String sourceService,
        Channel channel,
        String recipient,
        String templateCode,
        Map<String, Object> variables) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public int effectiveSchemaVersion() {
        return schemaVersion == null ? 1 : schemaVersion;
    }
}
```

The consumer rejects unknown versions as non-retryable, so a future v2 publisher against a v1
consumer produces a clear DLT entry rather than silently mis-parsed data.

**Pros.** Costs nothing today; the only moment this is free is before external publishers exist.
Makes DLT entries diagnosable.
**Cons.** Version fields are frequently added and never used. A schema registry is the heavier,
more rigorous answer if the event surface grows.

**Verdict. Yes** — purely because of timing. This is cheap now and expensive later.

---

## Lower-priority findings

**13. Builder for `NotificationRecord.accept`.** Seven positional parameters, four consecutive
`String`s (`recipient`, `templateCode`, `subject`, `body`). Transposing two compiles cleanly and
sends the wrong content — or the recipient — to the wrong place. A Lombok `@Builder` on a static
factory makes call sites self-describing. *Verdict: judgement call.* Only two call sites exist,
both covered by tests; the risk is real but currently contained.

**14. No outbound delivery-outcome events.** Nothing is published when a notification reaches
`SENT` or `DEAD_LETTER`, so callers must poll the status endpoint. A `notification.outcomes` topic
carrying `{notificationId, requestId, status, channel, timestamp}` would let a payment service
react to a failed receipt. *Verdict: worth doing when someone asks for it* — building it before
there is a consumer is speculative.

**15. Field injection in `StuckNotificationSweeper`.** `@Value` on a field, inconsistent with
constructor injection everywhere else, and it makes the class awkward to unit test. Move to a
constructor parameter. *Verdict: yes, trivial.*

**16. Scheduler pool of one thread.** Spring's default `TaskScheduler` is single-threaded. Today
there is one scheduled task so it works, but the second one added will contend with the sweeper —
and a slow sweep will delay it silently. Set `spring.task.scheduling.pool.size=2`. *Verdict: yes,
one line.*

**17. `ack-mode=record` commits an offset per record.** A broker round trip per notification. At
low volume it is the safest choice and correct; under load `BATCH` is materially faster with the
same at-least-once guarantee, since the delivery path is already idempotent. *Verdict: only when
throughput demands it.* Do not change it speculatively.

**18. No output escaping in template rendering.** `TemplateService.substitute` inserts variables
verbatim. For plain-text email and SMS the impact is limited to content spoofing — a variable
containing newlines can forge what looks like a separate message. The moment HTML email arrives
(a listed gap) this becomes stored XSS, since variables often carry user-controlled data.
*Verdict: yes, before HTML email ships.* Escape per channel at render time.

**19. No rate limiting on the REST API.** The Redis limiter protects *providers*, not the accept
path. A misbehaving caller can insert unbounded rows and saturate the database. A per-`sourceService`
limit at the controller would bound it. *Verdict: judgement call* — an internal-only API on a
trusted network makes this lower priority than the provider-side limiting already in place.

**20. Layer-based package structure.** `api / service / domain / messaging / channel / template`
is technical rather than feature-oriented. At 44 files this is perfectly navigable and changing it
would be churn. Worth revisiting only if the platform grows several more channels and features.
*Verdict: no.*

---

## Dimensions already in good shape

Reviewed and found sound — listed so the absence of a finding is deliberate rather than an
oversight:

- **Strategy pattern.** `ChannelSender` is a clean strategy with per-channel implementations,
  resolved by injecting `List<ChannelSender>` into an `EnumMap`. Idiomatic.
- **Dependency Inversion.** `DeliveryService` depends on the `ChannelSender` abstraction, never on
  `EmailChannelSender`. The `SmsClient` seam correctly isolates the provider.
- **Constructor injection** throughout (except finding 15). No field injection, no
  `@Autowired` on setters.
- **Transaction boundaries.** Fixed in `8a5bf16`: short `REQUIRES_NEW` transitions, no network I/O
  inside a transaction, `open-in-view=false` set.
- **Idempotency.** `requestId` with a unique index, plus an `isAlreadyDelivered` check in the
  worker and events keyed by notification id. The residual TOCTOU gap — a crash after a successful
  send but before `markSent` — is inherent without provider-side idempotency keys and is
  documented honestly.
- **Dual-write handling.** The table-as-outbox pattern with the sweeper is the correct shape.
- **DTO mapping.** Hand-written `NotificationResponse.from`. At this size a mapping framework
  would add a dependency and annotation processing for no benefit.
- **API versioning.** `/api/v1/` path versioning is the pragmatic default for internal services.
- **Redis fail-open.** The rate limiter allowing traffic when Redis is unreachable is the right
  trade-off and is explicitly tested.
- **PII handling.** `Redaction` applied in logs and API responses; bodies and recipients kept off
  Kafka by design. Genuinely better than most services of this size.
- **Cohesion after the refactor.** `NotificationService` / `NotificationWriter` /
  `NotificationStateWriter` / `DeliveryService` have distinct, defensible responsibilities.

## Not applicable

- **REST client abstraction.** There are no outbound HTTP clients — email is SMTP, SMS is an
  unimplemented seam. When a real SMS provider lands, implement `SmsClient` with `RestClient`,
  explicit connect/read timeouts, and the circuit breaker from recommendation 4. The abstraction
  is already in the right place.
- **Bulkhead / thread isolation.** Partially addressed by per-channel topics and consumer groups.
  Genuine bulkheading would need separate container factories with dedicated executors per
  channel — worth revisiting only after the circuit breaker exists.

---

## Suggested order

1. **Finding 1** (SMTP timeouts) — three lines, removes the worst failure mode.
2. **Findings 5, 15, 16** — small, independent, no design debate.
3. **Findings 3 + 7 together** — validation gap and layering in one change.
4. **Findings 2, 9, 10, 11** — independent, each contained.
5. **Finding 6** — needs findings 3 and 7 settled first.
6. **Finding 4** (circuit breaker) — after 1, and after confirming Boot 4 compatibility.
7. **Findings 8, 12** — only if more channels and external publishers are genuinely coming.

Nothing here is a rewrite. The largest single change is recommendation 7, and it is mechanical.