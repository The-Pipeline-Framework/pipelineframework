---
search: false
---

# Testing with Testcontainers

This guide covers how to test TPF applications with a focus on unit tests and integration tests using Testcontainers.

## Goals

- Keep business logic tests fast and deterministic.
- Use Testcontainers only when you need real dependencies (Postgres, Redis, external services).
- Separate framework development tests from application tests.

## Unit Tests (No Containers)

For unit tests, keep the step logic isolated and mock dependencies where possible.

```java
@QuarkusTest
class ProcessPaymentServiceTest {

  @Inject
  ProcessPaymentService service;

  @Test
  void processesPayment() {
    PaymentRecord record = new PaymentRecord("id-1", BigDecimal.TEN);
    Uni<PaymentStatus> result = service.process(record);
    UniAssertSubscriber<PaymentStatus> subscriber =
        result.subscribe().withSubscriber(UniAssertSubscriber.create());
    PaymentStatus status = subscriber.awaitItem().getItem();
    assertNotNull(status);
    assertNull(subscriber.getFailure());
  }
}
```

## Integration Tests with Testcontainers

Use Testcontainers when you need real infrastructure. The pattern below uses Postgres and Redis as examples.

```java
public class PostgresRedisResource implements QuarkusTestResourceLifecycleManager {

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> redis;

  @Override
  public Map<String, String> start() {
    postgres = new PostgreSQLContainer<>("postgres:17");
    redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    postgres.start();
    redis.start();

    return Map.of(
        "quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
        "quarkus.datasource.username", postgres.getUsername(),
        "quarkus.datasource.password", postgres.getPassword(),
        "quarkus.redis.hosts", "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379)
    );
  }

  @Override
  public void stop() {
    if (redis != null) {
      redis.stop();
    }
    if (postgres != null) {
      postgres.stop();
    }
  }
}

@QuarkusTest
@QuarkusTestResource(value = PostgresRedisResource.class, restrictToAnnotatedClass = true)
class PersistenceIntegrationIT {

  @Inject
  PaymentRepository repository;

  @Test
  void persistsRecords() {
    PaymentEntity entity = new PaymentEntity("id-1", BigDecimal.TEN);
    repository.persist(entity);
    assertTrue(repository.findByIdOptional("id-1").isPresent());
  }
}
```

## Running Tests

```bash
./mvnw test
```

```bash
./mvnw verify
```

`./mvnw test` runs the fast `test` phase and is the right home for unit tests such as `*Test`. `./mvnw verify` includes the `verify` phase, where integration tests such as `*IT` and container-backed checks should run.

## Tips

- Prefer unit tests for step logic, reserve containers for persistence, cache, or orchestration tests.
- Keep container startup localized to integration tests to avoid slowing down unit test runs.
- If you need to run container-backed tests in CI, make sure Docker is available and stable.

## Framework vs Application Tests

Application tests (this guide) focus on your pipeline behavior. If you are contributing to the framework itself, use the maintainer guide at [Testing Guidelines](/versions/v26.4.5/guide/evolve/testing-guidelines).
