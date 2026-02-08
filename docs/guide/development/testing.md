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
    PaymentRecord record = new PaymentRecord();
    Uni<PaymentStatus> result = service.process(record);
    UniAssertSubscriber<PaymentStatus> subscriber =
        result.subscribe().withSubscriber(UniAssertSubscriber.create());
    subscriber.awaitItem();
  }
}
```

## Integration Tests with Testcontainers

Use Testcontainers when you need real infrastructure. The pattern below uses Postgres and Redis as examples.

```java
@QuarkusTest
@Testcontainers
class PersistenceIntegrationTest {

  @Container
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:17");

  @Container
  static GenericContainer<?> redis =
      new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  @DynamicPropertySource
  static void datasourceProps(DynamicPropertyRegistry registry) {
    registry.add("quarkus.datasource.jdbc.url", postgres::getJdbcUrl);
    registry.add("quarkus.datasource.username", postgres::getUsername);
    registry.add("quarkus.datasource.password", postgres::getPassword);

    registry.add("quarkus.redis.hosts",
        () -> "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
  }

  @Test
  void persistsRecords() {
    // exercise repository or pipeline entrypoint
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

## Tips

- Prefer unit tests for step logic, reserve containers for persistence, cache, or orchestration tests.
- Keep container startup localized to integration tests to avoid slowing down unit test runs.
- If you need to run container-backed tests in CI, make sure Docker is available and stable.

## Framework vs Application Tests

Application tests (this guide) focus on your pipeline behavior. If you are contributing to the framework itself, use the maintainer guide at [Testing Guidelines](/guide/evolve/testing-guidelines).
