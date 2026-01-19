# Testing Guidelines for This Project

This document defines how all tests must be organized and executed.
These rules apply to all contributors — humans and AI-based agents.

---

## 1. POM files must be 100% declarative

POM files must not contain any logic. This means:

- No profiles that enable or disable tests
- No activation blocks
- No properties that control test inclusion/exclusion
- No conditional XML or inline expressions
- No behavioral branching inside plugin configuration
- No dynamic behavior based on flags or environment variables

POM files must define only static configuration for:
- Maven Surefire
- Maven Failsafe
- Jacoco (configured to apply to Surefire only)

All behavioral differences belong in CI/CD, not in POMs.

---

## 2. Test naming conventions (the only mechanism for test selection)

Test execution is determined exclusively by file naming.

Surefire (unit tests and @QuarkusTest) runs during the test phase and includes:
- Any test class whose name ends with Test

Failsafe (integration tests and @QuarkusIntegrationTest) runs during integration-test and verify and includes:
- Any test class whose name ends with IT

Do not override these conventions:
- No includes/excludes in POM
- No custom patterns
- No profile-based switching

Naming conventions are the single source of truth.

---

## 3. Coverage rules (Jacoco)

Coverage applies only to tests run by Maven Surefire.

Types of tests and coverage:
- Unit tests: covered
- @QuarkusTest: covered
- Integration tests (names ending with IT): not covered
- @QuarkusIntegrationTest: not covered

Rules:
- Jacoco must never instrument Failsafe executions
- No conditional logic to include/exclude integration tests
- Coverage must be enabled by CI only using the coverage profile

---

## 4. CI-driven behavior

CI/CD is the only place where behavior changes based on workflow type.

PR builds (fast CI) should run with:
- mvn verify -Pcoverage -DskipITs -Dquarkus.container-image.build=false

Effect:
- Only Surefire tests run
- Integration tests are skipped
- No container images are built
- Coverage includes only unit + @QuarkusTest

Main branch / Release builds should run with:
- mvn verify -Pcoverage -Dquarkus.container-image.build=false

Effect:
- Surefire and Failsafe both run
- Integration tests run
- Container images are built through docker builds (no Jib on GitHub Actions)
- Coverage still only from Surefire tests

What not to do:
- Do not add POM profiles to switch behavior
- Do not add activation logic
- Do not use properties inside POM to exclude tests
- Do not manipulate plugins conditionally inside XML

---

## 5. Integration tests (names ending with IT)

Integration tests must:
- Require Testcontainers or external infra
- Use @QuarkusIntegrationTest when applicable
- Run only in main-branch CI builds
- Avoid Jacoco entirely

Integration tests must not run on PRs.

---

## 6. Before adding any new tests

Follow this decision tree:

1. Pure business logic? → write a unit test (name ends with Test)
2. Needs Quarkus runtime, DI, config, serialization, etc? → use @QuarkusTest (name ends with Test)
3. Requires containers, external services, or full bootstrap? → use @QuarkusIntegrationTest (name ends with IT)

Name tests accordingly and let CI enforce execution.

---

## 7. Golden rules

POM = Declarative only  
CI = Controls behavior  
Naming = Determines test type  
Coverage = Surefire only

If a change violates any of these principles, it must be corrected before merging.

---