package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.config.pipeline.PipelineYamlJpaPredicate;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.BlockingQueryOperation;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryCacheability;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.query.QueryRequest;
import org.pipelineframework.query.QueryStepDescriptor;

@QuarkusTest
class JpaQueryConnectorQuarkusTest {
    @Inject
    EntityManagerFactory entityManagerFactory;

    @Inject
    JpaQueryConnector connector;

    @Test
    void loadsProjectedRecordFromDeclarativeJpaQuery() {
        replaceWith(new CustomerRiskEntity("customer-1", "HIGH", 91));

        CustomerRiskFacts facts = connector.queryOne(
                new QueryRequest<>(descriptor(), new CustomerRiskLookup("customer-1")),
                CustomerRiskFacts.class).toCompletableFuture().join();

        assertEquals(new CustomerRiskFacts("customer-1", "HIGH", 91), facts);
    }

    @Test
    @SuppressWarnings("unchecked")
    void nativeOperationUsesInvocationOutputTypeAndReturnsSemanticOutcome() {
        replaceWith(new CustomerRiskEntity("customer-native", "HIGH", 93));

        QueryOperation<Object, JpaFindOneConfiguration, Object> operation =
            (QueryOperation<Object, JpaFindOneConfiguration, Object>) connector.operations().stream()
                .filter(QueryOperation.class::isInstance)
                .filter(candidate -> "find.one".equals(candidate.id()))
                .findFirst()
                .orElseThrow();
        assertInstanceOf(BlockingQueryOperation.class, operation);
        assertEquals(QueryCacheability.CACHEABLE, operation.capabilities().cacheability());
        JpaFindOneConfiguration configuration = nativeConfiguration();
        QueryOutcome<Object> outcome = operation.query(new QueryInvocation<>(
                new CustomerRiskLookup("customer-native"),
                configuration,
                (Class<Object>) (Class<?>) CustomerRiskFacts.class,
                ConnectorExecutionContext.empty())).toCompletableFuture().join();

        assertEquals(
            new CustomerRiskFacts("customer-native", "HIGH", 93),
            assertInstanceOf(QueryOutcome.Found.class, outcome).output());
    }

    private static JpaFindOneConfiguration nativeConfiguration() {
        return new JpaFindOneConfiguration(
            CustomerRiskEntity.class.getName(),
            Map.of("customerId", new JpaPredicate("eq", List.of("input.customerId"))),
            Optional.of(Map.of("customerId", "customerId", "riskBand", "riskBand", "score", "score")),
            Optional.empty(),
            Optional.empty(),
            Optional.of("single"));
    }

    @Test
    void duplicateRowsFailWithoutLimit() {
        replaceWith(
            new CustomerRiskEntity("customer-duplicate", "HIGH", 91),
            new CustomerRiskEntity("customer-duplicate", "MEDIUM", 72));

        CompletionException failure = assertThrows(CompletionException.class, () -> connector.queryOne(
                new QueryRequest<>(descriptor(), new CustomerRiskLookup("customer-duplicate")),
                CustomerRiskFacts.class).toCompletableFuture().join());

        assertInstanceOf(IllegalStateException.class, failure.getCause());
    }

    @Test
    void orderByLimitReturnsLatestMatchingRow() {
        replaceWith(
            new CustomerRiskEntity("customer-latest", "LOW", 45, "ACTIVE", 1, null),
            new CustomerRiskEntity("customer-latest", "MEDIUM", 85, "ACTIVE", 2, null),
            new CustomerRiskEntity("customer-latest", "HIGH", 91, "ACTIVE", 3, null),
            new CustomerRiskEntity("customer-latest", "CRITICAL", 99, "INACTIVE", 3, null));

        CustomerRiskFacts facts = connector.queryOne(
                new QueryRequest<>(latestActiveRiskDescriptor(), new CustomerRiskLookup("customer-latest", 80)),
                CustomerRiskFacts.class).toCompletableFuture().join();

        assertEquals(new CustomerRiskFacts("customer-latest", "HIGH", 91), facts);
    }

    private void replaceWith(CustomerRiskEntity... entities) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        var transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            entityManager.createQuery("delete from " + CustomerRiskEntity.class.getName()).executeUpdate();
            for (CustomerRiskEntity entity : entities) {
                entityManager.persist(entity);
            }
            transaction.commit();
        } finally {
            if (transaction.isActive()) {
                transaction.rollback();
            }
            entityManager.close();
        }
    }

    private static QueryStepDescriptor descriptor() {
        return new QueryStepDescriptor(
            "LoadCustomerRisk",
            "customer-risk",
            "jpa",
            "v1",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName(),
            "ONE_TO_ONE",
            List.of("customerId"),
            new PipelineYamlJpaQuery(
                CustomerRiskEntity.class.getName(),
                Map.of("customerId", "input.customerId"),
                Map.of("customerId", "customerId", "riskBand", "riskBand", "score", "score"),
                "single"));
    }

    private static QueryStepDescriptor latestActiveRiskDescriptor() {
        Map<String, PipelineYamlJpaPredicate> where = new LinkedHashMap<>();
        where.put("customerId", PipelineYamlJpaPredicate.equalTo("input.customerId"));
        where.put("status", new PipelineYamlJpaPredicate("eq", List.of("ACTIVE")));
        where.put("score", new PipelineYamlJpaPredicate("gte", List.of("input.minimumScore")));
        where.put("deletedAt", new PipelineYamlJpaPredicate("isNull", List.of(true)));
        return new QueryStepDescriptor(
            "LoadLatestActiveRisk",
            "latest-active-risk",
            "jpa",
            "v2",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName(),
            "ONE_TO_ONE",
            List.of("customerId"),
            new PipelineYamlJpaQuery(
                CustomerRiskEntity.class.getName(),
                where,
                Map.of("customerId", "customerId", "riskBand", "riskBand", "score", "score"),
                Map.of("updatedAt", "desc"),
                1,
                "single"));
    }

    record CustomerRiskLookup(String customerId, int minimumScore) {
        CustomerRiskLookup(String customerId) {
            this(customerId, 0);
        }
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }
}
