package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.config.pipeline.PipelineYamlJpaPredicate;
import org.pipelineframework.query.QueryRequest;
import org.pipelineframework.query.QueryStepDescriptor;

@QuarkusTest
class JpaQueryConnectorQuarkusTest {
    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    JpaQueryConnector connector;

    @Test
    @RunOnVertxContext
    void loadsProjectedRecordFromDeclarativeJpaQuery(UniAsserter asserter) {
        asserter.execute(() -> sessionFactory.withTransaction((session, tx) ->
            session.createMutationQuery("delete from " + CustomerRiskEntity.class.getName()).executeUpdate()
                .replaceWithVoid()
                .chain(() -> session.persist(new CustomerRiskEntity("customer-1", "HIGH", 91)))));

        asserter.assertThat(
            () -> Uni.createFrom().completionStage(connector.queryOne(
                new QueryRequest<>(descriptor(), new CustomerRiskLookup("customer-1")),
                CustomerRiskFacts.class)),
            facts -> assertEquals(new CustomerRiskFacts("customer-1", "HIGH", 91), facts));
    }

    @Test
    @RunOnVertxContext
    void duplicateRowsFailWithoutLimit(UniAsserter asserter) {
        asserter.execute(() -> sessionFactory.withTransaction((session, tx) ->
            session.createMutationQuery("delete from " + CustomerRiskEntity.class.getName()).executeUpdate()
                .replaceWithVoid()
                .chain(() -> session.persist(new CustomerRiskEntity("customer-duplicate", "HIGH", 91)))
                .chain(() -> session.persist(new CustomerRiskEntity("customer-duplicate", "MEDIUM", 72)))));

        asserter.assertFailedWith(
            () -> Uni.createFrom().completionStage(connector.queryOne(
                new QueryRequest<>(descriptor(), new CustomerRiskLookup("customer-duplicate")),
                CustomerRiskFacts.class)),
            IllegalStateException.class);
    }

    @Test
    @RunOnVertxContext
    void orderByLimitReturnsLatestMatchingRow(UniAsserter asserter) {
        asserter.execute(() -> sessionFactory.withTransaction((session, tx) ->
            session.createMutationQuery("delete from " + CustomerRiskEntity.class.getName()).executeUpdate()
                .replaceWithVoid()
                .chain(() -> session.persist(new CustomerRiskEntity("customer-latest", "LOW", 45, "ACTIVE", 1, null)))
                .chain(() -> session.persist(new CustomerRiskEntity("customer-latest", "HIGH", 91, "ACTIVE", 2, null)))
                .chain(() -> session.persist(new CustomerRiskEntity("customer-latest", "CRITICAL", 99, "INACTIVE", 3, null)))));

        asserter.assertThat(
            () -> Uni.createFrom().completionStage(connector.queryOne(
                new QueryRequest<>(latestActiveRiskDescriptor(), new CustomerRiskLookup("customer-latest", 80)),
                CustomerRiskFacts.class)),
            facts -> assertEquals(new CustomerRiskFacts("customer-latest", "HIGH", 91), facts));
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
