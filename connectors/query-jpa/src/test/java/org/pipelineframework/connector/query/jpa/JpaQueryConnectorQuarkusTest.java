package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    record CustomerRiskLookup(String customerId) {
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }
}
