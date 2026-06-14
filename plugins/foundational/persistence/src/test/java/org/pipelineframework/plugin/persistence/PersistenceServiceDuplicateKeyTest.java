package org.pipelineframework.plugin.persistence;

import java.sql.SQLException;
import java.util.Optional;
import jakarta.persistence.PersistenceException;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.vertx.pgclient.PgException;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PersistenceServiceDuplicateKeyTest {

    @Test
    void ignoreDuplicateKeyPolicyHandlesSuppressedPostgresFailure() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceConfig config = ignorePolicyConfig();
        PersistenceService<TestEntity> service = service(manager, config);

        TestEntity entity = new TestEntity();
        PersistenceException wrapper = new PersistenceException("Failed to persist entity");
        wrapper.addSuppressed(new PgException(
            "duplicate key value violates unique constraint \"paymentrecord_pkey\"",
            "ERROR",
            "23505",
            "paymentrecord_pkey"));
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(wrapper));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem());
        verify(manager).persist(entity);
    }

    @Test
    void ignoreDuplicateKeyPolicyHandlesCauseChainPostgresFailure() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = service(manager, ignorePolicyConfig());

        TestEntity entity = new TestEntity();
        PersistenceException wrapper = new PersistenceException("Failed to persist entity",
            new PgException(
                "duplicate key value violates unique constraint \"paymentrecord_pkey\"",
                "ERROR",
                "23505",
                "paymentrecord_pkey"));
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(wrapper));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem());
        verify(manager).persist(entity);
    }

    @Test
    void ignoreDuplicateKeyPolicyFallsBackToDuplicateKeyText() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = service(manager, ignorePolicyConfig());

        TestEntity entity = new TestEntity();
        SQLException duplicate = new SQLException("duplicate key value violates unique constraint", "99999");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(duplicate));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem());
        verify(manager).persist(entity);
    }

    @Test
    void ignoreDuplicateKeyPolicyRecognizesGenericIntegritySqlState() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = service(manager, ignorePolicyConfig());

        TestEntity entity = new TestEntity();
        SQLException duplicate = new SQLException("constraint violation", "23000");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(duplicate));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem());
        verify(manager).persist(entity);
    }

    @Test
    void upsertDuplicateKeyPolicyPersistsOrUpdates() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceConfig config = config("upsert");
        PersistenceService<TestEntity> service = service(manager, config);

        TestEntity entity = new TestEntity();
        SQLException duplicate = new SQLException("duplicate key", "23505");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(duplicate));
        when(manager.persistOrUpdate(entity)).thenReturn(Uni.createFrom().item(entity));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem());
        verify(manager).persist(entity);
        verify(manager).persistOrUpdate(entity);
    }

    @Test
    void failDuplicateKeyPolicyPropagatesThroughNonTransientWrapper() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceConfig config = config("fail");
        PersistenceService<TestEntity> service = service(manager, config);

        TestEntity entity = new TestEntity();
        SQLException duplicate = new SQLException("duplicate key", "23505");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(duplicate));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable thrown = subscriber.getFailure();
        assertTrue(thrown instanceof NonRetryableException);
        assertSame(duplicate, thrown.getCause());
        verify(manager).persist(entity);
    }

    private static PersistenceService<TestEntity> service(PersistenceManager manager, PersistenceConfig config) {
        PersistenceService<TestEntity> service = new PersistenceService<>();
        service.persistenceManager = manager;
        service.config = config;
        return service;
    }

    private static PersistenceConfig ignorePolicyConfig() {
        return config("ignore");
    }

    private static PersistenceConfig config(String duplicateKeyPolicy) {
        PersistenceConfig config = mock(PersistenceConfig.class);
        when(config.duplicateKey()).thenReturn(duplicateKeyPolicy);
        when(config.providerClass()).thenReturn(Optional.empty());
        return config;
    }

    static final class TestEntity {
    }
}
