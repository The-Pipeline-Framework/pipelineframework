package org.pipelineframework.plugin.persistence.provider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.Entity;
import jakarta.persistence.EntityManager;

import io.quarkus.arc.InjectableInstance;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class VThreadPersistenceProviderTest {

    @Mock
    InjectableInstance<EntityManager> entityManagerInstance;

    @Mock
    EntityManager entityManager;

    private VThreadPersistenceProvider provider;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        provider = new VThreadPersistenceProvider(entityManagerInstance);
    }

    @Test
    void supportsEntityTypesOnly() {
        assertTrue(provider.supports(new TestEntity()));
        assertFalse(provider.supports("not an entity"));
        assertFalse(provider.supports(null));
    }

    @Test
    void persistsAndReturnsTheEntity() {
        TestEntity entity = new TestEntity();
        when(entityManagerInstance.isResolvable()).thenReturn(true);
        when(entityManagerInstance.get()).thenReturn(entityManager);

        UniAssertSubscriber<Object> subscriber = provider.persist(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitItem();
        assertSame(entity, subscriber.getItem());
        verify(entityManager).persist(entity);
    }

    @Test
    void failsWhenTheEntityManagerIsUnavailable() {
        when(entityManagerInstance.isResolvable()).thenReturn(false);

        UniAssertSubscriber<Object> subscriber = provider.persist(new TestEntity())
            .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        assertInstanceOf(IllegalStateException.class, subscriber.getFailure());
    }

    @Test
    void propagatesPersistenceFailures() {
        TestEntity entity = new TestEntity();
        RuntimeException failure = new RuntimeException("Persist failed");
        when(entityManagerInstance.isResolvable()).thenReturn(true);
        when(entityManagerInstance.get()).thenReturn(entityManager);
        doThrow(failure).when(entityManager).persist(entity);

        UniAssertSubscriber<Object> subscriber = provider.persist(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        assertSame(failure, subscriber.getFailure());
        verify(entityManager).persist(entity);
    }

    @Setter
    @Getter
    @Entity
    private static class TestEntity {
        @jakarta.persistence.Id
        private Long id;
    }
}
