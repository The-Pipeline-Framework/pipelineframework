/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.persistence.provider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.quarkus.arc.InjectableInstance;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import java.lang.reflect.Field;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Comprehensive unit tests for VThreadPersistenceProvider. Tests entity persistence, transaction
 * handling, and virtual thread detection.
 */
@QuarkusTest
class VThreadPersistenceProviderTest {

    @Mock
    private InjectableInstance<EntityManager> mockEntityManagerInstance;

    @Mock
    private EntityManager mockEntityManager;

    @Mock
    private EntityTransaction mockTransaction;

    private VThreadPersistenceProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        provider = new VThreadPersistenceProvider();

        // Inject the mock InjectableInstance
        Field field = VThreadPersistenceProvider.class.getDeclaredField("entityManagerInstance");
        field.setAccessible(true);
        field.set(provider, mockEntityManagerInstance);
    }

    @Test
    void testTypeReturnsObjectClass() {
        // When
        Class<Object> type = provider.type();

        // Then
        assertEquals(Object.class, type, "Type should be Object.class");
    }

    @Test
    void testSupportsReturnsTrueForEntityAnnotatedClass() {
        // Given
        TestEntity entity = new TestEntity();

        // When
        boolean supports = provider.supports(entity);

        // Then
        assertTrue(supports, "Should support @Entity annotated classes");
    }

    @Test
    void testSupportsReturnsFalseForNonEntityClass() {
        // Given
        String nonEntity = "Not an entity";

        // When
        boolean supports = provider.supports(nonEntity);

        // Then
        assertFalse(supports, "Should not support non-@Entity classes");
    }

    @Test
    void testSupportsReturnsFalseForNull() {
        // When
        boolean supports = provider.supports(null);

        // Then
        assertFalse(supports, "Should not support null");
    }

    @Test
    void testSupportsThreadContextReturnsFalseOnPlatformThread() {
        // Note: This test assumes running on the JUnit platform thread.
        // In a real virtual-thread context, supportsThreadContext() should return true instead.

        // When
        boolean supportsThreadContext = provider.supportsThreadContext();

        // Then
        // On platform threads, should return false
        assertFalse(
                supportsThreadContext,
                "Should return false when not on a virtual thread (running on platform thread)");
    }

    @Test
    void testPersistSuccessfullyPersistsEntity() {
        // Given
        TestEntity entity = new TestEntity();
        when(mockEntityManagerInstance.isResolvable()).thenReturn(true);
        when(mockEntityManagerInstance.get()).thenReturn(mockEntityManager);
        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem(), "Should return the persisted entity");
        verify(mockTransaction).begin();
        verify(mockEntityManager).persist(entity);
        verify(mockTransaction).commit();
        verify(mockEntityManager).close();
    }

    @Test
    void testPersistThrowsExceptionWhenEntityManagerNotResolvable() {
        // Given
        TestEntity entity = new TestEntity();
        when(mockEntityManagerInstance.isResolvable()).thenReturn(false);

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable failure = subscriber.getFailure();
        assertInstanceOf(
                IllegalStateException.class,
                failure,
                "Should throw IllegalStateException when EntityManager not available");
        assertTrue(
                failure.getMessage().contains("No EntityManager available"),
                "Error message should indicate no EntityManager");
    }

    @Test
    void testPersistRollsBackOnFailure() {
        // Given
        TestEntity entity = new TestEntity();
        when(mockEntityManagerInstance.isResolvable()).thenReturn(true);
        when(mockEntityManagerInstance.get()).thenReturn(mockEntityManager);
        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);
        when(mockTransaction.isActive()).thenReturn(true);

        RuntimeException persistException = new RuntimeException("Persist failed");
        doThrow(persistException).when(mockEntityManager).persist(entity);

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        verify(mockTransaction).begin();
        verify(mockEntityManager).persist(entity);
        verify(mockTransaction).rollback();
        verify(mockEntityManager).close();
        verify(mockTransaction, never()).commit();
    }

    @Test
    void testPersistDoesNotRollbackIfTransactionNotActive() {
        // Given
        TestEntity entity = new TestEntity();
        when(mockEntityManagerInstance.isResolvable()).thenReturn(true);
        when(mockEntityManagerInstance.get()).thenReturn(mockEntityManager);
        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);
        when(mockTransaction.isActive()).thenReturn(false);

        RuntimeException persistException = new RuntimeException("Persist failed");
        doThrow(persistException).when(mockEntityManager).persist(entity);

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        verify(mockTransaction).begin();
        verify(mockEntityManager).persist(entity);
        verify(mockTransaction, never()).rollback();
        verify(mockEntityManager).close();
    }

    @Test
    void testPersistClosesEntityManagerEvenOnFailure() {
        // Given
        TestEntity entity = new TestEntity();
        when(mockEntityManagerInstance.isResolvable()).thenReturn(true);
        when(mockEntityManagerInstance.get()).thenReturn(mockEntityManager);
        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);

        RuntimeException persistException = new RuntimeException("Persist failed");
        doThrow(persistException).when(mockEntityManager).persist(entity);

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        verify(mockEntityManager).close();
    }

    @Test
    void testPersistHandlesCommitFailure() {
        // Given
        TestEntity entity = new TestEntity();
        when(mockEntityManagerInstance.isResolvable()).thenReturn(true);
        when(mockEntityManagerInstance.get()).thenReturn(mockEntityManager);
        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);
        when(mockTransaction.isActive()).thenReturn(true);

        RuntimeException commitException = new RuntimeException("Commit failed");
        doThrow(commitException).when(mockTransaction).commit();

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        verify(mockTransaction).begin();
        verify(mockEntityManager).persist(entity);
        verify(mockTransaction).commit();
        verify(mockTransaction).rollback();
        verify(mockEntityManager).close();
    }

    @Test
    void testPersistReturnsOriginalEntityReference() {
        // Given
        TestEntity entity = new TestEntity();
        entity.setValue("test-value");
        when(mockEntityManagerInstance.isResolvable()).thenReturn(true);
        when(mockEntityManagerInstance.get()).thenReturn(mockEntityManager);
        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);

        // When
        Uni<Object> result = provider.persist(entity);

        // Then
        UniAssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        Object persistedEntity = subscriber.getItem();
        assertSame(entity, persistedEntity, "Should return the same entity reference");
        assertInstanceOf(TestEntity.class, persistedEntity);
        assertEquals("test-value", ((TestEntity) persistedEntity).getValue());
    }

    @Test
    void testSupportsVariousEntityTypes() {
        // Given
        TestEntity entity1 = new TestEntity();
        AnotherTestEntity entity2 = new AnotherTestEntity();

        // When
        boolean supports1 = provider.supports(entity1);
        boolean supports2 = provider.supports(entity2);

        // Then
        assertTrue(supports1, "Should support TestEntity");
        assertTrue(supports2, "Should support AnotherTestEntity");
    }

    @Test
    void testDoesNotSupportNonAnnotatedClasses() {
        // Given
        NonEntityClass nonEntity = new NonEntityClass();

        // When
        boolean supports = provider.supports(nonEntity);

        // Then
        assertFalse(supports, "Should not support non-@Entity classes");
    }

    /** Test entity class for testing */
    @Setter
    @Getter
    @Entity
    private static class TestEntity {
        private String value;
        @jakarta.persistence.Id
        private Long id;
    }

    /** Another test entity class */
    @Setter
    @Getter
    @Entity
    private static class AnotherTestEntity {
        @jakarta.persistence.Id
        private Long id;
    }

    /** Non-entity class for negative testing */
    private static class NonEntityClass {
    }
}
