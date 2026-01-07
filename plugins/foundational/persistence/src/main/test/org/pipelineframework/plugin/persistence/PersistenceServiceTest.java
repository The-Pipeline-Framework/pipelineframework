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

package org.pipelineframework.plugin.persistence;

import java.sql.SQLException;
import java.sql.SQLTransientException;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class PersistenceServiceTest {

    static class TestEntity {
    }

    @Test
    void process_WithDuplicateKeyFailureAndIgnorePolicy_ShouldReturnItem() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = new PersistenceService<>();
        service.persistenceManager = manager;
        service.config = () -> "ignore";

        TestEntity entity = new TestEntity();
        SQLException duplicate = new SQLException("duplicate key", "23505");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(duplicate));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(entity, subscriber.getItem());
        verify(manager).persist(entity);
    }

    @Test
    void process_WithDuplicateKeyFailureAndUpsertPolicy_ShouldPersistOrUpdate() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = new PersistenceService<>();
        service.persistenceManager = manager;
        service.config = () -> "upsert";

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
    void process_WithNonTransientFailure_ShouldWrapAsNonRetryable() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = new PersistenceService<>();
        service.persistenceManager = manager;

        TestEntity entity = new TestEntity();
        RuntimeException failure = new RuntimeException("boom");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(failure));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable thrown = subscriber.getFailure();
        assertTrue(thrown instanceof NonRetryableException);
        assertSame(failure, thrown.getCause());
    }

    @Test
    void process_WithDuplicateKeyFailureAndDefaultPolicy_ShouldWrapAsNonRetryable() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = new PersistenceService<>();
        service.persistenceManager = manager;
        service.config = () -> "fail";

        TestEntity entity = new TestEntity();
        SQLException duplicate = new SQLException("duplicate key", "23505");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(duplicate));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable thrown = subscriber.getFailure();
        assertTrue(thrown instanceof NonRetryableException);
        assertSame(duplicate, thrown.getCause());
    }

    @Test
    void process_WithTransientFailure_ShouldNotWrap() {
        PersistenceManager manager = mock(PersistenceManager.class);
        PersistenceService<TestEntity> service = new PersistenceService<>();
        service.persistenceManager = manager;

        TestEntity entity = new TestEntity();
        SQLTransientException failure = new SQLTransientException("connection refused");
        when(manager.persist(entity)).thenReturn(Uni.createFrom().failure(failure));

        UniAssertSubscriber<TestEntity> subscriber = service.process(entity)
            .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable thrown = subscriber.getFailure();
        assertSame(failure, thrown);
    }
}
