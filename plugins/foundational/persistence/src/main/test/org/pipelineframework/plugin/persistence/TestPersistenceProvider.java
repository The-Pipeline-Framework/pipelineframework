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

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.domain.TestEntity;
import org.pipelineframework.persistence.PersistenceProvider;

/** Test persistence provider for TestEntity used in unit tests. */
@ApplicationScoped
public class TestPersistenceProvider implements PersistenceProvider<TestEntity> {

    @Override
    public Class<TestEntity> type() {
        return TestEntity.class;
    }

    @Override
    public Uni<TestEntity> persist(TestEntity entity) {
        if (entity == null) {
            return Uni.createFrom().nullItem();
        }

        // In a test scenario, just return the entity as-is
        return Uni.createFrom().item(entity);
    }

    @Override
    public boolean supports(Object entity) {
        return entity instanceof TestEntity;
    }
}
