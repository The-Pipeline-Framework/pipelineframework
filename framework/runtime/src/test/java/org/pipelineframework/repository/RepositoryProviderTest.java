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

package org.pipelineframework.repository;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

class RepositoryProviderTest {

    @Test
    void defaultExistsOnlyMapsPayloadNotFoundToFalse() {
        PayloadReference reference = reference();
        RepositoryProvider provider = new TestRepositoryProvider(new PayloadNotFoundException(reference));

        assertFalse(provider.exists(reference).await().indefinitely());
    }

    @Test
    void defaultExistsPropagatesUnexpectedLoadFailures() {
        PayloadReference reference = reference();
        RepositoryProvider provider = new TestRepositoryProvider(new IllegalStateException("backend unavailable"));

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> provider.exists(reference).await().indefinitely());

        org.junit.jupiter.api.Assertions.assertEquals("backend unavailable", exception.getMessage());
    }

    private PayloadReference reference() {
        return new PayloadReference("test", null, "key", "text/plain", "string", null, 0, null, null);
    }

    private static final class TestRepositoryProvider implements RepositoryProvider {
        private final RuntimeException failure;

        private TestRepositoryProvider(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public Uni<PayloadReference> store(RepositoryWriteRequest request) {
            return Uni.createFrom().failure(new UnsupportedOperationException());
        }

        @Override
        public Uni<RepositoryReadResult> load(PayloadReference reference) {
            return Uni.createFrom().failure(failure);
        }
    }
}
