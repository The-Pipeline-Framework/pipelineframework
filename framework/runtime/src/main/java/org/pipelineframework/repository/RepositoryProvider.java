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

import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Storage backend SPI for materialized pipeline payload fields.
 */
public interface RepositoryProvider {

    String providerName();

    Uni<PayloadReference> store(RepositoryWriteRequest request);

    Uni<RepositoryReadResult> load(PayloadReference reference);

    default Uni<Boolean> exists(PayloadReference reference) {
        return load(reference)
            .onItem().transform(ignored -> true)
            .onFailure(PayloadNotFoundException.class).recoverWithItem(false);
    }

    default Uni<Boolean> delete(PayloadReference reference) {
        return Uni.createFrom().item(false);
    }

    default boolean supports(PayloadReference reference) {
        return reference != null && providerName().equalsIgnoreCase(reference.provider());
    }

    default boolean supportsThreadContext() {
        return true;
    }

    default ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }
}
