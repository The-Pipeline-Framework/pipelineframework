/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.connector;

import io.smallrye.mutiny.Multi;

/**
 * Produces connector records from a live or replay source.
 *
 * @param <T> source payload type
 */
@FunctionalInterface
public interface ConnectorSource<T> {

    /**
 * Produce a reactive stream of connector records from the source.
 *
 * The stream emits ConnectorRecord&lt;T&gt; items representing payloads from a live or replay source.
 *
 * @return a Multi that emits ConnectorRecord&lt;T&gt; instances from the source
 */
Multi<ConnectorRecord<T>> stream();
}
