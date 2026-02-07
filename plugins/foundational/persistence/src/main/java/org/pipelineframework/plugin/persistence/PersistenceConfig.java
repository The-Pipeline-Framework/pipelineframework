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

import io.smallrye.config.ConfigMapping;
import java.util.Optional;

import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "persistence")
public interface PersistenceConfig {

    /**
     * Policy to apply when a persistence provider reports a duplicate key.
     */
    @WithDefault("fail")
    String duplicateKey();

    /**
     * Fully-qualified persistence provider class name to lock selection at runtime.
     */
    @WithName("provider.class")
    Optional<String> providerClass();

    /**
     * Timeout for persistence execution wrapped on a Vert.x context.
     */
    @WithDefault("30")
    int vertxContextTimeoutSeconds();
}
